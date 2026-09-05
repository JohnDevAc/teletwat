#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUTPUT_DIR="${1:-$PROJECT_DIR/dist}"
PACKAGE_NAME="teletool"
PACKAGE_ARCH="arm64"
VERSION="${TELETOOL_PACKAGE_VERSION:-$(tr -d '\r\n' < "$PROJECT_DIR/VERSION")}"
VERSION="${VERSION#V}"
VERSION="${VERSION#v}"
INSTALLER_VERSION="${TELETOOL_INSTALLER_VERSION:-$(tr -d '\r\n' < "$PROJECT_DIR/INSTALLER_VERSION")}"
RELEASE_BRANCH="${TELETOOL_RELEASE_BRANCH:-main}"
GST_NDI_VERSION="${TELETOOL_GST_NDI_VERSION:-0.13.5}"

if ! [[ "$VERSION" =~ ^[0-9][0-9A-Za-z.+:~-]*$ ]]; then
  echo "Invalid TeleTool package version: $VERSION" >&2
  exit 1
fi
if ! [[ "$INSTALLER_VERSION" =~ ^[0-9]+([.][0-9]+)*$ ]]; then
  echo "Invalid TeleTool installer version: $INSTALLER_VERSION" >&2
  exit 1
fi
case "$RELEASE_BRANCH" in
  main) RELEASE_LABEL="Main"; RELEASE_DEVELOPMENT=false ;;
  dev) RELEASE_LABEL="Dev"; RELEASE_DEVELOPMENT=true ;;
  *)
    echo "Invalid TeleTool release branch: $RELEASE_BRANCH" >&2
    exit 1
    ;;
esac

for command_name in dpkg dpkg-deb dpkg-architecture file install sed; do
  command -v "$command_name" >/dev/null 2>&1 || {
    echo "Missing package build command: $command_name" >&2
    exit 1
  }
done

native_arch="$(dpkg --print-architecture)"
if [ "$native_arch" != "$PACKAGE_ARCH" ]; then
  echo "TeleTool packages must be built on ARM64; current architecture is $native_arch." >&2
  exit 1
fi

plugin_source="${TELETOOL_GST_NDI_PLUGIN:-}"
if [ -z "$plugin_source" ] && command -v pkg-config >/dev/null 2>&1; then
  plugin_dir="$(pkg-config --variable=pluginsdir gstreamer-1.0 2>/dev/null || true)"
  if [ -n "$plugin_dir" ] && [ -f "$plugin_dir/libgstndi.so" ]; then
    plugin_source="$plugin_dir/libgstndi.so"
  fi
fi
if [ -z "$plugin_source" ] || [ ! -f "$plugin_source" ]; then
  echo "Set TELETOOL_GST_NDI_PLUGIN to a built ARM64 libgstndi.so." >&2
  exit 1
fi
plugin_dir="$(cd "$(dirname "$plugin_source")" && pwd)"
plugin_notices="${TELETOOL_GST_NDI_NOTICES_DIR:-$plugin_dir/gst-plugin-ndi-licenses}"
plugin_source_archive="${TELETOOL_GST_NDI_SOURCE_ARCHIVE:-$plugin_dir/gst-plugin-ndi-$GST_NDI_VERSION.crate}"
if [ ! -d "$plugin_notices" ] || [ ! -f "$plugin_notices/DEPENDENCIES.tsv" ]; then
  echo "Missing GStreamer NDI dependency notices: $plugin_notices" >&2
  echo "Build the plugin with scripts/build_gst_ndi_plugin.sh before packaging." >&2
  exit 1
fi
if [ ! -f "$plugin_source_archive" ]; then
  echo "Missing GStreamer NDI corresponding source: $plugin_source_archive" >&2
  exit 1
fi
if ! file "$plugin_source" | grep -Eq 'ELF 64-bit.*(ARM aarch64|aarch64)'; then
  echo "$plugin_source is not an ARM64 ELF shared library." >&2
  file "$plugin_source" >&2
  exit 1
fi

multiarch="$(dpkg-architecture -qDEB_HOST_MULTIARCH)"
build_dir="$(mktemp -d)"
cleanup() {
  rm -rf -- "$build_dir"
}
trap cleanup EXIT

package_root="$build_dir/${PACKAGE_NAME}_${VERSION}_${PACKAGE_ARCH}"
app_root="$package_root/usr/lib/teletool"
control_root="$package_root/DEBIAN"

install -d \
  "$control_root" \
  "$app_root/bin" \
  "$app_root/static" \
  "$package_root/etc/avahi/services" \
  "$package_root/etc/sudoers.d" \
  "$package_root/lib/systemd/system" \
  "$package_root/usr/lib/$multiarch/gstreamer-1.0" \
  "$package_root/usr/share/doc/teletool/third-party/licenses" \
  "$package_root/usr/share/doc/teletool/third-party/source"

for source_file in app.py fleet_manager.py system_manager.py gst_base.py gst_ndi.py inferno_status.py ndi_runtime_config.py scan_report.py tvh.py config.example.json INSTALLER_VERSION; do
  install -m 0644 "$PROJECT_DIR/$source_file" "$app_root/$source_file"
done
printf 'V%s\n' "$VERSION" >"$app_root/VERSION"
chmod 0644 "$app_root/VERSION"
cp -a "$PROJECT_DIR/static/." "$app_root/static/"
install -m 0644 "$PROJECT_DIR/README.md" "$package_root/usr/share/doc/teletool/README.md"
install -m 0644 "$PROJECT_DIR/API.md" "$package_root/usr/share/doc/teletool/API.md"
install -m 0644 "$PROJECT_DIR/License.md" "$package_root/usr/share/doc/teletool/License.md"
install -m 0644 "$PROJECT_DIR/THIRD_PARTY_NOTICES.md" \
  "$package_root/usr/share/doc/teletool/THIRD_PARTY_NOTICES.md"
install -m 0644 "$PROJECT_DIR/packaging/debian/copyright" \
  "$package_root/usr/share/doc/teletool/copyright"
cp -a "$plugin_notices/." "$package_root/usr/share/doc/teletool/third-party/licenses/"
install -m 0644 "$plugin_source_archive" \
  "$package_root/usr/share/doc/teletool/third-party/source/gst-plugin-ndi-$GST_NDI_VERSION.crate"

cat > "$app_root/.teletool_release.json" <<JSON
{
  "branch": "$RELEASE_BRANCH",
  "label": "$RELEASE_LABEL",
  "development": $RELEASE_DEVELOPMENT
}
JSON
chmod 0644 "$app_root/.teletool_release.json"

sed -e "s/__VERSION__/$VERSION/g" \
  -e "s/__INSTALLER_VERSION__/$INSTALLER_VERSION/g" \
  "$PROJECT_DIR/packaging/debian/control.in" > "$control_root/control"
for maintainer_script in postinst prerm postrm; do
  install -m 0755 "$PROJECT_DIR/packaging/debian/$maintainer_script" \
    "$control_root/$maintainer_script"
done

install -m 0644 "$PROJECT_DIR/packaging/debian/teletool.service" \
  "$package_root/lib/systemd/system/teletool.service"
install -m 0644 "$PROJECT_DIR/packaging/debian/teletool-avahi.service" \
  "$package_root/etc/avahi/services/teletool.service"
install -m 0644 "$PROJECT_DIR/packaging/debian/teletool-update@.service" \
  "$package_root/lib/systemd/system/teletool-update@.service"
install -m 0440 "$PROJECT_DIR/packaging/debian/teletool.sudoers" \
  "$package_root/etc/sudoers.d/teletool"
install -m 0755 "$PROJECT_DIR/packaging/debian/configure-tvheadend" \
  "$app_root/bin/configure-tvheadend"
install -m 0755 "$PROJECT_DIR/packaging/debian/update-package" \
  "$app_root/bin/update-package"
install -m 0755 "$PROJECT_DIR/packaging/debian/check-update-health" \
  "$app_root/bin/check-update-health"
sed "s/__INSTALLER_VERSION__/$INSTALLER_VERSION/g" \
  "$PROJECT_DIR/packaging/debian/terminal-ui" > "$app_root/bin/terminal-ui"
chmod 0644 "$app_root/bin/terminal-ui"
sed 's|__NDI_DROP_PATH__|/var/lib/teletool/libndi.so.6|g' \
  "$PROJECT_DIR/packaging/debian/install-ndi-runtime" > \
  "$app_root/bin/install-ndi-runtime"
chmod 0755 "$app_root/bin/install-ndi-runtime"
install -m 0644 "$plugin_source" \
  "$package_root/usr/lib/$multiarch/gstreamer-1.0/libgstndi.so"

(
  cd "$package_root"
  find . -path ./DEBIAN -prune -o -type f -print0 | sort -z | \
    xargs -0 md5sum > DEBIAN/md5sums
)

install -d "$OUTPUT_DIR"
deb_path="$OUTPUT_DIR/${PACKAGE_NAME}_${VERSION}_${PACKAGE_ARCH}.deb"
dpkg-deb --root-owner-group --build "$package_root" "$deb_path"
echo "Built $deb_path"
