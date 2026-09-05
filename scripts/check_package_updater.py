#!/usr/bin/env python3
"""Validate that package-managed Web updates remain wired end to end."""

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def require(path: str, *needles: str) -> None:
    target = ROOT / path
    if not target.is_file():
        raise SystemExit(f"Missing package updater asset: {path}")
    text = target.read_text(encoding="utf-8")
    for needle in needles:
        if needle not in text:
            raise SystemExit(f"{path}: missing required text: {needle}")


require(
    "packaging/debian/update-package",
    "apt-get -qq",
    "teletool-inferno=$target_version",
    'inferno_action="${update_target#main-}"',
    "teletool-inferno-",
    "--no-install-recommends",
    "install --allow-downgrades",
    "apt-repo-dev",
    "update-status.json",
    "flock -n 9",
    'python3 "$(dirname "$0")/check-update-health" "$after" "$channel"',
)
require(
    "packaging/apt/install.sh",
    "MAIN_REPOSITORY_URL",
    "DEV_REPOSITORY_URL",
    'INSTALL_CHANNEL="${1:-${TELETOOL_APT_CHANNEL:-}}"',
    'INSTALL_INFERNO="${2:-${TELETOOL_INSTALL_INFERNO:-auto}}"',
    "read -r selected_channel < /dev/tty",
    "Inferno network audio output:",
    "Choose Inferno option",
    "apt-cache madison teletool-inferno",
    "Suites: $APT_SUITE",
    'install "$apt_recommends_flag" -y "$@"',
)
require(
    "packaging/debian/teletool-update@.service",
    "ExecStart=/usr/lib/teletool/bin/update-package %i",
)
require(
    "packaging/debian/teletool.sudoers",
    "/usr/bin/systemctl --no-block start teletool-update@main-keep.service",
    "/usr/bin/systemctl --no-block start teletool-update@main-install.service",
    "/usr/bin/systemctl --no-block start teletool-update@main-remove.service",
    "/usr/bin/systemctl --no-block start teletool-update@dev-keep.service",
    "/usr/bin/systemctl --no-block start teletool-update@dev-install.service",
    "/usr/bin/systemctl --no-block start teletool-update@dev-remove.service",
)
require(
    "scripts/build_deb.sh",
    "packaging/debian/teletool-update@.service",
    "packaging/debian/update-package",
    "scan_report.py",
    "TELETOOL_RELEASE_BRANCH",
    "packaging/debian/check-update-health",
)
require(
    "packaging/debian/control.in",
    "python3-reportlab",
)
require(
    "scripts/build_inferno_deb.sh",
    "Package: teletool-inferno",
    "alsa_pcm_inferno",
    "statime-linux",
    "INFERNO_REF",
    "STATIME_REF",
)
require(
    "packaging/inferno/postinst",
    "/usr/local/libexec/teletool-inferno/statime",
    "/etc/alsa/conf.d/99-teletool-inferno.conf",
    "/var/backups/teletool-inferno",
)
require(
    ".github/workflows/build-apt-package.yml",
    "teletool-arm64-dev-package",
    "TELETOOL_APT_SUITE: dev",
    "TELETOOL_RELEASE_BRANCH: dev",
    "scripts/build_inferno_deb.sh",
    "scripts/check_inferno_update_api.py",
    "scripts/check_audio_output_lifecycle.py",
    "scripts/check_rf_status.py",
    "scripts/check_tv_scan_report.py",
    "teletool-dev-apt",
    "teletool-stable-apt",
    "scripts/sign_apt_repo.sh",
    "git push --atomic origin HEAD:main HEAD:dev",
    "pages: write",
    "scripts/publish_stable_pages.sh",
    "scripts/check_gst_lifecycle.py",
    "scripts/check_concurrent_state.py",
    "scripts/check_update_health.py",
    "scripts/check_release_shell.py",
)
require(
    "packaging/debian/postinst",
    "if ! systemctl restart teletool.service; then",
)
require(
    "scripts/sign_apt_repo.sh",
    "TELETOOL_APT_GPG_PRIVATE_KEY",
    "TELETOOL_APT_GPG_FINGERPRINT",
    "dpkg-deb -f",
    "verify_package teletool-inferno",
    "gpgv --keyring",
    "sha256sum",
)
require(
    "system_manager.py",
    "_package_update_unit(branch, inferno_action)",
    "_normalise_inferno_action",
    '"inferno": _inferno_package_info()',
    "INFERNO_PACKAGE_CACHE_TTL_S",
    "_recover_stale_update_status",
    "systemctl",
    "show",
    "_read_package_update_status",
    "Updates require a package installation created by the published WGET installer.",
)
require(
    "static/system.html",
    'btn.textContent = "Check for Update"',
    'branchSelect.disabled = false',
    'id="updateInferno"',
    "inferno_action: infernoAction",
)

for path in (ROOT / "app.py", ROOT / "system_manager.py", ROOT / "static" / "system.html"):
    if "Managed by apt" in path.read_text(encoding="utf-8"):
        raise SystemExit(f"{path}: obsolete APT update lockout remains")

python_source = "\n".join(
    path.read_text(encoding="utf-8")
    for path in (ROOT / "app.py", ROOT / "system_manager.py")
)
for obsolete in (
    "_run_program_update_worker",
    "_download_github_update_archive",
    "archive/refs/heads",
    "pi_full_setup.sh",
    "install_network_privileges.sh",
):
    if obsolete in python_source:
        raise SystemExit(f"Python source: unsupported source updater remains: {obsolete}")

for obsolete_path in (
    ".vscode",
    "deploy",
    "requirements.txt",
    "install_network_privileges.sh",
    "scripts/pi_full_setup.sh",
    "scripts/pi_make_golden_image.sh",
    "scripts/pi_setup.sh",
    "scripts/pi_sync.ps1",
):
    if (ROOT / obsolete_path).exists():
        raise SystemExit(f"Obsolete project artifact remains: {obsolete_path}")

require(
    "README.md",
    "## Install with WGET",
    "wget -qO- https://johndevac.github.io/TeleTool/apt-repo/install.sh | sudo sh",
    "wget -qO- https://johndevac.github.io/TeleTool/apt-repo/install.sh | sudo sh -s -- dev",
    "wget -qO- https://johndevac.github.io/TeleTool/apt-repo/install.sh | sudo sh -s -- dev yes",
)

print("Package-managed Web updater is wired end to end.")
