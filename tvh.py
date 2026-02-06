import time
from typing import Dict, List, Optional, Tuple, Union

import requests
from requests.adapters import HTTPAdapter
from requests.auth import AuthBase
from urllib3.util.retry import Retry


class TvheadendClient:
    """Small Tvheadend API client with lightweight TTL caching.

    Why:
    - Channel lists and playlists are expensive to fetch repeatedly from the UI poller.
    - Using sync `requests` is fine as long as FastAPI endpoints that call it are sync too.
    """

    def __init__(
        self,
        base_url: str,
        timeout_s: Union[int, float] = 10,
        cache_ttl_s: float = 10.0,
        *,
        # Reliability knobs
        connect_timeout_s: Union[int, float] = 3,
        retries: int = 3,
        backoff_s: float = 0.4,
        auth: Optional[Union[Tuple[str, str], AuthBase]] = None,
        verify_tls: bool = True,
        pool_maxsize: int = 10,
    ):
        """Create a Tvheadend API client.

        Notes on reliability:
        - We use a Session + connection pooling for performance.
        - We also mount an HTTPAdapter with urllib3 Retry to survive transient
          TCP resets / 502/503/504s and short TVH restarts.
        """

        self.base_url = base_url.rstrip("/")
        self.cache_ttl_s = float(cache_ttl_s)

        # requests accepts a (connect, read) timeout tuple.
        self._timeout = (float(connect_timeout_s), float(timeout_s))
        self._verify_tls = bool(verify_tls)
        self._auth = auth

        self._retries = int(retries)
        self._backoff_s = float(backoff_s)
        self._pool_maxsize = int(pool_maxsize)

        self._sess = self._build_session(retries=self._retries, backoff_s=self._backoff_s, pool_maxsize=self._pool_maxsize)

        self._channels_cache: Optional[List[Dict]] = None
        self._channels_cache_t: float = 0.0

        # profile -> (pairs, t)
        self._m3u_cache: Dict[str, Tuple[List[Tuple[str, str]], float]] = {}

    def _build_session(self, *, retries: int, backoff_s: float, pool_maxsize: int) -> requests.Session:
        sess = requests.Session()

        # Retry on transient errors + dropped connections.
        # TVHeadend can briefly return 503 during restart/upgrade.
        retry = Retry(
            total=retries,
            connect=retries,
            read=retries,
            status=retries,
            backoff_factor=float(backoff_s),
            status_forcelist=(429, 502, 503, 504),
            allowed_methods=frozenset(["GET"]),
            raise_on_status=False,
            respect_retry_after_header=True,
        )

        adapter = HTTPAdapter(max_retries=retry, pool_connections=pool_maxsize, pool_maxsize=pool_maxsize)
        sess.mount("http://", adapter)
        sess.mount("https://", adapter)

        sess.headers.update(
            {
                "User-Agent": "tvh-bridge/1.0",
                "Accept": "application/json, text/plain;q=0.9, */*;q=0.8",
            }
        )
        return sess

    def _get(self, url: str, *, params: Optional[Dict] = None) -> requests.Response:
        """GET with a small amount of self-healing.

        Even with urllib3 Retry enabled, a Session can get stuck with a bad pool
        member after network changes. If we see a RequestException, we rebuild
        the session and try once more.
        """
        try:
            r = self._sess.get(url, params=params, timeout=self._timeout, auth=self._auth, verify=self._verify_tls)
            r.raise_for_status()
            return r
        except requests.RequestException:
            # Rebuild session and retry once.
            try:
                self._sess.close()
            except Exception:
                pass
            self._sess = self._build_session(
                retries=self._retries,
                backoff_s=self._backoff_s,
                pool_maxsize=self._pool_maxsize,
            )
            r = self._sess.get(url, params=params, timeout=self._timeout, auth=self._auth, verify=self._verify_tls)
            r.raise_for_status()
            return r

    def close(self) -> None:
        """Close the underlying HTTP session."""
        try:
            self._sess.close()
        except Exception:
            pass

    def _cache_valid(self, t: float) -> bool:
        return (time.time() - t) < self.cache_ttl_s

    def list_channels(self, force_refresh: bool = False) -> List[Dict]:
        """Return list of channels: uuid, name, number, enabled."""
        if not force_refresh and self._channels_cache is not None and self._cache_valid(self._channels_cache_t):
            return list(self._channels_cache)

        url = f"{self.base_url}/api/channel/grid"
        try:
            r = self._get(url, params={"start": 0, "limit": 10000})
            try:
                data = r.json()
            except ValueError as e:
                raise RuntimeError(f"TVH returned non-JSON for channel grid: {e}")
        except Exception:
            # If TVH is briefly unavailable, prefer stale-but-useful data.
            if self._channels_cache is not None and not force_refresh:
                return list(self._channels_cache)
            raise
        entries = data.get("entries", [])
        entries.sort(key=lambda c: (c.get("number") or 999999, c.get("name") or ""))

        chans = [
            {
                "uuid": c.get("uuid"),
                "name": c.get("name"),
                "number": c.get("number"),
                "enabled": bool(c.get("enabled", True)),
            }
            for c in entries
            if c.get("uuid") and c.get("name")
        ]

        self._channels_cache = chans
        self._channels_cache_t = time.time()
        return list(chans)

    def _parse_m3u(self, text: str) -> List[Tuple[str, str]]:
        """Parse M3U into (display_name, url)."""
        out: List[Tuple[str, str]] = []
        last_name: Optional[str] = None

        for ln in text.splitlines():
            ln = ln.strip()
            if not ln:
                continue
            if ln.startswith("#EXTINF:"):
                last_name = ln.split(",", 1)[1].strip() if "," in ln else None
                continue
            if ln.startswith("#"):
                continue
            if ln.startswith("http://") or ln.startswith("https://"):
                if last_name:
                    out.append((last_name, ln))
                last_name = None

        return out

    def _get_playlist_pairs(self, profile: str, force_refresh: bool = False) -> List[Tuple[str, str]]:
        key = profile or "pass"
        cached = self._m3u_cache.get(key)
        if not force_refresh and cached and self._cache_valid(cached[1]):
            return list(cached[0])

        m3u_url = f"{self.base_url}/playlist/channels.m3u"
        try:
            r = self._get(m3u_url, params={"profile": key})
            pairs = self._parse_m3u(r.text)
        except Exception:
            # Same idea: keep the UI/pipeline working if TVH blips.
            if cached and not force_refresh:
                return list(cached[0])
            raise

        self._m3u_cache[key] = (pairs, time.time())
        return list(pairs)

    def get_stream_url_for_uuid(self, channel_uuid: str, profile: str = "pass") -> str:
        """Map channel UUID -> stream URL by matching channel name against channels.m3u."""
        chans = self.list_channels()
        chan = next((c for c in chans if c["uuid"] == channel_uuid), None)
        if not chan:
            # One retry with a forced refresh (useful if TVH changed quickly)
            chans = self.list_channels(force_refresh=True)
            chan = next((c for c in chans if c["uuid"] == channel_uuid), None)
        if not chan:
            raise RuntimeError(f"Channel UUID not found: {channel_uuid}")

        name = chan["name"]
        pairs = self._get_playlist_pairs(profile=profile)

        for disp, url in pairs:
            if disp == name:
                return url
        for disp, url in pairs:
            if disp.lower() == name.lower():
                return url

        # Retry with forced playlist refresh
        pairs = self._get_playlist_pairs(profile=profile, force_refresh=True)
        for disp, url in pairs:
            if disp == name or disp.lower() == name.lower():
                return url

        raise RuntimeError(f"Stream URL not found in playlist for channel '{name}'")
