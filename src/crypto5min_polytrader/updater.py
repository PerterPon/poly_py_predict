"""Auto-update system for Crypto5min PolyTrader.

Checks the seller's update server for new versions.
Downloads update ZIPs from the seller's update server (key-protected).
No external dependencies (no GitHub, no third-party services).
"""
from __future__ import annotations

import io
import logging
import os
import shutil
import subprocess
import threading
import zipfile
from pathlib import Path
from typing import Optional

import requests

logger = logging.getLogger(__name__)

# ── Config ──────────────────────────────────────────────────────────
VERSION_FILE = Path(__file__).resolve().parent.parent.parent / 'VERSION'
# Fallback — inside Docker the layout is /app/VERSION
if not VERSION_FILE.exists():
    VERSION_FILE = Path('/app/VERSION')

# Keep these in sync with .env.example so updater works even when users
# upgraded from older builds that did not contain the update vars.
#
# NOTE: Some operators run multiple dashboards on the same VPS (e.g., copybot
# and crypto bot). In that case 8602 may be occupied by a different app.
# We therefore try a small list of defaults when C5_UPDATE_SERVER_URL is not set.
DEFAULT_UPDATE_SERVER_URLS = (
    'http://65.109.240.249:8602',
    'http://65.109.240.249:8603',
)
# No hardcoded key — customers must set C5_UPDATE_KEY in their .env.
# The key is sent via Authorization: Bearer header, never in the URL.

# Files/dirs that must NEVER be overwritten during update
PROTECTED = {'config', 'logs', 'data', '.env', 'config/.env'}

# ── Helpers ─────────────────────────────────────────────────────────

def current_version() -> str:
    """Read the local VERSION file."""
    try:
        return VERSION_FILE.read_text(encoding='utf-8').strip()
    except FileNotFoundError:
        return '0.0.0'


def _parse_version(v: str) -> tuple:
    """Parse 'v0.3.1' or '0.3.1' into (0, 3, 1)."""
    v = v.lstrip('v').strip()
    parts = []
    for p in v.split('.'):
        try:
            parts.append(int(p))
        except ValueError:
            parts.append(0)
    while len(parts) < 3:
        parts.append(0)
    return tuple(parts[:3])


def _resolve_update_servers_and_key() -> tuple[list[str], str]:
    server = os.getenv('C5_UPDATE_SERVER_URL', '').strip().rstrip('/')
    key = os.getenv('C5_UPDATE_KEY', '').strip()
    servers: list[str]
    if server:
        servers = [server]
    else:
        servers = [s.rstrip('/') for s in DEFAULT_UPDATE_SERVER_URLS if str(s).strip()]
    # No fallback key — must be set in .env
    return servers, key


def _auth_headers(key: str) -> dict[str, str]:
    """Build Authorization header for update requests."""
    return {'Authorization': f'Bearer {key}'} if key else {}


def _build_download_url_for_server(server: str, version: str, key: str) -> Optional[str]:
    if not server or not key:
        return None
    # Key is sent via Authorization header, NOT in the URL
    return f'{server.rstrip("/")}/update/serve/v{version}'


def _build_download_url(version: str) -> Optional[str]:
    """Build the download URL from the update server config.

    Customers set C5_UPDATE_SERVER_URL + C5_UPDATE_KEY in their .env.
    Downloads go through the seller's protected update server.
    """
    servers, key = _resolve_update_servers_and_key()
    if not servers or not key:
        return None
    # We don't know which default server will respond until check_for_update()
    # runs. This function is kept for backward-compat; prefer building with the
    # server selected in check_for_update().
    return _build_download_url_for_server(servers[0], version, key)


def check_for_update() -> dict:
    """Check the seller's update server for a newer version.

    Calls {C5_UPDATE_SERVER_URL}/update/latest?key={C5_UPDATE_KEY}
    to get the latest available version.

    Returns dict with keys:
        update_available (bool), current (str), latest (str),
        download_url (str|None), release_notes (str), error (str|None)
    """
    cur = current_version()
    result = {
        'update_available': False,
        'current': cur,
        'latest': cur,
        'download_url': None,
        'release_notes': '',
        'error': None,
    }

    servers, key = _resolve_update_servers_and_key()
    if not servers or not key:
        result['error'] = 'C5_UPDATE_SERVER_URL or C5_UPDATE_KEY not set'
        return result

    try:
        data = None
        chosen_server = ''
        last_status = None
        for server in servers:
            url = f'{server}/update/latest'
            try:
                resp = requests.get(url, timeout=15, headers=_auth_headers(key))
                last_status = resp.status_code
                if resp.status_code == 403:
                    result['error'] = 'Invalid update key'
                    return result
                if resp.status_code == 404:
                    # Not an update server (or disabled / bad key) — try next.
                    continue
                resp.raise_for_status()
                data = resp.json()
                if isinstance(data, dict) and 'latest' in data:
                    chosen_server = server
                    break
            except requests.RequestException:
                continue
            except Exception:
                continue

        if not isinstance(data, dict) or not chosen_server:
            result['error'] = 'Update server not available'
            if last_status is not None:
                result['error'] = f'Update server not available (last_status={last_status})'
            return result

        latest = data.get('latest', cur)
        result['latest'] = latest.lstrip('v')
        result['release_notes'] = data.get('release_notes', '')

        # Compare versions
        if _parse_version(latest) > _parse_version(cur):
            result['update_available'] = True
            result['download_url'] = _build_download_url_for_server(chosen_server, result['latest'], key)
            if not result['download_url']:
                result['error'] = 'Update available but download URL could not be built'

    except requests.RequestException as exc:
        result['error'] = f'Network error: {exc}'
        logger.warning('Update check failed: %s', exc)
    except Exception as exc:
        result['error'] = str(exc)
        logger.warning('Update check failed: %s', exc)

    return result


# ── Update state (shared across threads) ────────────────────────────
_update_lock = threading.Lock()
_update_status: dict = {'state': 'idle'}  # idle | downloading | applying | done | error


def get_update_status() -> dict:
    with _update_lock:
        return dict(_update_status)


def _set_status(state: str, **kw):
    with _update_lock:
        _update_status.clear()
        _update_status['state'] = state
        _update_status.update(kw)


def apply_update(download_url: str) -> None:
    """Download the release ZIP and apply the update (runs in background thread).

    Downloads from the seller's update server (key sent via header, not in URL).
    Preserves config/, logs/, data/ and .env files.
    After extraction, container restarts via Docker restart policy.
    """
    _set_status('downloading')
    try:
        app_root = Path('/app') if Path('/app/src').exists() else VERSION_FILE.parent

        # 1. Download ZIP from the seller's update server
        logger.info('Downloading update from %s', download_url)
        _, key = _resolve_update_servers_and_key()
        resp = requests.get(download_url, timeout=120, stream=True,
                            headers=_auth_headers(key))
        resp.raise_for_status()

        zip_bytes = io.BytesIO(resp.content)
        logger.info('Download complete (%d bytes)', len(resp.content))

        _set_status('applying')

        # 2. Extract to a temp dir
        tmp_dir = app_root / '_update_tmp'
        if tmp_dir.exists():
            shutil.rmtree(tmp_dir)
        tmp_dir.mkdir(parents=True)

        with zipfile.ZipFile(zip_bytes) as zf:
            zf.extractall(tmp_dir)

        # GitHub zipball nests in a folder like "krutftw-cryptotrader-abc1234/"
        # Find the actual root inside the extract
        children = list(tmp_dir.iterdir())
        extract_root = children[0] if len(children) == 1 and children[0].is_dir() else tmp_dir

        # 3. Copy new files, skip protected paths
        updated_files = []
        for item in extract_root.rglob('*'):
            rel = item.relative_to(extract_root)
            # Skip protected dirs/files
            top_part = rel.parts[0] if rel.parts else ''
            if top_part in PROTECTED or str(rel) in PROTECTED:
                continue
            dest = app_root / rel
            if item.is_dir():
                dest.mkdir(parents=True, exist_ok=True)
            else:
                dest.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(item, dest)
                updated_files.append(str(rel))

        # 4. Update VERSION file
        new_version_file = extract_root / 'VERSION'
        if new_version_file.exists():
            new_ver = new_version_file.read_text(encoding='utf-8').strip()
            (app_root / 'VERSION').write_text(new_ver + '\n', encoding='utf-8')

        # 5. Cleanup temp
        shutil.rmtree(tmp_dir, ignore_errors=True)

        logger.info('Update applied: %d files updated', len(updated_files))

        # 6. Rebuild Docker container (if running inside Docker)
        # The container will restart via docker-compose restart policy
        # Signal that the process should exit so Docker restarts it
        _set_status('done', files_updated=len(updated_files),
                    message='Update applied! Container will restart momentarily.')

        # Give a moment for the status endpoint to be read, then exit
        # Docker "restart: unless-stopped" will bring us back up
        def _delayed_exit():
            import time
            time.sleep(5)
            logger.info('Restarting for update...')
            os._exit(0)

        threading.Thread(target=_delayed_exit, daemon=True).start()

    except Exception as exc:
        logger.error('Update failed: %s', exc, exc_info=True)
        _set_status('error', message=str(exc))
