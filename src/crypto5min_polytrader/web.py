from __future__ import annotations

import hashlib
import hmac
import logging
import os
import secrets
import threading
import time
from collections import defaultdict
from pathlib import Path
from typing import Any, Optional

from dotenv import load_dotenv
from fastapi import FastAPI, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse, Response
from fastapi.templating import Jinja2Templates
from starlette.middleware.sessions import SessionMiddleware

from .config import C5Config
from .persistence import JsonStore
from .polymarket_account import snapshot_from_env, append_equity_point
from .polymarket_exec import PolyExecConfig, PolyExecutor, estimate_redeemed_profit_usdc
from .polymarket_reconcile import reconcile_recent_orders
from .runner import run_once, predict_latest, predict_snipe, save_state
from .model import FitResult
from .window import current_window, is_trade_time, is_snipe_time, seconds_remaining, seconds_into_window
from .polymarket_time import polymarket_now, get_time_offset_seconds
from . import runtime_config
from . import resolution as _resolution
from . import polymarket_settlement as _settlement
from . import early_exit as _early_exit
from . import polymarket_redeem as _redeem
from . import gas_topup as _gas
from .withdraw import WithdrawConfig, send_usdc, send_usdc_all
from .chainlink_feed import start_chainlink_feed, get_chainlink_snapshot
from .runner import _record_chainlink_window_open
from .polymarket_ops import close_all_positions_from_env, sell_all_enabled
from . import updater as _updater
from .wallet_validation import (
    is_evm_address,
    looks_like_private_key,
    patch_env_lines,
    resolve_wallet_signature_and_funder,
    wallet_error_message,
)


PRODUCT_NAME = 'Crypto15min PolyTrader'
logger = logging.getLogger(__name__)


APP_LOG_PATH = Path('logs') / 'app.log'


def _setup_file_logging() -> None:
    """Log to both stdout (docker logs) and logs/app.log.

    This enables a noob-friendly "Terminal" tab in the dashboard without
    needing access to host/docker logs.
    """

    try:
        APP_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
        root = logging.getLogger()
        # Avoid duplicate handlers if imported multiple times.
        for h in list(root.handlers or []):
            if isinstance(h, logging.FileHandler) and getattr(h, 'baseFilename', '').endswith(str(APP_LOG_PATH)):
                return

        fh = logging.FileHandler(APP_LOG_PATH, encoding='utf-8')
        fh.setLevel(logging.INFO)
        fmt = logging.Formatter('%(asctime)sZ %(levelname)s %(name)s: %(message)s')
        fh.setFormatter(fmt)

        # Attach to root + uvicorn loggers (uvicorn often sets propagate=False).
        root.addHandler(fh)
        for name in ('uvicorn', 'uvicorn.error', 'uvicorn.access'):
            lg = logging.getLogger(name)
            lg.addHandler(fh)

        # Ensure we emit INFO+.
        if root.level == logging.NOTSET or root.level > logging.INFO:
            root.setLevel(logging.INFO)
    except Exception:
        # Logging must never break the app.
        return


def _tail_file(path: Path, *, max_lines: int = 400, max_bytes: int = 220_000) -> tuple[str, int, str]:
    """Return (content, lines, warning)."""

    try:
        if not path.exists():
            return ('', 0, f'{path} does not exist yet.')
        data = path.read_text(encoding='utf-8', errors='replace')
        if len(data) > int(max_bytes):
            data = data[-int(max_bytes):]
        lines = data.splitlines()
        if len(lines) > int(max_lines):
            lines = lines[-int(max_lines):]
        return ('\n'.join(lines) + ('\n' if lines else ''), len(lines), '')
    except Exception as e:
        return ('', 0, f'Failed to read log: {e}')


def _project_root() -> Path:
    # .../products/crypto5min-polytrader/src/crypto5min_polytrader/web.py
    return Path(__file__).resolve().parents[2]


def _env_path() -> Path:
    root = _project_root()
    cfg = root / 'config' / '.env'
    legacy = root / '.env'
    # Prefer config/.env when present (persisted via Docker volume). Fall back to legacy /app/.env.
    if cfg.exists():
        return cfg
    if legacy.exists():
        return legacy
    return cfg


def _ensure_env_parent() -> None:
    p = _env_path()
    p.parent.mkdir(parents=True, exist_ok=True)


def _read_env_var_from_file(key: str) -> Optional[str]:
    _ensure_env_parent()
    p = _env_path()
    if not p.exists():
        return None
    for line in p.read_text(encoding='utf-8').splitlines():
        if line.strip().startswith(f'{key}='):
            val = line.split('=', 1)[1].strip()
            # Strip surrounding quotes (single or double) to match load_dotenv behaviour.
            if len(val) >= 2 and val[0] == val[-1] and val[0] in ('"', "'"):
                val = val[1:-1]
            return val
    return None


def _get_password() -> str:
    pw = os.getenv('C5_DASHBOARD_PASSWORD')
    if pw and pw.strip():
        return pw.strip()
    pw = _read_env_var_from_file('C5_DASHBOARD_PASSWORD')
    return (pw or '').strip()


def _allowed(request: Request, cfg: C5Config) -> bool:
    if not cfg.dashboard_allowed_ips:
        return True
    allowed = {ip.strip() for ip in cfg.dashboard_allowed_ips.split(',') if ip.strip()}
    if not allowed:
        return True
    ip = request.client.host if request.client else ''
    return ip in allowed


def _stable_session_secret() -> str:
    """Return a session secret that survives container restarts.

    Derived from the dashboard password if set (so rotating the password
    also rotates sessions, which is correct behaviour).  Falls back to a
    file-persisted random key so the first boot without a password still
    produces a stable secret across restarts.
    """
    import hashlib
    pw = os.getenv('C5_DASHBOARD_PASSWORD', '').strip()
    if pw:
        # Deterministic but not reversible — HMAC-SHA256 of the password.
        return hashlib.sha256(f'c5-session-v1:{pw}'.encode()).hexdigest()
    # No password set: use/create a persisted random key in the logs dir.
    key_path = Path('logs') / '.session_secret'
    key_path.parent.mkdir(parents=True, exist_ok=True)
    if key_path.exists():
        k = key_path.read_text(encoding='utf-8').strip()
        if len(k) >= 32:
            return k
    k = secrets.token_urlsafe(48)
    key_path.write_text(k, encoding='utf-8')
    return k


app = FastAPI(title=PRODUCT_NAME, docs_url=None, redoc_url=None, openapi_url=None)
templates = Jinja2Templates(directory=str(_project_root() / 'templates'))

# ── Static files (CSS, JS assets) ────────────────────────────────────
from starlette.staticfiles import StaticFiles as _StaticFiles
_static_dir = _project_root() / 'static'
if _static_dir.exists():
    app.mount('/static', _StaticFiles(directory=str(_static_dir)), name='static')

@app.get('/favicon.ico')
@app.get('/favicon-15m.svg')
def serve_favicon():
    from starlette.responses import FileResponse as _FR
    _path = _project_root() / 'static' / 'favicon-15m.svg'
    if _path.exists():
        return _FR(str(_path), media_type='image/svg+xml')
    return JSONResponse({'error': 'not found'}, status_code=404)


@app.get('/static/{filename:path}')
def serve_static(filename: str):
    """Explicit static file route as fallback with correct MIME types."""
    import mimetypes
    from starlette.responses import FileResponse as _FR
    _path = _project_root() / 'static' / filename
    if not _path.exists() or not _path.is_file():
        return JSONResponse({'error': 'not found'}, status_code=404)
    mime, _ = mimetypes.guess_type(str(_path))
    return _FR(str(_path), media_type=mime or 'application/octet-stream')
app.add_middleware(
    SessionMiddleware,
    secret_key=_stable_session_secret(),
    max_age=60 * 60 * 24 * 30,  # 30 days — survives container restarts
    same_site='lax',
    https_only=False,
)


# ── Security headers middleware ──────────────────────────────────────
from starlette.middleware.base import BaseHTTPMiddleware as _BaseMiddleware

class _SecurityHeadersMiddleware(_BaseMiddleware):
    """Add security headers to every response."""
    async def dispatch(self, request, call_next):
        response = await call_next(request)
        response.headers['X-Robots-Tag'] = 'noindex, nofollow, noarchive, nosnippet'
        response.headers['X-Content-Type-Options'] = 'nosniff'
        response.headers['X-Frame-Options'] = 'DENY'
        response.headers['Referrer-Policy'] = 'no-referrer'
        response.headers['Cache-Control'] = 'no-store'
        return response

app.add_middleware(_SecurityHeadersMiddleware)

STATE_STORE = JsonStore(Path('logs') / 'state.json')
# Shared per-symbol fit cache — written by main loop AND on-demand training endpoint
_SHARED_FITS: dict[str, Any] = {}
SETUP_TOKEN_STORE = Path('logs') / 'setup_token.txt'
KILLSWITCH_PATH = Path('logs') / 'killswitch.json'

POLY_EQUITY_STORE = JsonStore(Path('logs') / 'poly_equity.json')
POLY_LAST_TRADE_STORE = JsonStore(Path('logs') / 'poly_last_trade.json')
POLY_TRADES_STORE = JsonStore(Path('logs') / 'poly_trades.json')
WITHDRAW_LOG_STORE = JsonStore(Path('logs') / 'withdrawals.json')
POLY_OPS_LOG_STORE = JsonStore(Path('logs') / 'poly_ops.json')
MQ_CACHE_STORE = JsonStore(Path('logs') / 'mq_cache.json')


def _load_effective_env() -> dict[str, Any]:
    """Load config/.env + apply runtime overrides.

    Returns the overrides dict.
    """

    _ensure_env_parent()
    load_dotenv(_env_path(), override=True)
    overrides = runtime_config.load_overrides()
    runtime_config.apply_overrides_to_environ(overrides)
    return overrides


def _effective_cfg() -> C5Config:
    _load_effective_env()
    return C5Config.from_env()


def _effective_poly_cfg() -> PolyExecConfig:
    _load_effective_env()
    return PolyExecConfig.from_env()


def _load_json(store: JsonStore, default: Any) -> Any:
    try:
        return store.load(default=default)
    except Exception:
        return default


def _iso_ts(ts: float | int | None) -> str:
    if not ts:
        return ''
    try:
        import datetime

        return datetime.datetime.utcfromtimestamp(float(ts)).replace(tzinfo=datetime.timezone.utc).isoformat()
    except Exception:
        return ''


def _truthy(v: Any) -> bool:
    if v is None:
        return False
    if isinstance(v, bool):
        return v
    s = str(v).strip().lower()
    return s in {'1', 'true', 'yes', 'y', 'on'}


def _load_per_asset_tuning(overrides: dict[str, Any] | None = None) -> dict[str, Any]:
    """Parse C5_PER_ASSET_TUNING_JSON into a dict.

    Schema (example):
      {
        "SOL-USD": {"confidence_threshold": 0.58, "edge_min": 0.03, "mq_max_spread_bps": 90, "mq_min_depth_usdc": 25},
        "BTC-USD": {...}
      }
    """
    raw = None
    try:
        if isinstance(overrides, dict):
            raw = overrides.get('C5_PER_ASSET_TUNING_JSON')
    except Exception:
        raw = None
    if raw is None:
        raw = os.getenv('C5_PER_ASSET_TUNING_JSON', '')
    if not raw:
        return {}
    try:
        import json

        obj = json.loads(raw)
        if not isinstance(obj, dict):
            return {}
        # Strip stale MQ fields from per-asset rows — these should always
        # come from the global Market Quality settings, not per-asset overrides.
        for _sym, _row in obj.items():
            if isinstance(_row, dict):
                _row.pop('mq_max_spread_bps', None)
                _row.pop('mq_min_depth_usdc', None)
        return obj
    except Exception:
        return {}


def _suggest_per_asset_tuning(*, symbols: list[str], max_trades: int = 300) -> dict[str, Any]:
    """Suggest per-asset tuning settings from recent trade history.

    Heuristic (intentionally conservative):
    - Only uses resolved, filled trades.
    - Searches for a confidence threshold that improves win-rate without
      collapsing sample size.
    - Produces ONLY confidence_threshold suggestions for now (safest knob),
      but returns in the same schema as C5_PER_ASSET_TUNING_JSON.

    Returns: {"SOL-USD": {"confidence_threshold": 0.60, "sample": 48, "win_rate": 0.62}, ...}
    """

    trades = _load_json(POLY_TRADES_STORE, default=[]) or []
    if not isinstance(trades, list):
        trades = []
    trades = [t for t in trades if isinstance(t, dict)][-max_trades:]

    out: dict[str, Any] = {}
    for sym in symbols:
        # Use either "symbol" or "asset" fields depending on where the record came from.
        rows = []
        for t in trades:
            raw_sym = (t.get('symbol') or t.get('asset') or '').strip()
            # Normalise: 'sol' -> 'SOL-USD', 'SOL-USD' -> 'SOL-USD'
            if '-' not in raw_sym:
                raw_sym = raw_sym.upper() + '-USD'
            else:
                raw_sym = raw_sym.upper()
            tsym = raw_sym
            if tsym != sym:
                continue
            resolved = t.get('resolved') or ''
            # Accept win/loss. Also accept win_unfilled if redeemed — redemption
            # proves the order was actually filled and won on-chain.
            redeemed = t.get('redeem_status') == 'success'
            if resolved == 'win_unfilled' and redeemed:
                resolved = 'win'   # treat as filled win for learning purposes
            if resolved not in ('win', 'loss'):
                continue
            # Skip unfilled / cancelled — unless redemption proves a real fill.
            filled_size = float(t.get('filled_size') or 0.0)
            if filled_size <= 0 and not redeemed:
                continue
            try:
                conf = float(t.get('confidence') or 0.0)
            except Exception:
                continue
            rows.append((conf, resolved))

        if len(rows) < 12:
            continue

        # Candidate thresholds (0.52–0.80). Avoid pushing too high.
        best = None
        for thr_i in range(52, 81):
            thr = thr_i / 100.0
            filt = [r for r in rows if r[0] >= thr]
            if len(filt) < 10:
                continue
            wins = sum(1 for _, res in filt if res == 'win')
            wr = wins / len(filt)
            # Score balances quality vs quantity.
            score = wr * (len(filt) ** 0.5)
            if (best is None) or (score > best['score']):
                best = {'thr': thr, 'wins': wins, 'n': len(filt), 'wr': wr, 'score': score}

        if best is None:
            continue

        out[sym] = {
            'confidence_threshold': round(float(best['thr']), 2),
            'sample': int(best['n']),
            'win_rate': round(float(best['wr']), 3),
        }

    return out


def _sanitize_for_ui(obj: Any, *, max_str: int = 280, max_list: int = 25, depth: int = 4) -> Any:
    """Best-effort redaction + truncation for UI rendering.

    This endpoint is authenticated, but we still avoid accidentally dumping keys
    like private keys, auth headers, or huge payloads into the dashboard.
    """

    if depth <= 0:
        return '…'

    # Common secret-ish keys we never want to render.
    redact_keys = {
        'private_key',
        'pk',
        'secret',
        'api_key',
        'apikey',
        'authorization',
        'auth',
        'signature',
        'sig',
        'mnemonic',
        'seed',
        'password',
        'cookie',
        'set-cookie',
        'jwt',
        'token',
    }

    if obj is None:
        return None
    if isinstance(obj, (int, float, bool)):
        return obj
    if isinstance(obj, str):
        s = obj
        if len(s) > max_str:
            s = s[: max_str - 1] + '…'
        return s
    if isinstance(obj, dict):
        out: dict[str, Any] = {}
        for k, v in obj.items():
            ks = str(k)
            if ks.strip().lower() in redact_keys:
                out[ks] = '***'
                continue
            out[ks] = _sanitize_for_ui(v, max_str=max_str, max_list=max_list, depth=depth - 1)
        return out
    if isinstance(obj, list):
        items = obj[:max_list]
        return [_sanitize_for_ui(x, max_str=max_str, max_list=max_list, depth=depth - 1) for x in items]
    # Fallback: stringify.
    return _sanitize_for_ui(str(obj), max_str=max_str, max_list=max_list, depth=depth - 1)


def _paused() -> bool:
    return KILLSWITCH_PATH.exists()


def _set_paused(paused: bool) -> None:
    KILLSWITCH_PATH.parent.mkdir(parents=True, exist_ok=True)
    if paused:
        KILLSWITCH_PATH.write_text('{"paused": true, "ts": %.0f}\n' % time.time(), encoding='utf-8')
    else:
        try:
            KILLSWITCH_PATH.unlink(missing_ok=True)
        except TypeError:
            # Python < 3.8 compatibility (not expected here, but safe)
            if KILLSWITCH_PATH.exists():
                KILLSWITCH_PATH.unlink()


def _get_or_create_setup_token() -> str:
    SETUP_TOKEN_STORE.parent.mkdir(parents=True, exist_ok=True)
    if SETUP_TOKEN_STORE.exists():
        return SETUP_TOKEN_STORE.read_text(encoding='utf-8').strip()
    token = secrets.token_urlsafe(24)
    SETUP_TOKEN_STORE.write_text(token, encoding='utf-8')
    return token


def _is_authed(request: Request) -> bool:
    return bool(request.session.get('authed'))


@app.get('/health')
def health():
    env_p = _env_path()
    return {
        'ok': True,
        'product': PRODUCT_NAME,
        'env_path': str(env_p),
        'env_exists': env_p.exists(),
        'password_configured': bool(_get_password()),
    }


@app.get('/favicon.ico')
def favicon():
    # Avoid noisy 404s in the browser console. (We don't ship a real icon yet.)
    return Response(status_code=204)


@app.get('/', response_class=HTMLResponse)
def index(request: Request, error: str = ''):
    cfg = _effective_cfg()
    if not _allowed(request, cfg):
        return HTMLResponse('Forbidden', status_code=403)

    pw = _get_password()
    if not pw:
        return RedirectResponse(url='/setup', status_code=302)
    if not _is_authed(request):
        return RedirectResponse(url='/login', status_code=302)

    state = STATE_STORE.load(default={}) or {}
    poly_cfg = _effective_poly_cfg()
    overrides = runtime_config.load_overrides()
    # Per-asset tuning is optional; keep it safe so the dashboard never 500s
    # if no tuning JSON exists yet.
    per_asset_tuning = _load_per_asset_tuning(overrides)
    withdraw_cfg = WithdrawConfig.from_env()
    snap = _load_json(JsonStore(Path('logs') / 'poly_snapshot.json'), default=None)

    try:
        native_gas_min = float(os.getenv('C5_NATIVE_GAS_MIN', '0.15') or '0.15')
    except Exception:
        native_gas_min = 0.15

    # Derive human-friendly display mode: paper / dry / live
    if cfg.mode == 'paper':
        display_mode = 'paper'
    elif poly_cfg and poly_cfg.dry_run:
        display_mode = 'dry'
    else:
        display_mode = 'live'

    # Derive the wallet address for display (from snapshot or from key).
    wallet_address = ''
    if isinstance(snap, dict):
        wallet_address = snap.get('address', '')
    if not wallet_address:
        pk = (os.getenv('C5_POLY_PRIVATE_KEY', '') or '').strip()
        if pk:
            try:
                from .polymarket_account import derive_address
                wallet_address = derive_address(pk)
            except Exception:
                pass

    return templates.TemplateResponse(
        'dashboard.html',
        {
            'request': request,
            'product': PRODUCT_NAME,
            'cfg': cfg,
            'state': state,
            'poly_cfg': poly_cfg,
            'overrides': overrides,
            'per_asset_tuning': per_asset_tuning,
            'withdraw_cfg': withdraw_cfg,
            'snapshot': snap if isinstance(snap, dict) else None,
            'native_gas_min': native_gas_min,
            'display_mode': display_mode,
            'app_version': _updater.current_version(),
            'wallet_address': wallet_address,
            'error': error,
            'error_message': wallet_error_message(error),
            'active_symbol': cfg.symbol or '',
        },
    )


@app.get('/p/state', response_class=HTMLResponse)
def partial_state(request: Request, symbol: str = ''):
    cfg = _effective_cfg()
    if not _allowed(request, cfg):
        return HTMLResponse('Forbidden', status_code=403)
    if not _get_password() or not _is_authed(request):
        return HTMLResponse('', status_code=204)
    full_state = STATE_STORE.load(default={}) or {}
    active_sym = symbol.upper().strip() if symbol.strip() else (cfg.symbol or 'SOL-USD')
    primary = cfg.symbol or 'SOL-USD'

    # Always merge top-level training metadata into state
    def _merged_state(base, sym_override=None):
        s = dict(base)
        if sym_override and isinstance(sym_override, dict):
            s.update(sym_override)
        # Always carry training metadata from top-level
        for k in ('last_train_ts', 'retrain_minutes', 'wallet', 'reconcile'):
            if k in base and k not in s:
                s[k] = base[k]
        if 'last_train_ts' in base:
            s['last_train_ts'] = base['last_train_ts']
        if 'retrain_minutes' in base:
            s['retrain_minutes'] = base['retrain_minutes']
        return s

    symbols_dict = full_state.get('symbols', {}) if isinstance(full_state.get('symbols'), dict) else {}

    # Get the per-symbol state for the requested symbol
    sym_state = symbols_dict.get(active_sym) if active_sym else None

    if sym_state and isinstance(sym_state, dict) and sym_state.get('direction'):
        # Symbol has been trained — show hero view with its signal
        state = {
            'status': 'ok',
            'symbol': active_sym,
            'direction': sym_state.get('direction', ''),
            'confidence': sym_state.get('confidence', 0),
            'p_up': sym_state.get('p_up'),
            'price': sym_state.get('price', 0),
            'strong': sym_state.get('strong', False),
            'window_slug': sym_state.get('window_slug', ''),
            'ts': sym_state.get('ts', 0),
            # Carry training metadata for the retrain countdown widget
            'last_train_ts': full_state.get('last_train_ts', 0),
            'retrain_minutes': full_state.get('retrain_minutes', 60),
            # Polymarket status from primary state (last trade etc)
            'polymarket': full_state.get('polymarket'),
            'wallet': full_state.get('wallet'),
        }
    elif active_sym and active_sym not in symbols_dict:
        # Symbol selected but not trained yet
        state = {
            'status': 'ok',
            'symbol': active_sym,
            'direction': '',
            'confidence': 0,
            'price': 0,
            'ts': 0,
            'untrained': True,
            'last_train_ts': full_state.get('last_train_ts', 0),
            'retrain_minutes': full_state.get('retrain_minutes', 60),
        }
    else:
        # Primary symbol or no specific symbol — use full state as-is
        state = dict(full_state)

    return templates.TemplateResponse('partials/state.html', {
        'request': request, 'cfg': cfg, 'state': state, 'active_symbol': active_sym
    })


@app.get('/login', response_class=HTMLResponse)
def login_get(request: Request, error: str = ''):
    cfg = _effective_cfg()
    if not _allowed(request, cfg):
        return HTMLResponse('Forbidden', status_code=403)
    if not _get_password():
        return RedirectResponse(url='/setup', status_code=302)
    return templates.TemplateResponse('login.html', {'request': request, 'product': PRODUCT_NAME, 'error': error})


@app.post('/login')
def login_post(request: Request, password: str = Form(...)):
    cfg = _effective_cfg()
    if not _allowed(request, cfg):
        return HTMLResponse('Forbidden', status_code=403)

    expected = _get_password()
    if password.strip() != expected:
        return RedirectResponse(url='/login?error=Invalid%20password', status_code=302)

    request.session['authed'] = True
    return RedirectResponse(url='/', status_code=302)


@app.get('/logout')
def logout(request: Request):
    request.session.clear()
    return RedirectResponse(url='/login', status_code=302)


@app.post('/reset/stats')
def reset_stats(request: Request):
    """Clear all trades and win/loss stats."""
    cfg = _effective_cfg()
    if not _allowed(request, cfg):
        return HTMLResponse('Forbidden', status_code=403)
    if not _is_authed(request):
        return RedirectResponse(url='/login', status_code=302)
    POLY_TRADES_STORE.save([])
    POLY_LAST_TRADE_STORE.save({})
    POLY_EQUITY_STORE.save([])
    logger.info('stats reset by user')
    return RedirectResponse(url='/', status_code=302)


# ── Auto-update endpoints ───────────────────────────────────────────

@app.get('/api/update/check')
def update_check(request: Request):
    """Check GitHub for a newer release."""
    cfg = _effective_cfg()
    if not _allowed(request, cfg):
        return JSONResponse({'error': 'forbidden'}, status_code=403)
    if not _is_authed(request):
        return JSONResponse({'error': 'unauthorized'}, status_code=401)
    info = _updater.check_for_update()
    return JSONResponse(info)


@app.get('/api/update/status')
def update_status(request: Request):
    """Poll the progress of an in-flight update."""
    cfg = _effective_cfg()
    if not _allowed(request, cfg):
        return JSONResponse({'error': 'forbidden'}, status_code=403)
    if not _is_authed(request):
        return JSONResponse({'error': 'unauthorized'}, status_code=401)
    return JSONResponse(_updater.get_update_status())


@app.post('/api/update/apply')
def update_apply(request: Request):
    """User approved the update — download & apply in background."""
    cfg = _effective_cfg()
    if not _allowed(request, cfg):
        return JSONResponse({'error': 'forbidden'}, status_code=403)
    if not _is_authed(request):
        return JSONResponse({'error': 'unauthorized'}, status_code=401)

    status = _updater.get_update_status()
    if status['state'] in ('downloading', 'applying'):
        return JSONResponse({'error': 'Update already in progress'}, status_code=409)

    info = _updater.check_for_update()
    if not info.get('update_available'):
        return JSONResponse({'error': 'No update available'}, status_code=400)

    url = info.get('download_url')
    if not url:
        return JSONResponse({'error': 'No download URL found. Check C5_UPDATE_SERVER_URL and C5_UPDATE_KEY in your .env'}, status_code=400)

    import threading
    threading.Thread(target=_updater.apply_update, args=(url,), daemon=True).start()
    logger.info('User approved update to v%s — downloading...', info.get('latest'))
    return JSONResponse({'ok': True, 'message': f'Updating to v{info.get("latest")}...'})


# ── Update server (seller-only) ─────────────────────────────────────
# Set C5_UPDATE_SERVER=true + C5_UPDATE_KEY on the seller's VPS.
# Customers' bots download from here using their C5_UPDATE_KEY.

from fastapi.responses import FileResponse as _FileResponse


def _update_server_enabled() -> bool:
    return os.getenv('C5_UPDATE_SERVER', '').strip().lower() in ('true', '1', 'yes')


def _update_key() -> str:
    return os.getenv('C5_UPDATE_KEY', '').strip()


def _releases_dir() -> Path:
    return Path(os.getenv('C5_RELEASES_DIR', '/app/releases'))


def _constant_time_key_check(provided: str) -> bool:
    """Timing-safe comparison of the provided key against the server key.

    Returns False both when the key is wrong and when the update server
    is disabled — callers should return a uniform 404 in either case
    so attackers can't fingerprint whether this is an update server.
    """
    if not _update_server_enabled():
        return False
    server_key = _update_key()
    if not server_key:
        return False
    # hmac.compare_digest prevents timing side-channels
    return hmac.compare_digest(server_key.encode(), provided.encode())


# ── Rate limiter for update endpoints ────────────────────────────────
_UPDATE_RATE: dict[str, list[float]] = defaultdict(list)  # ip -> [timestamps]
_UPDATE_RATE_LIMIT = 10   # max requests per window
_UPDATE_RATE_WINDOW = 60  # seconds

def _rate_limited(request: Request) -> bool:
    """Return True if the client IP has exceeded the rate limit."""
    ip = request.client.host if request.client else 'unknown'
    now = time.time()
    hits = _UPDATE_RATE[ip]
    # Prune old entries
    _UPDATE_RATE[ip] = hits = [t for t in hits if now - t < _UPDATE_RATE_WINDOW]
    if len(hits) >= _UPDATE_RATE_LIMIT:
        return True
    hits.append(now)
    return False


def _extract_key(request: Request, query_key: str = '') -> str:
    """Extract update key from Authorization header (preferred) or query param (legacy)."""
    # Prefer: Authorization: Bearer <key>
    auth = request.headers.get('authorization', '').strip()
    if auth.lower().startswith('bearer '):
        return auth[7:].strip()
    # Legacy fallback: ?key=<key>  (kept for backward compat with older bots)
    return query_key


@app.get('/update/latest')
def update_latest(request: Request, key: str = ''):
    """Return the latest available version from the releases directory.

    Only active when C5_UPDATE_SERVER=true (seller's VPS only).
    Customers' bots call this to check if an update is available.
    Authentication via Authorization header (preferred) or ?key= query param.
    Returns a uniform 404 for bad keys / disabled server to prevent fingerprinting.
    """
    if _rate_limited(request):
        return JSONResponse({'error': 'Too many requests'}, status_code=429)

    provided_key = _extract_key(request, key)
    if not _constant_time_key_check(provided_key):
        # Uniform 404 — don't reveal whether this is an update server
        return JSONResponse({'error': 'Not found'}, status_code=404)

    releases = _releases_dir()
    if not releases.exists():
        return JSONResponse({'latest': '0.0.0', 'release_notes': ''})

    # Find highest version ZIP in releases/
    import re as _re
    best_ver = (0, 0, 0)
    best_name = ''
    for f in releases.iterdir():
        if not f.name.endswith('.zip'):
            continue
        m = _re.search(r'v?(\d+\.\d+\.\d+)', f.name)
        if m:
            parts = tuple(int(x) for x in m.group(1).split('.'))
            if parts > best_ver:
                best_ver = parts
                best_name = m.group(1)

    # Optional release notes file
    notes_file = releases / 'RELEASE_NOTES.md'
    notes = ''
    if notes_file.exists():
        try:
            notes = notes_file.read_text(encoding='utf-8')
        except Exception:
            pass

    return JSONResponse({'latest': best_name or '0.0.0', 'release_notes': notes})


@app.get('/update/serve/{version}')
def update_serve(version: str, request: Request, key: str = ''):
    """Serve a release ZIP to authorized customers.

    Only active when C5_UPDATE_SERVER=true (seller's VPS only).
    Authentication via Authorization header (preferred) or ?key= query param.
    Returns a uniform 404 for bad keys / disabled server to prevent fingerprinting.
    """
    if _rate_limited(request):
        return JSONResponse({'error': 'Too many requests'}, status_code=429)

    provided_key = _extract_key(request, key)
    if not _constant_time_key_check(provided_key):
        return JSONResponse({'error': 'Not found'}, status_code=404)

    # Look for the ZIP: releases/Crypto5min_PolyTrader_v0.3.2.zip
    ver = version.lstrip('v')
    zip_name = f'Crypto5min_PolyTrader_v{ver}.zip'
    releases = _releases_dir()
    zip_path = releases / zip_name
    if not zip_path.exists():
        # Also try just the version number
        for f in releases.iterdir():
            if f.name.endswith('.zip') and ver in f.name:
                zip_path = f
                break
    if not zip_path.exists():
        return JSONResponse({'error': f'Release {version} not found'}, status_code=404)

    logger.info('Serving update %s (%d bytes)', zip_name, zip_path.stat().st_size)
    return _FileResponse(
        path=str(zip_path),
        filename=zip_name,
        media_type='application/zip',
    )


@app.post('/pause')
def pause(request: Request):
    cfg = _effective_cfg()
    if not _allowed(request, cfg):
        return HTMLResponse('Forbidden', status_code=403)
    if not _is_authed(request):
        return RedirectResponse(url='/login', status_code=302)
    _set_paused(True)
    STATE_STORE.save({'ts': time.time(), 'status': 'paused'})
    return RedirectResponse(url='/', status_code=302)


@app.post('/resume')
def resume(request: Request):
    cfg = _effective_cfg()
    if not _allowed(request, cfg):
        return HTMLResponse('Forbidden', status_code=403)
    if not _is_authed(request):
        return RedirectResponse(url='/login', status_code=302)
    _set_paused(False)
    STATE_STORE.save({'ts': time.time(), 'status': 'resuming'})
    return RedirectResponse(url='/', status_code=302)


@app.get('/setup', response_class=HTMLResponse)
def setup_get(request: Request, token: str = '', error: str = ''):
    cfg = _effective_cfg()
    if not _allowed(request, cfg):
        return HTMLResponse('Forbidden', status_code=403)
    if _get_password():
        # Already set up — redirect to login (or /reconfigure if authed).
        return RedirectResponse(url='/login', status_code=302)

    real = _get_or_create_setup_token()
    if token != real:
        return templates.TemplateResponse(
            'setup.html',
            {
                'request': request,
                'product': PRODUCT_NAME,
                'needs_token': True,
                'setup_token': real,
                'setup_error': wallet_error_message(error),
            },
        )

    return templates.TemplateResponse(
        'setup.html',
        {
            'request': request,
            'product': PRODUCT_NAME,
            'needs_token': False,
            'setup_token': real,
            'symbol': os.getenv('C5_SYMBOL', 'BTC-USD') or 'BTC-USD',
            'setup_error': wallet_error_message(error),
        },
    )


@app.get('/reconfigure', response_class=HTMLResponse)
def reconfigure_get(request: Request, error: str = ''):
    """Allow logged-in users to re-run the setup wizard (e.g. to add a wallet key they skipped)."""
    cfg = _effective_cfg()
    if not _allowed(request, cfg):
        return HTMLResponse('Forbidden', status_code=403)
    if not _is_authed(request):
        return RedirectResponse(url='/login', status_code=302)

    real = _get_or_create_setup_token()
    return templates.TemplateResponse(
        'setup.html',
        {
            'request': request,
            'product': PRODUCT_NAME,
            'needs_token': False,
            'setup_token': real,
            'symbol': os.getenv('C5_SYMBOL', 'BTC-USD') or 'BTC-USD',
            'reconfigure': True,
            'setup_error': wallet_error_message(error),
        },
    )


@app.post('/setup/save')
def setup_save(
    request: Request,
    token: str = Form(...),
    dashboard_password: str = Form(...),
    symbol: str = Form('BTC-USD'),
    private_key: str = Form(''),
    zerox_api_key: str = Form(''),
    gas_topup_enabled: str = Form(''),
    gas_target_native: str = Form('1.0'),
    gas_max_usdc: str = Form('5'),
    max_usdc: str = Form('5'),
    start_mode: str = Form('paper'),
    withdraw_to_address: str = Form(''),
    withdraw_max_usdc: str = Form('100'),
    wallet_type: str = Form('metamask'),
    funder_address: str = Form(''),
):
    cfg = _effective_cfg()
    if not _allowed(request, cfg):
        return HTMLResponse('Forbidden', status_code=403)
    real = _get_or_create_setup_token()
    if token != real:
        return RedirectResponse(url='/setup', status_code=302)
    if not dashboard_password.strip():
        return RedirectResponse(url=f'/setup?token={real}', status_code=302)

    setup_error_target = '/reconfigure' if _is_authed(request) else f'/setup?token={real}'

    def _setup_redirect_with_error(code: str) -> RedirectResponse:
        sep = '&' if '?' in setup_error_target else '?'
        return RedirectResponse(url=f'{setup_error_target}{sep}error={code}', status_code=302)

    lines = []
    # Quote values that might contain special characters (#, spaces, etc.)
    # so load_dotenv and shell .env parsers don't misinterpret them.
    # python-dotenv supports backslash escapes inside double quotes.
    # $ must also be escaped — bash/docker expand $VAR inside double quotes.
    def _q(v: str) -> str:
        return '"' + v.replace('\\', '\\\\').replace('"', '\\"').replace('$', '\\$') + '"'

    lines.append(f'C5_DASHBOARD_PASSWORD={_q(dashboard_password.strip())}')
    lines.append(f'C5_SYMBOL={symbol.strip()}')

    # Polymarket config from setup wizard Step 2 + 3.
    pk = private_key.strip()
    if pk:
        if not pk.startswith('0x'):
            pk = '0x' + pk

        # Server-side auto-detection: use profile-based validation to
        # determine the correct sig_type and funder, overriding the form
        # values. This prevents bad config even if JS is bypassed.
        validation = _validate_wallet_key(pk)
        if not validation.get('ok'):
            return _setup_redirect_with_error('invalid_key')

        sig_type = str(validation['sig_type'])
        funder = validation.get('funder') or None

        lines.append(f'C5_POLY_PRIVATE_KEY={_q(pk)}')
        # Buyers expect the bot to claim winnings automatically.
        # Can still be disabled later via the sidebar toggle.
        lines.append('C5_POLY_AUTO_REDEEM_ENABLED=true')
        lines.append(f'C5_POLY_SIGNATURE_TYPE={sig_type}')
        if funder:
            lines.append(f'C5_POLY_FUNDER_ADDRESS={funder}')

    # Optional: 0x API key for in-app gas top-ups.
    zx = (zerox_api_key or '').strip()
    if zx:
        lines.append(f'C5_ZEROX_API_KEY={_q(zx)}')

    # Optional: auto gas top-up settings (best-effort, requires some existing gas).
    is_gas_topup = (gas_topup_enabled or '').strip().lower() in {'1', 'true', 'yes', 'on'}
    if is_gas_topup:
        lines.append('C5_GAS_TOPUP_ENABLED=true')
    else:
        lines.append('C5_GAS_TOPUP_ENABLED=false')
    try:
        tgt = float((gas_target_native or '1.0').strip() or '1.0')
    except Exception:
        tgt = 1.0
    try:
        mx = float((gas_max_usdc or '5').strip() or '5')
    except Exception:
        mx = 5.0
    lines.append(f'C5_GAS_TOPUP_TARGET_NATIVE={max(0.0, tgt)}')
    lines.append(f'C5_GAS_TOPUP_MAX_USDC={max(0.0, mx)}')

    # Display symbol (Polygon native gas token). Many users still call it MATIC.
    lines.append('C5_NATIVE_GAS_SYMBOL=POL')
    try:
        max_usdc_val = float(max_usdc.strip() or '5')
    except ValueError:
        max_usdc_val = 5.0
    lines.append(f'C5_POLY_MAX_USDC_PER_TRADE={max_usdc_val}')

    # Withdraw config from setup wizard.
    waddr = withdraw_to_address.strip()
    if waddr:
        lines.append(f'C5_WITHDRAW_TO_ADDRESS={waddr}')
        lines.append('C5_WITHDRAW_ENABLED=true')
        try:
            wmax = float(withdraw_max_usdc.strip() or '100')
        except ValueError:
            wmax = 100.0
        lines.append(f'C5_WITHDRAW_MAX_USDC={wmax}')
    else:
        lines.append('C5_WITHDRAW_ENABLED=false')

    # Start mode: paper (default), dry, or live.
    mode = start_mode.strip().lower()
    if mode == 'live' and pk:
        lines.append('C5_POLY_ENABLED=true')
        lines.append('C5_POLY_DRY_RUN=false')
        lines.append('C5_MODE=polymarket')
    elif mode == 'dry' and pk:
        lines.append('C5_POLY_ENABLED=true')
        lines.append('C5_POLY_DRY_RUN=true')
        lines.append('C5_MODE=polymarket')
    else:
        # Paper mode — Polymarket execution disabled.
        lines.append('C5_POLY_ENABLED=false')
        lines.append('C5_POLY_DRY_RUN=true')
        lines.append('C5_MODE=paper')

    example = (_project_root() / '.env.example').read_text(encoding='utf-8').splitlines()
    passthrough_keys = {
        'C5_DASHBOARD_HOST',
        'C5_DASHBOARD_PORT',
        'C5_DASHBOARD_PUBLIC_PORT',
        'C5_DASHBOARD_ALLOWED_IPS',
        'C5_GRANULARITY_SECONDS',
        'C5_LOOKBACK_DAYS',
        'C5_RETRAIN_MINUTES',
        'C5_CONFIDENCE_THRESHOLD',
        'C5_PAPER_STARTING_CASH',
        'C5_PAPER_FEE_BPS',
        'C5_PAPER_SLIPPAGE_BPS',
        'C5_PAPER_POSITION_FRACTION',
        'C5_LOG_LEVEL',
        'C5_TRADE_LEAD_SECONDS',
        'C5_POLYGON_RPC',

        # Arb-first (safe defaults, no secrets)
        'C5_POLY_ARB_ENABLED',
        'C5_POLY_ARB_MIN_EDGE_CENTS',
        'C5_POLY_ARB_TAKER_FEE_BPS',
        'C5_POLY_ARB_SLIPPAGE_BPS',

        # Bet sizing defaults (safe, no secrets)
        'C5_POLY_BET_MODE',
        'C5_POLY_BET_PERCENT',
        'C5_POLY_KELLY_FRACTION',

    }
    already_set = {ln.split('=', 1)[0] for ln in lines if '=' in ln}
    for ln in example:
        ln = ln.strip()
        if not ln or ln.startswith('#') or '=' not in ln:
            continue
        k, v = ln.split('=', 1)
        if k in already_set:
            continue
        if k in passthrough_keys:
            lines.append(f'{k}={v}')

    _ensure_env_parent()
    _env_path().write_text('\n'.join(lines) + '\n', encoding='utf-8')
    return RedirectResponse(url='/login', status_code=302)


# ── Helpers: on-chain verification ─────────────────────────────────────

def _get_proxy_owners(proxy_address: str, rpc_url: str | None = None) -> list[str]:
    """Call getOwners() on a Gnosis Safe proxy contract. Returns list of owner addresses."""
    from web3 import Web3
    rpc = rpc_url or os.getenv('C5_POLYGON_RPC', 'https://polygon-bor-rpc.publicnode.com')
    w3 = Web3(Web3.HTTPProvider(rpc))
    raw = w3.eth.call({
        'to': Web3.to_checksum_address(proxy_address),
        'data': '0xa0e67e2b',
    })
    if len(raw) < 64:
        return []
    length = int.from_bytes(raw[32:64], 'big')
    owners = []
    for i in range(length):
        start = 64 + i * 32
        addr_bytes = raw[start:start + 32]
        owners.append(Web3.to_checksum_address('0x' + addr_bytes[-20:].hex()))
    return owners


def _get_onchain_usdc_balance(address: str, rpc_url: str | None = None) -> float:
    """Get USDC.e balance on Polygon for the given address."""
    from web3 import Web3
    USDC_E = '0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174'
    rpc = rpc_url or os.getenv('C5_POLYGON_RPC', 'https://polygon-bor-rpc.publicnode.com')
    w3 = Web3(Web3.HTTPProvider(rpc))
    selector = bytes.fromhex('70a08231')
    padded = bytes.fromhex(address[2:].lower().zfill(64))
    raw = w3.eth.call({'to': Web3.to_checksum_address(USDC_E), 'data': '0x' + (selector + padded).hex()})
    return int.from_bytes(raw, 'big') / 1e6


def _validate_wallet_key(private_key: str) -> dict:
    """Profile-based wallet validation. Returns a dict with all detection results."""
    import sys
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    from derive_l2_keys import _fetch_profile, _detect_sig_type

    from .polymarket_account import derive_address, clob_balance_usdc, fetch_positions

    pk = private_key.strip()
    if not pk.startswith('0x'):
        pk = '0x' + pk

    # 1. Derive EOA
    try:
        eoa = derive_address(pk)
    except Exception as e:
        return {'ok': False, 'error': f'Invalid private key: {e}'}

    # 2. Fetch Polymarket profile
    profile = _fetch_profile(eoa)
    if profile is None:
        return {
            'ok': False,
            'error': f'No Polymarket account found for this wallet ({eoa}). '
                     f'Make sure you exported the key from the same wallet you use on polymarket.com.',
            'address': eoa,
        }

    # 3. Detect sig_type + funder from profile
    sig_type, funder = _detect_sig_type(eoa, profile)
    sig_labels = {0: 'MetaMask (EOA)', 1: 'Email / Google', 2: 'MetaMask (Gnosis Safe proxy)'}
    label = sig_labels.get(sig_type, f'type {sig_type}')

    # 4. If proxy wallet, verify EOA is an owner
    if sig_type in (1, 2) and funder.lower() != eoa.lower():
        try:
            owners = _get_proxy_owners(funder)
            owner_addrs = [o.lower() for o in owners]
            if eoa.lower() not in owner_addrs:
                return {
                    'ok': False,
                    'error': f'This key controls {eoa} but your Polymarket proxy wallet ({funder}) '
                             f'is owned by {", ".join(owners)}. You may have exported the wrong key.',
                    'address': eoa,
                    'funder': funder,
                    'sig_type': sig_type,
                    'label': label,
                }
        except Exception:
            pass  # Can't verify — proxy might not be a Gnosis Safe, continue

    # 5. Try CLOB auth + balance (with sig_type fallback)
    clob_url = os.getenv('C5_POLY_CLOB_URL', 'https://clob.polymarket.com') or 'https://clob.polymarket.com'
    clob_bal = 0.0

    def _clob_error_msg(exc: Exception) -> str:
        if isinstance(exc, (ImportError, ModuleNotFoundError)):
            return (
                f'Missing dependency: {exc}. '
                'Please rebuild the Docker container: docker compose down && docker compose up -d --build'
            )
        return f'Key is valid but Polymarket CLOB rejected authentication ({exc}). Try again in a minute.'

    try:
        clob_bal = clob_balance_usdc(pk, signature_type=sig_type, funder=funder, clob_url=clob_url)
    except (ImportError, ModuleNotFoundError) as exc:
        return {
            'ok': False,
            'error': _clob_error_msg(exc),
            'address': eoa,
            'funder': funder,
            'sig_type': sig_type,
            'label': label,
        }
    except Exception:
        # If proxy exists, try the other sig_type (public-profile API doesn't return walletType)
        alt_sig = None
        if sig_type == 2:
            alt_sig = 1  # MetaMask guess failed, try Email/Google
        elif sig_type == 1:
            alt_sig = 2  # Email/Google guess failed, try MetaMask
        if alt_sig is not None:
            try:
                clob_bal = clob_balance_usdc(pk, signature_type=alt_sig, funder=funder, clob_url=clob_url)
                sig_type = alt_sig
                label = sig_labels.get(sig_type, f'type {sig_type}')
            except Exception as exc2:
                return {
                    'ok': False,
                    'error': _clob_error_msg(exc2),
                    'address': eoa,
                    'funder': funder,
                    'sig_type': sig_type,
                    'label': label,
                }
        else:
            return {
                'ok': False,
                'error': f'Key is valid but Polymarket CLOB rejected authentication. Try again in a minute.',
                'address': eoa,
                'funder': funder,
                'sig_type': sig_type,
                'label': label,
            }

    # 6. On-chain USDC balance
    onchain_bal = 0.0
    try:
        onchain_bal = _get_onchain_usdc_balance(funder)
    except Exception:
        pass

    # 7. Positions
    position_count = 0
    try:
        positions = fetch_positions(funder)
        position_count = len(positions)
    except Exception:
        pass

    # 8. Build warning if balance is $0 but has on-chain USDC
    warning = None
    if clob_bal == 0 and onchain_bal > 0:
        warning = f'CLOB balance is $0 but you have ${onchain_bal:,.2f} on-chain USDC. You may need to deposit via polymarket.com first.'
    elif clob_bal == 0 and position_count == 0:
        warning = 'Balance is $0 and no positions found. Make sure you have deposited USDC into your Polymarket account.'

    return {
        'ok': True,
        'sig_type': sig_type,
        'balance': clob_bal,
        'funder': funder,
        'address': eoa,
        'label': label,
        'onchain_balance': onchain_bal,
        'position_count': position_count,
        'warning': warning,
    }


# ── Auto-detect wallet signature type ──────────────────────────────────
@app.post('/api/detect-wallet')
def api_detect_wallet(
    request: Request,
    private_key: str = Form(''),
    funder_address: str = Form(''),
):
    """Profile-based wallet detection: derive EOA, fetch profile, detect type."""
    pk = (private_key or '').strip()
    if not pk:
        return JSONResponse({'ok': False, 'error': 'No private key provided'})

    result = _validate_wallet_key(pk)
    return JSONResponse(result)


# ── Wallet settings (users who skipped wallet step in setup) ──────────
@app.post('/settings/wallet')
def settings_wallet(
    request: Request,
    private_key: str = Form(''),
    wallet_type: str = Form('metamask'),
    funder_address: str = Form(''),
):
    """Update wallet key + type from dashboard (for users who skipped setup)."""
    cfg = _effective_cfg()
    if not _allowed(request, cfg):
        return HTMLResponse('Forbidden', status_code=403)
    if not _is_authed(request):
        return RedirectResponse(url='/login', status_code=302)

    pk = private_key.strip()
    if not pk:
        return RedirectResponse(url='/?error=private_key_required', status_code=302)

    # Validate key — must produce a valid EOA address.
    try:
        from .polymarket_account import derive_address
        eoa = derive_address(pk)
    except Exception:
        return RedirectResponse(url='/?error=invalid_key', status_code=302)

    # Quote helper (same as setup_save).
    def _q(v: str) -> str:
        return '"' + v.replace('\\', '\\\\').replace('"', '\\"') + '"'

    sig_type, fa, clear_funder, wallet_err = resolve_wallet_signature_and_funder(
        wallet_type,
        funder_address,
        derived_eoa=eoa,
    )
    if wallet_err:
        return RedirectResponse(url=f'/?error={wallet_err}', status_code=302)

    # Patch .env file — read, update/insert, write back.
    _ensure_env_parent()
    env_p = _env_path()
    if env_p.exists():
        raw_lines = env_p.read_text(encoding='utf-8').splitlines()
    else:
        raw_lines = []

    patch_keys: dict[str, str] = {
        'C5_POLY_PRIVATE_KEY': _q(pk),
        'C5_POLY_SIGNATURE_TYPE': sig_type,
        'C5_POLY_AUTO_REDEEM_ENABLED': 'true',
    }
    if fa and sig_type in ('1', '2'):
        patch_keys['C5_POLY_FUNDER_ADDRESS'] = fa

    delete_keys: set[str] = set()
    if clear_funder:
        delete_keys.add('C5_POLY_FUNDER_ADDRESS')

    new_lines = patch_env_lines(raw_lines, patch_keys, delete_keys=delete_keys)

    env_p.write_text('\n'.join(new_lines) + '\n', encoding='utf-8')

    # Apply to environ immediately so next cycle picks it up.
    os.environ['C5_POLY_PRIVATE_KEY'] = pk
    os.environ['C5_POLY_SIGNATURE_TYPE'] = sig_type
    os.environ['C5_POLY_AUTO_REDEEM_ENABLED'] = 'true'
    if fa and sig_type in ('1', '2'):
        os.environ['C5_POLY_FUNDER_ADDRESS'] = fa
    elif clear_funder:
        os.environ.pop('C5_POLY_FUNDER_ADDRESS', None)

    # Push sig_type + funder into runtime overrides (these are in ALLOWED_KEYS).
    runtime_config.update_overrides({
        'C5_POLY_SIGNATURE_TYPE': sig_type,
        'C5_POLY_FUNDER_ADDRESS': fa if (fa and sig_type in ('1', '2')) else None,
    })

    load_dotenv(_env_path(), override=True)
    return RedirectResponse(url='/', status_code=302)


@app.get('/p/live', response_class=HTMLResponse)
def partial_live(request: Request, symbol: str = ''):
    cfg = _effective_cfg()
    if not _allowed(request, cfg):
        return HTMLResponse('Forbidden', status_code=403)
    if not _get_password() or not _is_authed(request):
        return HTMLResponse('', status_code=204)
    _active_sym = symbol.upper().strip() if symbol.strip() else (cfg.symbol or 'SOL-USD')

    poly_cfg = _effective_poly_cfg()
    # Load last trade — filter by active symbol if specified
    _all_trades = _load_json(POLY_TRADES_STORE, default=[]) or []
    if not isinstance(_all_trades, list): _all_trades = []
    _sym_trades = [t for t in _all_trades if isinstance(t, dict) and
                   (t.get('symbol','').upper().replace('-USD','') == _active_sym.replace('-USD','') or
                    _active_sym in (t.get('window_slug') or ''))]
    last = _sym_trades[-1] if _sym_trades else {}
    if not last:
        last = _load_json(POLY_LAST_TRADE_STORE, default={}) or {}
    snap = _load_json(JsonStore(Path('logs') / 'poly_snapshot.json'), default=None)
    equity_series = _load_json(POLY_EQUITY_STORE, default=[]) or []
    if not isinstance(equity_series, list):
        equity_series = []
    equity_series = [p for p in equity_series if isinstance(p, dict)]
    start_equity = None
    start_ts = None
    equity_delta = None
    equity_delta_pct = None
    try:
        if equity_series:
            start_equity = float(equity_series[0].get('equity') or 0.0)
            start_ts = float(equity_series[0].get('ts') or 0.0)
            end_equity = float(equity_series[-1].get('equity') or 0.0)
            equity_delta = end_equity - start_equity
            if start_equity > 0:
                equity_delta_pct = (equity_delta / start_equity) * 100.0
    except Exception:
        start_equity = None
        start_ts = None
        equity_delta = None
        equity_delta_pct = None

    now = time.time()
    _live_asset = (_active_sym or cfg.symbol or 'BTC-USD').split('-')[0].lower()
    win = current_window(now, asset=_live_asset)
    resolution_stats = _resolution.load_stats()
    # Filter stats by active symbol if supported
    try:
        sym_stats = _resolution.load_stats(symbol=_active_sym)
        if sym_stats and sym_stats.get('total'):
            resolution_stats = sym_stats
    except TypeError:
        pass  # load_stats doesn't accept symbol param — use global stats

    live = {
        'enabled': bool(poly_cfg.enabled),
        'dry_run': bool(poly_cfg.dry_run),
        'really_trading': bool(poly_cfg.enabled and (not poly_cfg.dry_run)),
        'has_key': bool((poly_cfg.private_key or '').strip()),
        'cooldown_seconds': int(poly_cfg.cooldown_seconds),
        'max_usdc_per_trade': float(poly_cfg.max_usdc_per_trade),
        'market_query': poly_cfg.market_query,
        'last_trade_ts': last.get('ts'),
        'last_trade_iso': _iso_ts(last.get('ts')),
        'last_trade': last if isinstance(last, dict) else {},
        'snapshot': snap if isinstance(snap, dict) else None,
        # Native gas token warning threshold (Polygon PoS native coin)
        'native_gas_min': float(os.getenv('C5_NATIVE_GAS_MIN', '0.15') or '0.15'),
        # P/L since tracking started (based on poly_equity.json)
        'equity_start_usdc': start_equity,
        'equity_start_ts': start_ts,
        'equity_delta_usdc': equity_delta,
        'equity_delta_pct': equity_delta_pct,
        # Window info
        'window_slug': win.slug,
        'window_seconds_remaining': seconds_remaining(now),
        'window_seconds_into': seconds_into_window(now),
        'is_trade_time': is_trade_time(cfg.trade_lead_seconds, now),
        # Resolution stats
        'wins': resolution_stats.get('wins', 0),
        'losses': resolution_stats.get('losses', 0),
        'wins_unfilled': resolution_stats.get('wins_unfilled', 0),
        'losses_unfilled': resolution_stats.get('losses_unfilled', 0),
        'win_rate': resolution_stats.get('win_rate', 0.0),
        'total_resolved': resolution_stats.get('total', 0),
        'total_filled': resolution_stats.get('total_filled', 0),
    }

    return templates.TemplateResponse('partials/live.html', {'request': request, 'cfg': cfg, 'live': live, 'active_symbol': _active_sym})


@app.get('/p/trades', response_class=HTMLResponse)
def partial_trades(request: Request, symbol: str = ''):
    cfg = _effective_cfg()
    if not _allowed(request, cfg):
        return HTMLResponse('Forbidden', status_code=403)
    if not _get_password() or not _is_authed(request):
        return HTMLResponse('', status_code=204)

    trades = _load_json(POLY_TRADES_STORE, default=[]) or []
    if not isinstance(trades, list):
        trades = []
    # newest first
    out: list[dict] = []
    for t in reversed([x for x in trades if isinstance(x, dict)]):
        d = dict(t)
        d['ts_iso'] = _iso_ts(d.get('ts'))

        # UI-only derived fields.
        # Show a "redeemed profit" number only once a win has actually been
        # redeemed on-chain (or confirmed) to avoid confusing intermediate states.
        if d.get('resolved') == 'win' and d.get('redeem_status') == 'success':
            d['redeem_profit_usdc'] = estimate_redeemed_profit_usdc(d)
        else:
            d['redeem_profit_usdc'] = None

        out.append(d)
        if len(out) >= 50:
            break
    return templates.TemplateResponse('partials/trades.html', {'request': request, 'cfg': cfg, 'trades': out})


@app.get('/p/pnl', response_class=HTMLResponse)
def partial_pnl(request: Request):
    """P&L summary cards — realized/unrealized/win-rate/avg-trade."""
    cfg = _effective_cfg()
    if not _allowed(request, cfg):
        return HTMLResponse('Forbidden', status_code=403)
    if not _get_password() or not _is_authed(request):
        return HTMLResponse('', status_code=204)

    trades = _load_json(POLY_TRADES_STORE, default=[]) or []
    if not isinstance(trades, list):
        trades = []

    snap = _load_json(JsonStore(Path('logs') / 'poly_snapshot.json'), default={}) or {}

    win_profits: list[float] = []
    loss_costs: list[float] = []

    for t in trades:
        if not isinstance(t, dict):
            continue
        resolved = t.get('resolved')
        if resolved not in ('win', 'loss'):
            continue
        filled = float(t.get('filled_size') or 0.0)
        if filled <= 0:
            continue

        if resolved == 'win':
            pnl = estimate_redeemed_profit_usdc(t)
            if pnl is None:
                usdc = float(t.get('usdc') or 0.0)
                px = float(t.get('avg_fill_price') or t.get('price') or 0.0)
                if usdc > 0 and 0 < px < 1:
                    pnl = usdc * (1.0 - px)
            if pnl is not None:
                win_profits.append(pnl)
        else:
            usdc = float(t.get('usdc') or 0.0)
            px = float(t.get('avg_fill_price') or t.get('price') or 0.0)
            cost = -(filled * px) if (filled > 0 and 0 < px < 1) else -usdc if usdc > 0 else None
            if cost is not None:
                loss_costs.append(abs(cost))

    total_wins = len(win_profits)
    total_losses = len(loss_costs)
    total_realized = sum(win_profits) - sum(loss_costs) if (win_profits or loss_costs) else None
    win_rate = (total_wins / (total_wins + total_losses) * 100.0) if (total_wins + total_losses) > 0 else 0.0

    pnl_ctx = {
        'total_realized': total_realized,
        'unrealized': float(snap.get('unrealized_pnl_usdc') or 0.0) if snap.get('unrealized_pnl_usdc') is not None else None,
        'cost_basis': float(snap.get('cost_basis_usdc') or 0.0),
        'open_positions': int(snap.get('active_positions') or 0),
        'total_wins': total_wins,
        'total_losses': total_losses,
        'win_rate': win_rate,
        'avg_win': (sum(win_profits) / total_wins) if total_wins > 0 else None,
        'avg_loss': (sum(loss_costs) / total_losses) if total_losses > 0 else None,
        'best_trade': max(win_profits) if win_profits else None,
        'worst_trade': max(loss_costs) if loss_costs else None,
    }

    return templates.TemplateResponse('partials/pnl.html', {'request': request, 'pnl': pnl_ctx})


# ── Ledger API (auto-generated monthly/annual Excel workbooks) ──────────────

@app.get('/api/ledgers')
def api_list_ledgers(request: Request):
    """List all auto-generated ledger files."""
    cfg = _effective_cfg()
    if not _allowed(request, cfg):
        return JSONResponse({'error': 'forbidden'}, status_code=403)
    if not _get_password() or not _is_authed(request):
        return JSONResponse({'error': 'unauthorized'}, status_code=401)
    from .ledger_manager import list_ledgers as _list_ledgers
    return JSONResponse({'ledgers': _list_ledgers()})


@app.get('/api/ledgers/{filename}')
def api_download_ledger(request: Request, filename: str):
    """Download a specific auto-generated ledger file."""
    cfg = _effective_cfg()
    if not _allowed(request, cfg):
        return JSONResponse({'error': 'forbidden'}, status_code=403)
    if not _get_password() or not _is_authed(request):
        return JSONResponse({'error': 'unauthorized'}, status_code=401)
    from .ledger_manager import get_ledger_bytes as _get_ledger
    data = _get_ledger(filename)
    if data is None:
        return JSONResponse({'error': 'not found'}, status_code=404)
    import io
    from starlette.responses import StreamingResponse
    return StreamingResponse(
        io.BytesIO(data),
        media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        headers={'Content-Disposition': f'attachment; filename="{filename}"'},
    )


@app.post('/api/ledgers/rebuild')
def api_rebuild_ledgers(request: Request):
    """Force a full ledger rebuild."""
    cfg = _effective_cfg()
    if not _allowed(request, cfg):
        return JSONResponse({'error': 'forbidden'}, status_code=403)
    if not _get_password() or not _is_authed(request):
        return JSONResponse({'error': 'unauthorized'}, status_code=401)
    from .ledger_manager import update_ledgers as _update_ledgers
    _update_ledgers(force=True)
    from .ledger_manager import list_ledgers as _list_ledgers
    return JSONResponse({'ok': True, 'ledgers': _list_ledgers()})


@app.get('/p/ops', response_class=HTMLResponse)
def partial_ops(request: Request, symbol: str = ''):
    cfg = _effective_cfg()
    if not _allowed(request, cfg):
        return HTMLResponse('Forbidden', status_code=403)
    if not _get_password() or not _is_authed(request):
        return HTMLResponse('', status_code=204)

    # Keep environment+overrides in sync with the rest of the app.
    poly_cfg = _effective_poly_cfg()
    overrides = runtime_config.load_overrides() or {}

    auto_redeem_enabled = _truthy(overrides.get('C5_POLY_AUTO_REDEEM_ENABLED') or os.getenv('C5_POLY_AUTO_REDEEM_ENABLED'))
    polymarket_mode_enabled = bool(poly_cfg.enabled)

    state = STATE_STORE.load(default={}) or {}
    if not isinstance(state, dict):
        state = {}

    redeem = state.get('redeem') if isinstance(state.get('redeem'), dict) else None
    redeem_reconcile = state.get('redeem_reconcile') if isinstance(state.get('redeem_reconcile'), dict) else None

    try:
        events_limit = int(float(os.getenv('C5_UI_OPS_LOG_LIMIT', '200') or '200'))
    except Exception:
        events_limit = 200
    events_limit = max(25, min(500, events_limit))

    log = _load_json(POLY_OPS_LOG_STORE, default=[]) or []
    if not isinstance(log, list):
        log = []
    # newest first
    log = [x for x in log if isinstance(x, dict)][-events_limit:][::-1]

    def _event_summary(action: str, result: Any) -> str:
        if isinstance(result, dict):
            if result.get('error'):
                return f"error={result.get('error')}"
            if action in {'redeem_positions', 'redeem_confirm'}:
                return (
                    f"candidates={result.get('candidates')} submitted={result.get('submitted')} planned={result.get('planned')}"
                )
            if action == 'redeem_reconcile':
                return (
                    f"checked={result.get('checked')} updated={result.get('updated')} pending={result.get('pending')}"
                )
            if action == 'sell_all':
                # close_all_positions_from_env returns various shapes; show the key count-ish fields.
                for k in ('planned', 'submitted', 'closed', 'positions'):
                    if k in result:
                        return f"{k}={result.get(k)}"
            # generic fallback
            keys = list(result.keys())
            keys = keys[:6]
            return 'keys=' + ','.join(str(k) for k in keys)
        if result is None:
            return ''
        return str(result)

    events_out: list[dict] = []
    for e in log:
        ts = e.get('ts')
        action = str(e.get('action') or '')
        res = e.get('result')
        ok = None
        if isinstance(e.get('ok'), bool):
            ok = e.get('ok')
        elif isinstance(res, dict) and isinstance(res.get('ok'), bool):
            ok = res.get('ok')
        events_out.append(
            {
                'ts': _iso_ts(ts) or str(ts or '—'),
                'action': action or '—',
                'ok': ok,
                'summary': _sanitize_for_ui(_event_summary(action, res), max_str=220, depth=2),
                # If we ever want to add a details expander later, keep a safe payload ready.
                'detail': _sanitize_for_ui(res, max_str=280, max_list=20, depth=3),
            }
        )

    # ── Early exit status ──────────────────────────────────────────────
    ee_enabled_raw   = str(overrides.get('C5_EARLY_EXIT_ENABLED') or os.getenv('C5_EARLY_EXIT_ENABLED', 'false')).strip().lower()
    ee_enabled       = ee_enabled_raw in ('1', 'true', 'yes')
    ee_trail_pct     = float(overrides.get('C5_EARLY_EXIT_TRAIL_PCT')     or os.getenv('C5_EARLY_EXIT_TRAIL_PCT',     '15') or 15)
    ee_trail_act_pct = float(overrides.get('C5_EARLY_EXIT_TRAIL_ACT_PCT') or os.getenv('C5_EARLY_EXIT_TRAIL_ACT_PCT', '10') or 10)
    ee_sl_pct        = float(overrides.get('C5_EARLY_EXIT_SL_PCT')        or os.getenv('C5_EARLY_EXIT_SL_PCT',        '50') or 50)

    # Load open positions being monitored
    ee_trades = _load_json(POLY_TRADES_STORE, default=[]) or []
    if not isinstance(ee_trades, list):
        ee_trades = []
    _ee_peaks = _load_json(JsonStore(Path('logs') / 'early_exit_peaks.json'), default={}) or {}
    ee_open = []
    for _t in ee_trades:
        if not isinstance(_t, dict): continue
        if _t.get('dry_run'): continue
        if _t.get('early_exit'): continue
        _res = _t.get('resolved') or ''
        if _res in ('win', 'loss', 'win_unfilled', 'loss_unfilled'): continue
        if _t.get('redeem_status') == 'success': continue
        _fs = 0.0
        try: _fs = float(_t.get('filled_size') or _t.get('size') or 0)
        except Exception: pass
        if _fs <= 0: continue
        if not _t.get('token_id'): continue
        _entry = 0.0
        try: _entry = float(_t.get('price') or 0)
        except Exception: pass
        if _entry <= 0: continue
        _peak = float(_ee_peaks.get(_t.get('token_id','')) or _entry)
        _trail_stop   = round(_peak * (1.0 - ee_trail_pct / 100.0), 4)
        _sl_floor     = round(_entry * (1.0 - ee_sl_pct / 100.0), 4)
        _trail_active = (_peak - _entry) / _entry * 100.0 >= ee_trail_act_pct
        ee_open.append({
            'slug':          _t.get('window_slug', '?'),
            'direction':     str(_t.get('direction') or '').upper(),
            'entry':         round(_entry, 4),
            'shares':        round(_fs, 4),
            'peak':          round(_peak, 4),
            'trail_stop':    _trail_stop,
            'sl_floor':      _sl_floor,
            'trail_active':  _trail_active,
            'trail_act_pct': round(ee_trail_act_pct, 1),
            'placed_ts':     _iso_ts(_t.get('placed_ts') or _t.get('ts')) or '?',
        })
    # Load early exit history from trades log
    ee_history = []
    for _t in reversed(ee_trades[-200:]):
        if not isinstance(_t, dict): continue
        if not _t.get('early_exit'): continue
        _pnl = _t.get('early_exit_pnl_pct')
        ee_history.append({
            'slug':      _t.get('window_slug', '?'),
            'direction': str(_t.get('direction') or '').upper(),
            'trigger':   _t.get('early_exit_trigger', '?'),
            'entry':     round(float(_t.get('price') or 0), 4),
            'bid':       round(float(_t.get('early_exit_bid') or 0), 4),
            'pnl_pct':   round(float(_pnl), 2) if _pnl is not None else None,
            'ts':        _iso_ts(_t.get('early_exit_ts') or _t.get('ts')) or '?',
        })
        if len(ee_history) >= 20:
            break

    return templates.TemplateResponse(
        'partials/ops.html',
        {
            'request': request,
            'cfg': cfg,
            'updated_ts': _iso_ts(time.time()),
            'auto_redeem_enabled': auto_redeem_enabled,
            'polymarket_mode_enabled': polymarket_mode_enabled,
            'redeem': _sanitize_for_ui(redeem, max_str=280, max_list=20, depth=3),
            'redeem_reconcile': _sanitize_for_ui(redeem_reconcile, max_str=280, max_list=20, depth=3),
            'events': events_out,
            'events_limit': events_limit,
            'ee_enabled':       ee_enabled,
            'ee_trail_pct':     ee_trail_pct,
            'ee_trail_act_pct': ee_trail_act_pct,
            'ee_sl_pct':        ee_sl_pct,
            'ee_open':          ee_open,
            'ee_history':       ee_history,
        },
    )


@app.get('/p/terminal', response_class=HTMLResponse)
def partial_terminal(request: Request, symbol: str = ''):
    cfg = _effective_cfg()
    if not _allowed(request, cfg):
        return HTMLResponse('Forbidden', status_code=403)
    if not _get_password() or not _is_authed(request):
        return HTMLResponse('', status_code=204)

    try:
        max_lines = int(float(os.getenv('C5_UI_TERMINAL_LOG_LINES', '400') or '400'))
    except Exception:
        max_lines = 400
    max_lines = max(100, min(1500, max_lines))

    content, lines, warning = _tail_file(APP_LOG_PATH, max_lines=max_lines)
    return templates.TemplateResponse(
        'partials/terminal.html',
        {
            'request': request,
            'content': content,
            'lines': lines,
            'warning': warning,
        },
    )


# ── Stub routes for legacy/phantom requests (suppress 404 noise) ─────
# These endpoints are requested by some browsers/extensions but don't
# exist in the crypto5min dashboard.  Return 204 No Content silently.
_PHANTOM_PATHS = [
    '/p/bot-status', '/p/copy-status', '/p/copy-log',
    '/p/debug-logs', '/p/portfolio', '/p/performance',
]
for _pp in _PHANTOM_PATHS:
    app.add_api_route(_pp, lambda: Response(status_code=204), methods=['GET'])


@app.get('/api/poly/equity')
def api_poly_equity(request: Request):
    cfg = _effective_cfg()
    if not _allowed(request, cfg):
        return JSONResponse({'error': 'forbidden'}, status_code=403)
    if not _get_password() or not _is_authed(request):
        return JSONResponse({'error': 'unauthorized'}, status_code=401)

    series = _load_json(POLY_EQUITY_STORE, default=[]) or []
    if not isinstance(series, list):
        series = []
    # return last N points to keep payload small
    series = [p for p in series if isinstance(p, dict)][-1500:]
    return JSONResponse(series)


@app.get('/api/poly/pnl_curve')
def api_poly_pnl_curve(request: Request):
    """Best-effort PnL curve derived from resolved filled trades.

    This is used as an overlay on the Equity Curve chart so you can
    visually compare equity vs (approx) realized PnL.
    """
    cfg = _effective_cfg()
    if not _allowed(request, cfg):
        return JSONResponse({'error': 'forbidden'}, status_code=403)
    if not _get_password() or not _is_authed(request):
        return JSONResponse({'error': 'unauthorized'}, status_code=401)

    trades = _load_json(POLY_TRADES_STORE, default=[]) or []
    if not isinstance(trades, list):
        trades = []

    from .polymarket_exec import estimate_redeemed_profit_usdc

    rows = []
    for t in trades:
        if not isinstance(t, dict):
            continue
        if t.get('resolved') not in ('win', 'loss'):
            continue
        filled = float(t.get('filled_size') or 0.0)
        if filled <= 0:
            continue
        ts = int(float(t.get('resolved_ts') or t.get('ts') or 0) or 0)
        if ts <= 0:
            continue
        pnl = None
        if t.get('resolved') == 'win':
            pnl = estimate_redeemed_profit_usdc(t)
            # Fallback to +usdc*(1-price) when estimate isn't available.
            if pnl is None:
                usdc = float(t.get('usdc') or 0.0)
                px = float(t.get('avg_fill_price') or t.get('price') or 0.0)
                if usdc > 0 and 0 < px < 1:
                    pnl = usdc * (1.0 - px)
        else:
            # Loss: approx -cost basis
            usdc = float(t.get('usdc') or 0.0)
            px = float(t.get('avg_fill_price') or t.get('price') or 0.0)
            if filled > 0 and 0 < px < 1:
                pnl = -(filled * px)
            elif usdc > 0:
                pnl = -usdc

        if pnl is None:
            continue
        rows.append((ts, float(pnl)))

    rows.sort(key=lambda x: x[0])
    curve = []
    cum = 0.0
    for ts, pnl in rows[-1500:]:
        cum += pnl
        curve.append({'ts': ts, 'pnl': round(cum, 4)})
    return JSONResponse(curve)


@app.get('/api/poly/trades')
def api_poly_trades(request: Request):
    cfg = _effective_cfg()
    if not _allowed(request, cfg):
        return JSONResponse({'error': 'forbidden'}, status_code=403)
    if not _get_password() or not _is_authed(request):
        return JSONResponse({'error': 'unauthorized'}, status_code=401)
    trades = _load_json(POLY_TRADES_STORE, default=[]) or []
    if not isinstance(trades, list):
        trades = []
    return JSONResponse(trades[-500:])


@app.get('/api/per_asset/suggestions')
def api_per_asset_suggestions(request: Request):
    """Suggest per-asset tuning settings from recent trade history."""
    cfg = _effective_cfg()
    if not _allowed(request, cfg):
        return JSONResponse({'error': 'forbidden'}, status_code=403)
    if not _get_password() or not _is_authed(request):
        return JSONResponse({'error': 'unauthorized'}, status_code=401)

    symbols = list(cfg.symbols or [cfg.symbol])
    return JSONResponse(_suggest_per_asset_tuning(symbols=symbols))


@app.post('/api/train_symbol')
def api_train_symbol(request: Request, symbol: str = ''):
    """Trigger an on-demand retrain for a specific symbol.
    Called when the user clicks a symbol button that has no cached fit yet.
    Runs synchronously in a background thread so the response returns immediately.
    """
    cfg = _effective_cfg()
    if not _allowed(request, cfg):
        return JSONResponse({'error': 'forbidden'}, status_code=403)
    if not _get_password() or not _is_authed(request):
        return JSONResponse({'error': 'unauthorized'}, status_code=401)

    sym = (symbol or cfg.symbol or '').upper().strip()
    if not sym:
        return JSONResponse({'error': 'no symbol'}, status_code=400)

    all_syms = list(cfg.symbols or [cfg.symbol])
    if sym not in all_syms:
        return JSONResponse({'error': f'{sym} not in active symbols'}, status_code=400)

    import threading

    def _do_train():
        try:
            logger.info('api_train_symbol: starting on-demand retrain for %s', sym)
            st = run_once(cfg.with_symbol(sym))
            fit_obj = st.pop('fit', None) if isinstance(st, dict) else None
            if fit_obj is not None and isinstance(fit_obj, FitResult):
                _SHARED_FITS[sym] = fit_obj
                logger.info('api_train_symbol: fit cached for %s', sym)
            # Merge into state store
            full_state = STATE_STORE.load(default={}) or {}
            if not isinstance(full_state, dict):
                full_state = {}
            syms_dict = full_state.get('symbols', {})
            if not isinstance(syms_dict, dict):
                syms_dict = {}
            if isinstance(st, dict):
                st.pop('fit', None)
                syms_dict[sym] = st
            full_state['symbols'] = syms_dict
            STATE_STORE.save(full_state)
            logger.info('api_train_symbol: retrain complete for %s', sym)
        except Exception as exc:
            logger.warning('api_train_symbol: retrain failed for %s: %s', sym, exc)

    t = threading.Thread(target=_do_train, daemon=True)
    t.start()
    return JSONResponse({'ok': True, 'symbol': sym, 'status': 'training_started'})


@app.get('/api/early_exit_status')
def api_early_exit_status(request: Request):
    cfg = _effective_cfg()
    if not _allowed(request, cfg):
        return JSONResponse({'error': 'forbidden'}, status_code=403)
    if not _get_password() or not _is_authed(request):
        return JSONResponse({'error': 'unauthorized'}, status_code=401)

    overrides = runtime_config.load_overrides() or {}
    ee_enabled_raw = str(overrides.get('C5_EARLY_EXIT_ENABLED') or os.getenv('C5_EARLY_EXIT_ENABLED', 'false')).strip().lower()
    ee_enabled = ee_enabled_raw in ('1', 'true', 'yes')
    ee_trail_pct     = float(overrides.get('C5_EARLY_EXIT_TRAIL_PCT')     or os.getenv('C5_EARLY_EXIT_TRAIL_PCT',     '15') or 15)
    ee_trail_act_pct = float(overrides.get('C5_EARLY_EXIT_TRAIL_ACT_PCT') or os.getenv('C5_EARLY_EXIT_TRAIL_ACT_PCT', '10') or 10)
    ee_sl_pct        = float(overrides.get('C5_EARLY_EXIT_SL_PCT')        or os.getenv('C5_EARLY_EXIT_SL_PCT',        '50') or 50)

    trades = _load_json(POLY_TRADES_STORE, default=[]) or []
    if not isinstance(trades, list):
        trades = []

    open_positions = []
    for t in trades:
        if not isinstance(t, dict): continue
        if t.get('dry_run'): continue
        if t.get('early_exit'): continue
        res = t.get('resolved') or ''
        if res in ('win', 'loss', 'win_unfilled', 'loss_unfilled'): continue
        if t.get('redeem_status') == 'success': continue
        try:
            fs = float(t.get('filled_size') or t.get('size') or 0)
        except Exception:
            fs = 0.0
        if fs <= 0: continue
        if not t.get('token_id'): continue
        try:
            entry = float(t.get('price') or 0)
        except Exception:
            entry = 0.0
        if entry <= 0: continue
        # Load persisted peak if available
        _peaks = _load_json(JsonStore(Path('logs') / 'early_exit_peaks.json'), default={}) or {}
        _peak = float(_peaks.get(t.get('token_id','')) or entry)
        _trail_stop = round(_peak * (1.0 - ee_trail_pct / 100.0), 4)
        _sl_floor   = round(entry * (1.0 - ee_sl_pct / 100.0), 4)
        _trail_active = (_peak - entry) / entry * 100.0 >= ee_trail_act_pct
        open_positions.append({
            'slug':          t.get('window_slug', '?'),
            'direction':     str(t.get('direction') or '').upper(),
            'entry':         round(entry, 4),
            'shares':        round(fs, 4),
            'bid':           round(entry, 4),  # will be stale — live bid in early_exit module
            'peak':          round(_peak, 4),
            'trail_stop':    _trail_stop,
            'sl_floor':      _sl_floor,
            'trail_active':  _trail_active,
            'trail_act_pct': round(ee_trail_act_pct, 1),
            'pnl_pct':       0.0,
            'placed_ts':     _iso_ts(t.get('placed_ts') or t.get('ts')) or '?',
        })

    history = []
    for t in reversed(trades[-200:]):
        if not isinstance(t, dict): continue
        if not t.get('early_exit'): continue
        pnl = t.get('early_exit_pnl_pct')
        history.append({
            'slug':      t.get('window_slug', '?'),
            'direction': str(t.get('direction') or '').upper(),
            'trigger':   t.get('early_exit_trigger', '?'),
            'entry':     round(float(t.get('price') or 0), 4),
            'bid':       round(float(t.get('early_exit_bid') or 0), 4),
            'pnl_pct':   round(float(pnl), 2) if pnl is not None else None,
            'ts':        _iso_ts(t.get('early_exit_ts') or t.get('ts')) or '?',
        })
        if len(history) >= 20:
            break

    return JSONResponse({
        'ok':            True,
        'enabled':       ee_enabled,
        'trail_pct':     ee_trail_pct,
        'trail_act_pct': ee_trail_act_pct,
        'sl_pct':        ee_sl_pct,
        'open_positions': open_positions,
        'history':       history,
    })


@app.get('/api/learning_status')
def api_learning_status(request: Request):
    cfg = _effective_cfg()
    if not _allowed(request, cfg):
        return JSONResponse({'error': 'forbidden'}, status_code=403)
    if not _get_password() or not _is_authed(request):
        return JSONResponse({'error': 'unauthorized'}, status_code=401)

    MIN_TRADES = 12
    symbols = list(cfg.symbols or [cfg.symbol])

    trades = _load_json(POLY_TRADES_STORE, default=[]) or []
    if not isinstance(trades, list):
        trades = []
    trades = [t for t in trades if isinstance(t, dict)][-300:]

    overrides = runtime_config.load_overrides()
    pa_tuning = _load_per_asset_tuning(overrides)
    if not isinstance(pa_tuning, dict):
        pa_tuning = {}

    symbols_out = {}
    for sym in symbols:
        all_filled = []
        resolved_filled = []
        for t in trades:
            raw_sym = (t.get('symbol') or t.get('asset') or '').strip()
            if '-' not in raw_sym:
                raw_sym = raw_sym.upper() + '-USD'
            else:
                raw_sym = raw_sym.upper()
            tsym = raw_sym
            if tsym != sym:
                continue
            fs = float(t.get('filled_size') or 0.0)
            redeemed = t.get('redeem_status') == 'success'
            # Count as filled if filled_size > 0 OR if successfully redeemed
            # (redemption proves the order was filled on-chain)
            if fs <= 0 and not redeemed:
                continue
            all_filled.append(t)
            resolved = t.get('resolved') or ''
            if resolved == 'win_unfilled' and redeemed:
                resolved = 'win'
            if resolved in ('win', 'loss'):
                resolved_filled.append(t)

        n_resolved = len(resolved_filled)
        wins = sum(1 for t in resolved_filled if t.get('resolved') == 'win')
        losses = n_resolved - wins
        win_rate = round(wins / n_resolved, 3) if n_resolved > 0 else None

        pa_row = pa_tuning.get(sym, {})
        applied_thr = pa_row.get('confidence_threshold') if isinstance(pa_row, dict) else None

        suggestion = None
        if n_resolved >= MIN_TRADES:
            sugg = _suggest_per_asset_tuning(symbols=[sym])
            if sym in sugg:
                suggestion = sugg[sym]

        symbols_out[sym] = {
            'resolved_filled':   n_resolved,
            'min_trades':        MIN_TRADES,
            'ready':             n_resolved >= MIN_TRADES,
            'wins':              wins,
            'losses':            losses,
            'win_rate':          win_rate,
            'applied_threshold': applied_thr,
            'suggestion':        suggestion,
            'collecting':        n_resolved < MIN_TRADES,
        }

    return JSONResponse({'ok': True, 'symbols': symbols_out})


@app.get('/api/window')
def api_window(request: Request):
    """Current 5-min window info: slug, countdown, trade zone status."""
    cfg = _effective_cfg()
    if not _allowed(request, cfg):
        return JSONResponse({'error': 'forbidden'}, status_code=403)
    if not _get_password() or not _is_authed(request):
        return JSONResponse({'error': 'unauthorized'}, status_code=401)

    local_now = time.time()
    aligned_now = polymarket_now()
    win = current_window(aligned_now)
    return JSONResponse({
        'slug': win.slug,
        'local_slug': current_window(local_now).slug,
        'start_ts': win.start_ts,
        'end_ts': win.end_ts,
        'seconds_remaining': seconds_remaining(aligned_now),
        'seconds_into': seconds_into_window(aligned_now),
        'is_trade_time': is_trade_time(cfg.trade_lead_seconds, aligned_now),
        'lead_seconds': cfg.trade_lead_seconds,
        'time_offset_sec': round(float(get_time_offset_seconds()), 3),
    })


@app.get('/api/resolution')
def api_resolution(request: Request):
    """Win/loss resolution stats."""
    cfg = _effective_cfg()
    if not _allowed(request, cfg):
        return JSONResponse({'error': 'forbidden'}, status_code=403)
    if not _get_password() or not _is_authed(request):
        return JSONResponse({'error': 'unauthorized'}, status_code=401)

    return JSONResponse(_resolution.load_stats())


_btc_price_cache: dict = {'price': 0.0, 'ts': 0.0}

# Per-symbol spot price cache for multi-market support.
_spot_price_cache: dict[str, dict] = {}  # symbol -> {'price': float, 'ts': float}

@app.get('/api/btc/price')
def api_btc_price(request: Request):
    """Live BTC-USD spot price from Coinbase (backward compat)."""
    import requests as _req
    cfg = _effective_cfg()
    if not _allowed(request, cfg):
        return JSONResponse({'error': 'forbidden'}, status_code=403)
    if not _get_password() or not _is_authed(request):
        return JSONResponse({'error': 'unauthorized'}, status_code=401)
    now = time.time()
    if now - _btc_price_cache['ts'] < 5:
        return JSONResponse(_btc_price_cache)
    try:
        r = _req.get('https://api.coinbase.com/v2/prices/BTC-USD/spot', timeout=5)
        p = float(r.json()['data']['amount'])
        _btc_price_cache.update({'price': p, 'ts': now})
    except Exception:
        pass
    return JSONResponse(_btc_price_cache)


@app.get('/api/active-symbol')
def api_active_symbol(request: Request):
    cfg = _effective_cfg()
    if not _get_password() or not _is_authed(request):
        from starlette.responses import PlainTextResponse
        return PlainTextResponse('BTC-USD')
    from starlette.responses import PlainTextResponse
    return PlainTextResponse(cfg.symbol or 'BTC-USD')


@app.get('/api/spot/price')
def api_spot_price(request: Request, symbol: str = ''):
    """Live spot price for any Coinbase-supported pair (e.g. ETH-USD, SOL-USD)."""
    import requests as _req
    cfg = _effective_cfg()
    if not _allowed(request, cfg):
        return JSONResponse({'error': 'forbidden'}, status_code=403)
    if not _get_password() or not _is_authed(request):
        return JSONResponse({'error': 'unauthorized'}, status_code=401)
    # Normalize symbol.
    _cfg2 = _effective_cfg()
    sym = symbol.upper().strip() or (_cfg2.symbol or 'BTC-USD').upper()
    if sym not in ('BTC-USD', 'ETH-USD', 'SOL-USD', 'XRP-USD'):
        sym = (_cfg2.symbol or 'BTC-USD').upper()
    now = time.time()
    cached = _spot_price_cache.get(sym, {'price': 0.0, 'ts': 0.0})
    if now - cached.get('ts', 0) < 5 and cached.get('price', 0) > 0:
        return JSONResponse({'symbol': sym, **cached})
    try:
        r = _req.get(f'https://api.coinbase.com/v2/prices/{sym}/spot', timeout=5)
        p = float(r.json()['data']['amount'])
        _spot_price_cache[sym] = {'price': p, 'ts': now}
        return JSONResponse({'symbol': sym, 'price': p, 'ts': now})
    except Exception:
        return JSONResponse({'symbol': sym, **cached})


@app.get('/api/mq')
def api_market_quality(request: Request, symbol: str = ''):
    """Return live market quality metrics (spread + depth) for UP/DOWN outcomes.

    Dashboard-only visibility helper.
    """

    cfg = _effective_cfg()
    if not _allowed(request, cfg):
        return JSONResponse({'error': 'forbidden'}, status_code=403)
    if not _get_password() or not _is_authed(request):
        return JSONResponse({'error': 'unauthorized'}, status_code=401)

    sym = (symbol or cfg.symbol or 'BTC-USD').upper().strip()

    # Load per-asset tuning overrides (optional)
    overrides = runtime_config.load_overrides()
    pa_all = _load_per_asset_tuning(overrides)
    pa = pa_all.get(sym, {}) if isinstance(pa_all, dict) else {}
    if not isinstance(pa, dict):
        pa = {}

    def _to_f(x: Any, d: float) -> float:
        try:
            return float(x)
        except Exception:
            return float(d)

    # Global MQ overrides take precedence over per-asset MQ fields.
    # Per-asset MQ fields were a mistake — they shadow the user's global settings.
    _global_spread = overrides.get('C5_MQ_MAX_SPREAD_BPS') or os.getenv('C5_MQ_MAX_SPREAD_BPS', '120') or '120'
    _global_depth  = overrides.get('C5_MQ_MIN_DEPTH_USDC') or os.getenv('C5_MQ_MIN_DEPTH_USDC', '15') or '15'
    max_spread_bps = _to_f(_global_spread, 120.0)
    min_depth_usdc = _to_f(_global_depth, 15.0)
    depth_cap_bps  = _to_f(overrides.get('C5_MQ_DEPTH_CAP_BPS') or os.getenv('C5_MQ_DEPTH_CAP_BPS', '30') or '30', 30.0)

    try:
        poly_cfg = _effective_poly_cfg()
        poly_exec = PolyExecutor(poly_cfg)

        # Use slug-based market discovery (same as the runner) — much more reliable
        # than the legacy text-search _find_market().
        from .window import current_window as _cw
        import time as _time
        _asset = sym.replace('-USD', '').lower()  # e.g. 'sol'
        _win = _cw(now=_time.time(), asset=_asset)
        market = poly_exec._find_market_by_slug(_win.slug)  # type: ignore[attr-defined]

        # If the current window isn't listed yet (happens in first ~30s), try the next one
        if not isinstance(market, dict):
            from .window import next_window as _nw
            _next = _nw(now=_time.time(), asset=_asset)
            market = poly_exec._find_market_by_slug(_next.slug)  # type: ignore[attr-defined]

        # Last resort: fall back to legacy text search
        if not isinstance(market, dict):
            market = poly_exec._find_market()  # type: ignore[attr-defined]

        if not isinstance(market, dict):
            return JSONResponse({'ok': False, 'error': 'market_not_found', 'symbol': sym,
                                 'tried_slug': _win.slug})

        up_tid, dn_tid = poly_exec._tokens_for_complement(market)  # type: ignore[attr-defined]
        if not up_tid or not dn_tid:
            return JSONResponse({'ok': False, 'error': 'missing_token_ids', 'symbol': sym})

        from .polymarket_orderbook import (
            fetch_orderbook_summary,
            best_ask,
            best_bid,
            depth_usdc_up_to_price,
        )

        def _mq_for_token(token_id: str) -> dict[str, Any]:
            book = fetch_orderbook_summary(clob_url=poly_cfg.clob_url, token_id=str(token_id), timeout=3.0)
            if book is None:
                return {'ok': False, 'token_id': str(token_id), 'error': 'orderbook_unavailable'}
            a = best_ask(book)
            b = best_bid(book)
            ask = float(a.price) if a else 0.0
            bid = float(b.price) if b else 0.0
            mid = (ask + bid) / 2.0 if (ask > 0 and bid > 0) else 0.0
            spread_bps = ((ask - bid) / mid * 10000.0) if mid > 0 else 0.0
            cap_px = ask * (1.0 + max(0.0, float(depth_cap_bps)) / 10000.0) if ask > 0 else 0.0
            depth = float(depth_usdc_up_to_price(book, cap_px)) if cap_px > 0 else 0.0

            passed = True
            if max_spread_bps > 0 and spread_bps > max_spread_bps:
                passed = False
            if min_depth_usdc > 0 and depth < min_depth_usdc:
                passed = False

            return {
                'ok': True,
                'token_id': str(token_id),
                'ask': round(ask, 4),
                'bid': round(bid, 4),
                'mid': round(mid, 4),
                'spread_bps': round(float(spread_bps), 2),
                'depth_cap_bps': round(float(depth_cap_bps), 2),
                'depth_usdc': round(float(depth), 2),
                'passes': bool(passed),
            }

        up = _mq_for_token(str(up_tid))
        dn = _mq_for_token(str(dn_tid))

        result = {
            'ok': True,
            'symbol': sym,
            'window_slug': market.get('slug', _win.slug),
            'thresholds': {
                'max_spread_bps': float(max_spread_bps),
                'min_depth_usdc': float(min_depth_usdc),
                'depth_cap_bps': float(depth_cap_bps),
            },
            'up': up,
            'down': dn,
        }
        # Cache successful result so failures can fall back to last-good data
        try:
            import time as _time
            result['cached_at'] = _time.time()
            MQ_CACHE_STORE.save(result)
        except Exception:
            pass
        return JSONResponse(result)
    except Exception as exc:
        # Try to return last cached result with a stale flag
        try:
            cached = MQ_CACHE_STORE.load(default=None)
            if cached and isinstance(cached, dict):
                cached['stale'] = True
                cached['stale_reason'] = str(exc)[:120]
                return JSONResponse(cached)
        except Exception:
            pass
        return JSONResponse({'ok': False, 'error': 'mq_failed', 'detail': str(exc)[:200], 'symbol': sym})


@app.get('/api/chainlink/prices')
def api_chainlink_prices(request: Request):
    """All Chainlink oracle prices (multi-asset)."""
    from .chainlink_feed import get_all_chainlink_snapshots
    cfg = _effective_cfg()
    if not _allowed(request, cfg):
        return JSONResponse({'error': 'forbidden'}, status_code=403)
    if not _get_password() or not _is_authed(request):
        return JSONResponse({'error': 'unauthorized'}, status_code=401)
    return JSONResponse(get_all_chainlink_snapshots())


@app.post('/settings/symbols')
def settings_symbols(request: Request, symbols: str = Form(''), primary: str = Form('')):
    cfg = _effective_cfg()
    if not _allowed(request, cfg):
        return HTMLResponse('Forbidden', status_code=403)
    if not _is_authed(request):
        return RedirectResponse(url='/login', status_code=302)

    # Validate symbols list.
    items = [x.strip().upper() for x in (symbols or '').split(',') if x.strip()]
    items = items[:12]
    patch: dict[str, Any] = {}
    patch['C5_SYMBOLS'] = ','.join(items) if items else ''
    # Primary must be in the submitted list; fall back to first item if not.
    p = (primary or '').strip().upper()
    if items:
        patch['C5_SYMBOL'] = p if p in items else items[0]
    runtime_config.update_overrides(patch)
    return RedirectResponse(url='/', status_code=302)


@app.post('/settings/polymarket')
def settings_polymarket(
    request: Request,
    enabled: str = Form(''),
    dry_run: str = Form(''),
    arb_enabled: str = Form(''),
    arb_min_edge_cents: str = Form(''),
    arb_taker_fee_bps: str = Form(''),
    arb_slippage_bps: str = Form(''),
    snipe_enabled: str = Form(''),
    snipe_lead_seconds: str = Form(''),
    snipe_min_delta: str = Form(''),
    snipe_bet_multiplier: str = Form(''),
    delta_first: str = Form(''),
    delta_pricing: str = Form(''),
    high_risk: str = Form(''),
    expert_mode: str = Form(''),
    expert_ack: str = Form(''),
    auto_redeem: str = Form(''),
    force_gtc: str = Form(''),
    market_query: str = Form(''),
    outcome_up: str = Form(''),
    outcome_down: str = Form(''),
    max_usdc: str = Form(''),
    bet_mode: str = Form(''),
    bet_percent: str = Form(''),
    kelly_fraction: str = Form(''),
    cooldown_seconds: str = Form(''),
    trade_lead_seconds: str = Form(''),
    edge_min: str = Form(''),
    ask_mode: str = Form(''),
):
    cfg = _effective_cfg()
    if not _allowed(request, cfg):
        return HTMLResponse('Forbidden', status_code=403)
    if not _is_authed(request):
        return RedirectResponse(url='/login', status_code=302)

    is_enabled = (enabled or '').lower() in {'1', 'true', 'yes', 'on'}
    is_dry = (dry_run or '').lower() in {'1', 'true', 'yes', 'on'}
    is_arb = (arb_enabled or '').lower() in {'1', 'true', 'yes', 'on'}
    is_snipe = (snipe_enabled or '').lower() in {'1', 'true', 'yes', 'on'}
    is_high_risk = (high_risk or '').lower() in {'1', 'true', 'yes', 'on'}

    # Expert/UNSAFE mode: allow uncapped bet% up to 100%.
    # Typed confirm is required to ENABLE, but not required to keep enabled.
    is_expert_checked = (expert_mode or '').lower() in {'1', 'true', 'yes', 'on'}
    ack_ok = (expert_ack or '').strip().lower() == 'i understand'
    existing = runtime_config.load_overrides()
    was_expert = str(existing.get('C5_POLY_EXPERT_MODE', '')).strip().lower() in {'1', 'true', 'yes', 'on'}
    effective_expert = bool(is_expert_checked and (was_expert or ack_ok))

    is_redeem_checked = (auto_redeem or '').lower() in {'1', 'true', 'yes', 'on'}
    # Auto-redeem is a normal toggle (default ON). No typed confirmation.
    effective_redeem = bool(is_redeem_checked)

    is_force_gtc = (force_gtc or '').lower() in {'1', 'true', 'yes', 'on'}

    patch: dict[str, Any] = {}
    patch['C5_MODE'] = 'polymarket' if is_enabled else 'paper'
    patch['C5_POLY_DRY_RUN'] = 'true' if is_dry else 'false'
    patch['C5_POLY_ARB_ENABLED'] = 'true' if is_arb else 'false'
    patch['C5_SNIPE_ENABLED'] = 'true' if is_snipe else 'false'
    patch['C5_DELTA_FIRST'] = 'true' if (delta_first or '').lower() in {'1', 'true', 'yes', 'on'} else 'false'
    patch['C5_DELTA_PRICING'] = 'true' if (delta_pricing or '').lower() in {'1', 'true', 'yes', 'on'} else 'false'
    patch['C5_POLY_FORCE_GTC'] = 'true' if is_force_gtc else 'false'
    try:
        if snipe_lead_seconds.strip():
            v = max(1, int(snipe_lead_seconds))
            patch['C5_SNIPE_LEAD_SECONDS'] = str(v)
    except Exception:
        pass
    try:
        if snipe_min_delta.strip():
            v = max(0.001, float(snipe_min_delta))
            patch['C5_SNIPE_MIN_DELTA_PCT'] = str(v)
    except Exception:
        pass
    try:
        if snipe_bet_multiplier.strip():
            v = max(1.0, float(snipe_bet_multiplier))
            patch['C5_POLY_SNIPE_BET_MULTIPLIER'] = v
    except Exception:
        pass
    try:
        if trade_lead_seconds.strip():
            v = max(1, int(float(trade_lead_seconds)))
            patch['C5_POLY_TRADE_LEAD_SECONDS'] = str(v)
    except Exception:
        pass
    try:
        if edge_min.strip():
            v = max(0.0, min(1.0, float(edge_min)))
            patch['C5_POLY_EDGE_MIN'] = v
    except Exception:
        pass

    # Price source selection for base ask.
    # - prefer_live: use live CLOB ask when available, else Gamma
    # - legacy_max:  use max(live_ask, gamma_ask)
    if ask_mode.strip().lower() in {'prefer_live', 'legacy_max'}:
        patch['C5_POLY_ASK_MODE'] = ask_mode.strip().lower()
    patch['C5_POLY_HIGH_RISK_MODE'] = 'true' if is_high_risk else 'false'
    # If the user unchecks expert mode, disable immediately.
    # If they check it, only enable if previously enabled OR typed confirm is correct.
    patch['C5_POLY_EXPERT_MODE'] = 'true' if effective_expert else 'false'
    patch['C5_POLY_AUTO_REDEEM_ENABLED'] = 'true' if effective_redeem else 'false'
    if market_query.strip():
        patch['C5_POLY_MARKET_QUERY'] = market_query.strip()
    if outcome_up.strip():
        patch['C5_POLY_OUTCOME_UP'] = outcome_up.strip()
    if outcome_down.strip():
        patch['C5_POLY_OUTCOME_DOWN'] = outcome_down.strip()

    try:
        if max_usdc.strip():
            v = max(0.0, float(max_usdc))
            patch['C5_POLY_MAX_USDC_PER_TRADE'] = v
    except Exception:
        pass

    # Arb knobs (noob-friendly): clamp to reasonable ranges.
    try:
        if arb_min_edge_cents.strip():
            v_edge = float(arb_min_edge_cents)
            v_edge = max(0.0, min(25.0, v_edge))
            patch['C5_POLY_ARB_MIN_EDGE_CENTS'] = v_edge
    except Exception:
        pass
    try:
        if arb_taker_fee_bps.strip():
            v_fee = float(arb_taker_fee_bps)
            v_fee = max(0.0, min(500.0, v_fee))
            patch['C5_POLY_ARB_TAKER_FEE_BPS'] = v_fee
    except Exception:
        pass
    try:
        if arb_slippage_bps.strip():
            v_slip = float(arb_slippage_bps)
            v_slip = max(0.0, min(500.0, v_slip))
            patch['C5_POLY_ARB_SLIPPAGE_BPS'] = v_slip
    except Exception:
        pass
    if bet_mode.strip().lower() in {'fixed', 'percent', 'kelly'}:
        patch['C5_POLY_BET_MODE'] = bet_mode.strip().lower()
    try:
        if bet_percent.strip():
            v_pct = max(0.0, min(100.0, float(bet_percent)))
            # Safety rail: default cap at 10% unless explicitly unlocked.
            cap = 100.0 if effective_expert else (50.0 if is_high_risk else 10.0)
            v_pct = min(v_pct, cap)
            patch['C5_POLY_BET_PERCENT'] = v_pct
    except Exception:
        pass
    try:
        if kelly_fraction.strip():
            v_k = float(kelly_fraction)
            v_k = max(0.0, min(1.0, v_k))
            patch['C5_POLY_KELLY_FRACTION'] = v_k
    except Exception:
        pass
    try:
        if cooldown_seconds.strip():
            v2 = max(10, int(float(cooldown_seconds)))
            patch['C5_POLY_COOLDOWN_SECONDS'] = v2
    except Exception:
        pass

    runtime_config.update_overrides(patch)
    return RedirectResponse(url='/', status_code=302)


@app.post('/settings/risk')
def settings_risk(
    request: Request,
    risk_daily_loss_pct: str = Form(''),
    risk_consec_loss_limit: str = Form(''),
    risk_unfilled_ratio: str = Form(''),
    risk_unfilled_lookback: str = Form(''),
    risk_auto_resume_minutes: str = Form(''),
):
    cfg = _effective_cfg()
    if not _allowed(request, cfg):
        return HTMLResponse('Forbidden', status_code=403)
    if not _is_authed(request):
        return RedirectResponse(url='/login', status_code=302)

    patch: dict[str, Any] = {}
    try:
        if risk_daily_loss_pct.strip():
            v = max(0.0, min(100.0, float(risk_daily_loss_pct)))
            patch['C5_RISK_DAILY_LOSS_PCT'] = v
    except Exception:
        pass
    try:
        if risk_consec_loss_limit.strip():
            v = max(0, int(float(risk_consec_loss_limit)))
            patch['C5_RISK_CONSEC_LOSS_LIMIT'] = v
    except Exception:
        pass
    try:
        if risk_unfilled_ratio.strip():
            v = max(0.0, min(1.0, float(risk_unfilled_ratio)))
            patch['C5_RISK_UNFILLED_RATIO'] = v
    except Exception:
        pass
    try:
        if risk_unfilled_lookback.strip():
            v = max(1, int(float(risk_unfilled_lookback)))
            patch['C5_RISK_UNFILLED_LOOKBACK'] = v
    except Exception:
        pass
    try:
        if risk_auto_resume_minutes.strip():
            v = max(0, int(float(risk_auto_resume_minutes)))
            patch['C5_RISK_AUTO_RESUME_MINUTES'] = v
    except Exception:
        pass

    runtime_config.update_overrides(patch)
    return RedirectResponse(url='/', status_code=302)


@app.post('/settings/model')
def settings_model(
    request: Request,
    confidence_threshold: str = Form(''),
    incremental_candles: str = Form(''),
    rtds_json_ping_enabled: str = Form(''),
    rtds_json_ping_interval_sec: str = Form(''),
    chainlink_stale_threshold_sec: str = Form(''),
):
    cfg = _effective_cfg()
    if not _allowed(request, cfg):
        return HTMLResponse('Forbidden', status_code=403)
    if not _is_authed(request):
        return RedirectResponse(url='/login', status_code=302)

    patch: dict[str, Any] = {}

    try:
        if confidence_threshold.strip():
            v = float(confidence_threshold)
            v = max(0.5, min(0.99, v))
            patch['C5_CONFIDENCE_THRESHOLD'] = v
    except Exception:
        pass

    # Data fetching performance
    patch['C5_COINBASE_INCREMENTAL_CANDLES'] = 'true' if (incremental_candles or '').lower() in {'1', 'true', 'yes', 'on'} else 'false'

    # RTDS feed keepalive (JSON ping messages)
    patch['C5_RTDS_JSON_PING_ENABLED'] = 'true' if (rtds_json_ping_enabled or '').lower() in {'1', 'true', 'yes', 'on'} else 'false'
    try:
        if rtds_json_ping_interval_sec.strip():
            v = max(1.0, float(rtds_json_ping_interval_sec))
            patch['C5_RTDS_JSON_PING_INTERVAL_SEC'] = v
    except Exception:
        pass

    try:
        if chainlink_stale_threshold_sec.strip():
            v = max(5.0, float(chainlink_stale_threshold_sec))
            patch['C5_CHAINLINK_STALE_THRESHOLD_SEC'] = v
    except Exception:
        pass

    if patch:
        runtime_config.update_overrides(patch)
    return RedirectResponse(url='/', status_code=302)


@app.post('/settings/market_quality')
def settings_market_quality(
    request: Request,
    mq_max_spread_bps: str = Form(''),
    mq_min_depth_usdc: str = Form(''),
    mq_depth_cap_bps: str = Form(''),
    mq_edge_spread_mult: str = Form(''),
    mq_min_book_usdc: str = Form(''),
):
    cfg = _effective_cfg()
    if not _allowed(request, cfg):
        return HTMLResponse('Forbidden', status_code=403)
    if not _is_authed(request):
        return RedirectResponse(url='/login', status_code=302)

    patch: dict[str, Any] = {}
    try:
        if mq_max_spread_bps.strip():
            patch['C5_MQ_MAX_SPREAD_BPS'] = max(0.0, float(mq_max_spread_bps))
    except Exception:
        pass
    try:
        if mq_min_depth_usdc.strip():
            patch['C5_MQ_MIN_DEPTH_USDC'] = max(0.0, float(mq_min_depth_usdc))
    except Exception:
        pass
    try:
        if mq_depth_cap_bps.strip():
            patch['C5_MQ_DEPTH_CAP_BPS'] = max(0.0, float(mq_depth_cap_bps))
    except Exception:
        pass
    try:
        if mq_edge_spread_mult.strip():
            patch['C5_MQ_EDGE_SPREAD_MULT'] = max(0.0, float(mq_edge_spread_mult))
    except Exception:
        pass
    try:
        if mq_min_book_usdc.strip():
            patch['C5_POLY_MIN_BOOK_USDC'] = max(0.0, float(mq_min_book_usdc))
    except Exception:
        pass

    if patch:
        runtime_config.update_overrides(patch)
    return RedirectResponse(url='/', status_code=302)


@app.post('/settings/early_exit')
def settings_early_exit(
    request: Request,
    early_exit_enabled: str = Form(''),
    early_exit_trail_pct: str = Form(''),
    early_exit_trail_act_pct: str = Form(''),
    early_exit_sl_pct: str = Form(''),
):
    cfg = _effective_cfg()
    if not _allowed(request, cfg):
        return HTMLResponse('Forbidden', status_code=403)
    if not _is_authed(request):
        return RedirectResponse(url='/login', status_code=302)

    patch: dict[str, Any] = {}
    if early_exit_enabled.strip():
        patch['C5_EARLY_EXIT_ENABLED'] = early_exit_enabled.strip().lower() in ('true', '1', 'yes')
    try:
        if early_exit_trail_pct.strip():
            patch['C5_EARLY_EXIT_TRAIL_PCT'] = max(1.0, min(50.0, float(early_exit_trail_pct)))
    except Exception:
        pass
    try:
        if early_exit_trail_act_pct.strip():
            patch['C5_EARLY_EXIT_TRAIL_ACT_PCT'] = max(1.0, min(50.0, float(early_exit_trail_act_pct)))
    except Exception:
        pass
    try:
        if early_exit_sl_pct.strip():
            patch['C5_EARLY_EXIT_SL_PCT'] = max(1.0, min(99.0, float(early_exit_sl_pct)))
    except Exception:
        pass

    if patch:
        runtime_config.update_overrides(patch)
    return RedirectResponse(url='/', status_code=302)


@app.post('/settings/per_asset')
async def settings_per_asset(request: Request):
    """Save per-asset tuning from the dashboard."""

    cfg = _effective_cfg()
    if not _allowed(request, cfg):
        return HTMLResponse('Forbidden', status_code=403)
    if not _is_authed(request):
        return RedirectResponse(url='/login', status_code=302)

    form = await request.form()
    overrides = runtime_config.load_overrides()
    cur = _load_per_asset_tuning(overrides)

    # Build next per-asset dict from submitted fields.
    nxt: dict[str, Any] = dict(cur) if isinstance(cur, dict) else {}
    symbols = list(cfg.symbols or [cfg.symbol])

    def _sym_key(sym: str) -> str:
        return sym.replace('-', '_').replace('/', '_').upper()

    for sym in symbols:
        k = _sym_key(sym)
        row = dict(nxt.get(sym) or {}) if isinstance(nxt.get(sym), dict) else {}

        def _get(name: str) -> str:
            v = form.get(f'pa_{k}_{name}')
            return str(v) if v is not None else ''

        # Confidence threshold (0.50–0.99)
        try:
            v = _get('confidence_threshold').strip()
            if v:
                row['confidence_threshold'] = max(0.5, min(0.99, float(v)))
        except Exception:
            pass

        # Edge gate (probability edge)
        try:
            v = _get('edge_min').strip()
            if v:
                row['edge_min'] = max(0.0, float(v))
        except Exception:
            pass

        # Market quality knobs
        try:
            v = _get('mq_max_spread_bps').strip()
            if v:
                row['mq_max_spread_bps'] = max(0.0, float(v))
        except Exception:
            pass
        try:
            v = _get('mq_min_depth_usdc').strip()
            if v:
                row['mq_min_depth_usdc'] = max(0.0, float(v))
        except Exception:
            pass

        # Save row only if it has at least one key.
        if row:
            nxt[sym] = row

    try:
        import json
        runtime_config.update_overrides({'C5_PER_ASSET_TUNING_JSON': json.dumps(nxt, separators=(',', ':'))})
    except Exception:
        pass

    return RedirectResponse(url='/', status_code=302)


@app.post('/settings/per_asset/apply_suggestions')
def settings_per_asset_apply_suggestions(request: Request):
    """Compute per-asset suggestions and write them into overrides.

    This is an opt-in convenience for users who want the bot to adapt over time.
    It only adjusts the *confidence_threshold* per asset.
    """
    cfg = _effective_cfg()
    if not _allowed(request, cfg):
        return HTMLResponse('Forbidden', status_code=403)
    if not _is_authed(request):
        return RedirectResponse(url='/login', status_code=302)

    overrides = runtime_config.load_overrides()
    cur = _load_per_asset_tuning(overrides)
    if not isinstance(cur, dict):
        cur = {}

    symbols = list(cfg.symbols or [cfg.symbol])
    sugg = _suggest_per_asset_tuning(symbols=symbols)

    # Merge: preserve existing per-asset keys but overwrite confidence_threshold.
    nxt: dict[str, Any] = dict(cur)
    for sym, row in sugg.items():
        if not isinstance(row, dict):
            continue
        thr = row.get('confidence_threshold')
        try:
            thr_f = float(thr)
        except Exception:
            continue
        cur_row = nxt.get(sym)
        if not isinstance(cur_row, dict):
            cur_row = {}
        # Auto-apply only touches confidence_threshold — never MQ spread/depth
        # fields, which are global settings the user controls separately.
        cur_row['confidence_threshold'] = max(0.5, min(0.99, thr_f))
        # Strip MQ fields so they don't shadow the global Market Quality settings
        cur_row.pop('mq_max_spread_bps', None)
        cur_row.pop('mq_min_depth_usdc', None)
        nxt[sym] = cur_row

    try:
        import json
        runtime_config.update_overrides({'C5_PER_ASSET_TUNING_JSON': json.dumps(nxt, separators=(',', ':'))})
    except Exception:
        pass

    return RedirectResponse(url='/?tab=tab-signal', status_code=302)


@app.post('/settings/gas')
def settings_gas(
    request: Request,
    native_gas_symbol: str = Form(''),
    native_gas_min: str = Form(''),
    gas_topup_enabled: str = Form(''),
    gas_topup_target_native: str = Form(''),
    gas_topup_max_usdc: str = Form(''),
):
    cfg = _effective_cfg()
    if not _allowed(request, cfg):
        return HTMLResponse('Forbidden', status_code=403)
    if not _is_authed(request):
        return RedirectResponse(url='/login', status_code=302)

    patch: dict[str, Any] = {}
    if native_gas_symbol.strip():
        patch['C5_NATIVE_GAS_SYMBOL'] = native_gas_symbol.strip().upper()

    try:
        if native_gas_min.strip():
            v = max(0.0, float(native_gas_min))
            patch['C5_NATIVE_GAS_MIN'] = v
    except Exception:
        pass

    patch['C5_GAS_TOPUP_ENABLED'] = 'true' if (gas_topup_enabled or '').lower() in {'1', 'true', 'yes', 'on'} else 'false'

    try:
        if gas_topup_target_native.strip():
            v = max(0.0, float(gas_topup_target_native))
            patch['C5_GAS_TOPUP_TARGET_NATIVE'] = v
    except Exception:
        pass

    try:
        if gas_topup_max_usdc.strip():
            v = max(0.0, float(gas_topup_max_usdc))
            patch['C5_GAS_TOPUP_MAX_USDC'] = v
    except Exception:
        pass

    runtime_config.update_overrides(patch)
    return RedirectResponse(url='/', status_code=302)


@app.post('/mode/paper')
def mode_paper(request: Request):
    cfg = _effective_cfg()
    if not _allowed(request, cfg):
        return HTMLResponse('Forbidden', status_code=403)
    if not _is_authed(request):
        return RedirectResponse(url='/login', status_code=302)
    runtime_config.update_overrides({'C5_MODE': 'paper', 'C5_POLY_DRY_RUN': 'true'})
    # Use HX-Refresh so HTMX reloads the page in-place (avoids 302 → /setup loop).
    return Response(status_code=200, headers={'HX-Refresh': 'true'})


@app.post('/mode/dry')
def mode_dry(request: Request):
    cfg = _effective_cfg()
    if not _allowed(request, cfg):
        return HTMLResponse('Forbidden', status_code=403)
    if not _is_authed(request):
        return RedirectResponse(url='/login', status_code=302)
    runtime_config.update_overrides({'C5_MODE': 'polymarket', 'C5_POLY_DRY_RUN': 'true'})
    return Response(status_code=200, headers={'HX-Refresh': 'true'})


@app.post('/mode/live')
def mode_live(request: Request):
    cfg = _effective_cfg()
    if not _allowed(request, cfg):
        return HTMLResponse('Forbidden', status_code=403)
    if not _is_authed(request):
        return RedirectResponse(url='/login', status_code=302)
    runtime_config.update_overrides({'C5_MODE': 'polymarket', 'C5_POLY_DRY_RUN': 'false'})
    return Response(status_code=200, headers={'HX-Refresh': 'true'})


@app.post('/poly/sell_all')
def poly_sell_all(request: Request, confirm: str = Form('')):
    cfg = _effective_cfg()
    if not _allowed(request, cfg):
        return JSONResponse({'error': 'forbidden'}, status_code=403)
    if not _is_authed(request):
        return JSONResponse({'error': 'unauthorized'}, status_code=401)

    if not sell_all_enabled():
        return JSONResponse({'error': 'sell_all_disabled', 'hint': 'Set C5_POLY_SELL_ALL_ENABLED=true'}, status_code=400)

    if (confirm or '').strip().lower() not in {'sell all', 'i understand', 'confirm'}:
        return JSONResponse({'error': 'confirm_required', 'hint': 'send confirm=SELL ALL'}, status_code=400)

    poly_cfg = _effective_poly_cfg()
    # If execution is disabled, we still allow dry-run to show planned orders.
    dry = bool(poly_cfg.dry_run) or (not bool(poly_cfg.enabled))
    result = close_all_positions_from_env(dry_run=dry)
    entry = {'ts': int(time.time()), 'action': 'sell_all', 'dry_run': dry, 'result': result}
    log = _load_json(POLY_OPS_LOG_STORE, default=[]) or []
    if not isinstance(log, list):
        log = []
    log.append(entry)
    log = log[-100:]
    POLY_OPS_LOG_STORE.save(log)
    return JSONResponse(result)


@app.post('/poly/redeem/preview')
def poly_redeem_preview(request: Request):
    cfg = _effective_cfg()
    if not _allowed(request, cfg):
        return JSONResponse({'error': 'forbidden'}, status_code=403)
    if not _is_authed(request):
        return JSONResponse({'error': 'unauthorized'}, status_code=401)

    # Preview can run even if disabled; we return a hint.
    enabled = bool(_redeem.is_redeem_enabled())
    try:
        max_trades = int(float(os.getenv('C5_POLY_MAX_REDEEM_TRADES_PER_RUN', '5') or '5'))
    except Exception:
        max_trades = 5
    try:
        retry_minutes = int(float(os.getenv('C5_POLY_REDEEM_RETRY_MINUTES', '60') or '60'))
    except Exception:
        retry_minutes = 60
    cand = _redeem.find_redeem_candidates(
        max_trades=max(1, max_trades),
        retry_minutes=max(5, retry_minutes),
    )
    return JSONResponse({'ok': True, 'enabled': enabled, **cand})


@app.post('/poly/redeem/confirm')
def poly_redeem_confirm(request: Request, confirm: str = Form('')):
    cfg = _effective_cfg()
    if not _allowed(request, cfg):
        return JSONResponse({'error': 'forbidden'}, status_code=403)
    if not _is_authed(request):
        return JSONResponse({'error': 'unauthorized'}, status_code=401)

    if (confirm or '').strip().lower() not in {'i understand', 'redeem', 'confirm'}:
        return JSONResponse({'error': 'confirm_required', 'hint': 'send confirm=I understand'}, status_code=400)

    if not _redeem.is_redeem_enabled():
        return JSONResponse({'error': 'redeem_disabled', 'hint': 'Set C5_POLY_AUTO_REDEEM_ENABLED=true'}, status_code=400)

    poly_cfg = _effective_poly_cfg()
    dry = bool(poly_cfg.dry_run) or (not bool(poly_cfg.enabled))
    res = _redeem.process_auto_redeem(dry_run=dry)

    # If the redeem attempt hit a known transaction send issue, return a
    # controlled error response (so the UI shows a useful message instead of a
    # generic "Internal Server Error").
    try:
        results = res.get('results') if isinstance(res, dict) else None
        bad: list[dict] = []
        if isinstance(results, list):
            for r in results:
                if isinstance(r, dict) and (r.get('ok') is False):
                    bad.append(r)
        if bad:
            # Prefer a specific status when nonce/gas replacement issues occur.
            err_codes = {str(x.get('error') or '') for x in bad}
            status = 409 if 'replacement_transaction_underpriced' in err_codes else 400
            hint = 'Redeem failed. Check Ops & Logs and try again in a minute.'
            if status == 409:
                hint = 'A previous Polygon tx may still be pending. Wait a bit, then try again.'
            return JSONResponse({'error': 'redeem_failed', 'hint': hint, 'details': bad[-3:], 'result': res}, status_code=status)
    except Exception:
        pass
    entry = {'ts': int(time.time()), 'action': 'redeem_confirm', 'dry_run': dry, 'result': res}
    log = _load_json(POLY_OPS_LOG_STORE, default=[]) or []
    if not isinstance(log, list):
        log = []
    log.append(entry)
    log = log[-200:]
    POLY_OPS_LOG_STORE.save(log)
    return JSONResponse(res)


@app.post('/gas/topup/preview')
def gas_topup_preview(request: Request):
    cfg = _effective_cfg()
    if not _allowed(request, cfg):
        return JSONResponse({'error': 'forbidden'}, status_code=403)
    if not _is_authed(request):
        return JSONResponse({'error': 'unauthorized'}, status_code=401)

    pk = os.getenv('C5_POLY_PRIVATE_KEY', '').strip()
    if not pk:
        return JSONResponse({'error': 'missing_private_key'}, status_code=400)

    try:
        from eth_account import Account  # type: ignore

        addr = Account.from_key(pk).address
    except Exception as e:
        return JSONResponse({'error': 'bad_private_key', 'detail': str(e)}, status_code=400)

    # Use last snapshot balance when available (cheap); otherwise compute quickly.
    snap = _load_json(JsonStore(Path('logs') / 'poly_snapshot.json'), default=None)
    cur = None
    if isinstance(snap, dict):
        try:
            cur = float(snap.get('native_gas_balance'))
        except Exception:
            cur = None
    if cur is None:
        try:
            from .polymarket_account import native_gas_balance

            cur = float(native_gas_balance(addr, rpc_url=os.getenv('C5_POLYGON_RPC', 'https://polygon-bor-rpc.publicnode.com')))
        except Exception:
            cur = 0.0

    res = _gas.preview_topup(from_address=addr, current_native=float(cur))
    return JSONResponse(res)


@app.post('/gas/topup/confirm')
def gas_topup_confirm(request: Request, confirm: str = Form('')):
    cfg = _effective_cfg()
    if not _allowed(request, cfg):
        return JSONResponse({'error': 'forbidden'}, status_code=403)
    if not _is_authed(request):
        return JSONResponse({'error': 'unauthorized'}, status_code=401)

    if (confirm or '').strip().lower() not in {'i understand', 'confirm', 'top up'}:
        return JSONResponse({'error': 'confirm_required', 'hint': 'send confirm=I understand'}, status_code=400)

    pk = os.getenv('C5_POLY_PRIVATE_KEY', '').strip()
    if not pk:
        return JSONResponse({'error': 'missing_private_key'}, status_code=400)

    try:
        from eth_account import Account  # type: ignore

        addr = Account.from_key(pk).address
    except Exception as e:
        return JSONResponse({'error': 'bad_private_key', 'detail': str(e)}, status_code=400)

    # Read latest known gas balance.
    snap = _load_json(JsonStore(Path('logs') / 'poly_snapshot.json'), default=None)
    cur = 0.0
    if isinstance(snap, dict):
        try:
            cur = float(snap.get('native_gas_balance') or 0.0)
        except Exception:
            cur = 0.0

    res = _gas.execute_topup(private_key=pk, from_address=addr, current_native=float(cur))

    # Ops log entry.
    try:
        entry = {'ts': int(time.time()), 'action': 'gas_topup', 'result': res}
        log = _load_json(POLY_OPS_LOG_STORE, default=[]) or []
        if not isinstance(log, list):
            log = []
        log.append(entry)
        log = log[-200:]
        POLY_OPS_LOG_STORE.save(log)
    except Exception:
        pass

    status = 200 if res.get('ok') else 400
    return JSONResponse(res, status_code=status)


@app.post('/withdraw/all/preview')
def withdraw_all_preview(request: Request):
    cfg = _effective_cfg()
    if not _allowed(request, cfg):
        return JSONResponse({'error': 'forbidden'}, status_code=403)
    if not _is_authed(request):
        return JSONResponse({'error': 'unauthorized'}, status_code=401)

    wc = WithdrawConfig.from_env()
    if not wc.enabled:
        return JSONResponse({'error': 'withdraw_disabled'}, status_code=400)
    if not wc.to_address:
        return JSONResponse({'error': 'missing_to_address'}, status_code=400)
    pk = os.getenv('C5_POLY_PRIVATE_KEY', '')
    if not pk:
        return JSONResponse({'error': 'missing_private_key'}, status_code=400)

    # We compute how much would be sent (capped by max_usdc).
    try:
        # send_usdc_all will compute balance and cap; we call balance logic indirectly
        from eth_account import Account  # type: ignore

        addr = Account.from_key(pk).address
        # lazy import to avoid making withdraw preview depend on web3 at import-time
        from .withdraw import usdc_balance

        bal = usdc_balance(addr, rpc_url=wc.rpc_url, usdc_address=wc.usdc_address)
        amt = min(float(bal), float(wc.max_usdc))
    except Exception as e:
        return JSONResponse({'error': 'balance_check_failed', 'detail': str(e)}, status_code=500)

    return JSONResponse(
        {
            'ok': True,
            'from': addr,
            'to_address': wc.to_address,
            'wallet_usdc_balance': float(bal),
            'max_usdc': float(wc.max_usdc),
            'would_send_usdc': float(amt),
            'rpc_url': wc.rpc_url,
            'usdc_address': wc.usdc_address,
        }
    )


@app.post('/withdraw/all/confirm')
def withdraw_all_confirm(request: Request, confirm: str = Form('')):
    cfg = _effective_cfg()
    if not _allowed(request, cfg):
        return JSONResponse({'error': 'forbidden'}, status_code=403)
    if not _is_authed(request):
        return JSONResponse({'error': 'unauthorized'}, status_code=401)

    if (confirm or '').strip().lower() not in {'i understand', 'confirm', 'yes'}:
        return JSONResponse({'error': 'confirm_required', 'hint': 'send confirm=I understand'}, status_code=400)

    wc = WithdrawConfig.from_env()
    if not wc.enabled:
        return JSONResponse({'error': 'withdraw_disabled'}, status_code=400)
    pk = os.getenv('C5_POLY_PRIVATE_KEY', '')
    if not pk:
        return JSONResponse({'error': 'missing_private_key'}, status_code=400)
    if not wc.to_address:
        return JSONResponse({'error': 'missing_to_address'}, status_code=400)

    try:
        res = send_usdc_all(pk, to_address=wc.to_address, rpc_url=wc.rpc_url, usdc_address=wc.usdc_address, max_usdc=wc.max_usdc)
    except Exception as e:
        return JSONResponse({'error': 'send_failed', 'detail': str(e)}, status_code=500)

    entry = {
        'ts': int(time.time()),
        'to': wc.to_address,
        'amount_usdc': float(res.get('sent_usdc', 0.0)),
        'tx_hash': res.get('tx_hash'),
        'mode': 'withdraw_all',
    }
    log = _load_json(WITHDRAW_LOG_STORE, default=[]) or []
    if not isinstance(log, list):
        log = []
    log.append(entry)
    log = log[-200:]
    WITHDRAW_LOG_STORE.save(log)
    return JSONResponse({'ok': True, **res})


@app.post('/withdraw/preview')
def withdraw_preview(request: Request, amount_usdc: str = Form('0')):
    cfg = _effective_cfg()
    if not _allowed(request, cfg):
        return JSONResponse({'error': 'forbidden'}, status_code=403)
    if not _is_authed(request):
        return JSONResponse({'error': 'unauthorized'}, status_code=401)

    wc = WithdrawConfig.from_env()
    if not wc.enabled:
        return JSONResponse({'error': 'withdraw_disabled'}, status_code=400)
    try:
        amt = float(amount_usdc)
    except Exception:
        return JSONResponse({'error': 'invalid_amount'}, status_code=400)
    if amt <= 0:
        return JSONResponse({'error': 'invalid_amount'}, status_code=400)
    if amt > float(wc.max_usdc):
        return JSONResponse({'error': 'exceeds_max', 'max_usdc': wc.max_usdc}, status_code=400)
    if not wc.to_address:
        return JSONResponse({'error': 'missing_to_address'}, status_code=400)

    return JSONResponse(
        {
            'ok': True,
            'to_address': wc.to_address,
            'amount_usdc': amt,
            'max_usdc': wc.max_usdc,
            'rpc_url': wc.rpc_url,
            'usdc_address': wc.usdc_address,
        }
    )


@app.post('/withdraw/confirm')
def withdraw_confirm(request: Request, amount_usdc: str = Form('0'), confirm: str = Form('')):
    cfg = _effective_cfg()
    if not _allowed(request, cfg):
        return JSONResponse({'error': 'forbidden'}, status_code=403)
    if not _is_authed(request):
        return JSONResponse({'error': 'unauthorized'}, status_code=401)

    if (confirm or '').strip().lower() not in {'i understand', 'confirm', 'yes'}:
        return JSONResponse({'error': 'confirm_required', 'hint': 'send confirm=I understand'}, status_code=400)

    wc = WithdrawConfig.from_env()
    if not wc.enabled:
        return JSONResponse({'error': 'withdraw_disabled'}, status_code=400)
    pk = os.getenv('C5_POLY_PRIVATE_KEY', '')
    if not pk:
        return JSONResponse({'error': 'missing_private_key'}, status_code=400)

    try:
        amt = float(amount_usdc)
    except Exception:
        return JSONResponse({'error': 'invalid_amount'}, status_code=400)
    if amt <= 0:
        return JSONResponse({'error': 'invalid_amount'}, status_code=400)
    if amt > float(wc.max_usdc):
        return JSONResponse({'error': 'exceeds_max', 'max_usdc': wc.max_usdc}, status_code=400)
    if not wc.to_address:
        return JSONResponse({'error': 'missing_to_address'}, status_code=400)

    try:
        tx = send_usdc(pk, to_address=wc.to_address, amount_usdc=amt, rpc_url=wc.rpc_url, usdc_address=wc.usdc_address)
    except Exception as e:
        return JSONResponse({'error': 'send_failed', 'detail': str(e)}, status_code=500)

    entry = {'ts': int(time.time()), 'to': wc.to_address, 'amount_usdc': amt, 'tx_hash': tx}
    log = _load_json(WITHDRAW_LOG_STORE, default=[]) or []
    if not isinstance(log, list):
        log = []
    log.append(entry)
    log = log[-200:]
    WITHDRAW_LOG_STORE.save(log)
    return JSONResponse({'ok': True, 'tx_hash': tx})


def _bg_loop() -> None:
    # ── Start Chainlink RTDS WebSocket feed in a background thread ──
    import asyncio
    def _chainlink_thread():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(start_chainlink_feed())
        loop.run_forever()
    threading.Thread(target=_chainlink_thread, daemon=True, name='chainlink_feed').start()
    logger.info('Chainlink RTDS feed thread launched')

    last_train = 0.0
    last_pause_state = False
    poly_exec: PolyExecutor | None = None
    last_poly_snapshot = 0.0
    snapshot_store = JsonStore(Path('logs') / 'poly_snapshot.json')
    last_chainlink_open_slug: str = ''

    # Cached model from last retrain — used for fast per-window predictions.
    cached_fit: FitResult | None = None
    cached_state: dict = {}
    cached_fits: dict[str, Any] = {}  # per-symbol FitResult cache

    # Track which 5-min window slug we last traded to avoid double-firing.
    # Per-asset tracking: {'btc': 'btc-updown-5m-...', 'eth': 'eth-updown-5m-...'}
    last_traded_slug: dict[str, str] = {}
    # Track which window slug we last sniped (late-entry). Per-asset.
    last_sniped_slug: dict[str, str] = {}
    # Per-asset Chainlink open tracking.
    last_chainlink_open_by_asset: dict[str, str] = {}

    # Resolution polling cadence (every ~90s).
    last_resolution_poll = 0.0

    # Order reconciliation cadence (every ~20s) — keep fill status sane.
    last_reconcile_poll = 0.0

    # Best-effort post-resolution cleanup cadence (~2 min)
    last_settlement_poll = 0.0

    # On-chain redemption cadence (~5 min)
    last_redeem_poll = 0.0

    # Redeem tx reconciliation cadence (~30s)
    last_redeem_reconcile = 0.0

    # Gas top-up cadence (~3 min)
    last_gas_topup = 0.0

    # Signal refresh cadence (~15s) — keeps dashboard live between trade windows.
    last_signal_refresh = 0.0

    # Early exit poll cadence (~30s) — take profit / stop loss on open positions.
    last_early_exit_poll = 0.0

    while True:
        try:
            _load_effective_env()
            cfg = C5Config.from_env()
            poly_cfg = PolyExecConfig.from_env()
            if poly_exec is None or poly_exec.cfg != poly_cfg:
                poly_exec = PolyExecutor(poly_cfg)
            if not _get_password():
                time.sleep(5)
                continue

            if _paused():
                if not last_pause_state:
                    # Save a small state marker so the UI reflects pause quickly.
                    STATE_STORE.save({'ts': time.time(), 'status': 'paused'})
                    last_pause_state = True
                time.sleep(5)
                continue
            if last_pause_state:
                last_pause_state = False
                # Force an immediate refresh after resuming.
                last_train = 0.0

            now = time.time()

            # Best-effort Polymarket account snapshot (for live status + PnL panel).
            # We do this on a short cadence regardless of model retrain schedule.
            if now - last_poly_snapshot >= 60:
                try:
                    snap = snapshot_from_env()
                    if snap is not None:
                        d = snap.as_dict()
                        snapshot_store.save(d)
                        append_equity_point(str(POLY_EQUITY_STORE.path), snap, max_points=5000)
                        # Mirror a tiny wallet block into state.json for sidebar/noob warnings.
                        cached_state['wallet'] = {
                            'native_symbol': d.get('native_gas_symbol'),
                            'native_balance': d.get('native_gas_balance'),
                            'address': d.get('address'),
                        }
                except Exception:
                    pass
                last_poly_snapshot = now

            # ── AUTO GAS TOP-UP (~3 min cadence) ───────────────────────
            # Best-effort helper: keep enough native gas token for redeems/withdraw.
            # This requires: 0x API key + some existing gas + USDC balance.
            if now - last_gas_topup >= 180:
                try:
                    if os.getenv('C5_GAS_TOPUP_ENABLED', '').strip().lower() in {'1', 'true', 'yes', 'on'}:
                        pk = (os.getenv('C5_POLY_PRIVATE_KEY', '') or '').strip()
                        if pk:
                            from eth_account import Account  # type: ignore

                            addr = Account.from_key(pk).address
                            cur = 0.0
                            try:
                                cur = float((cached_state.get('wallet') or {}).get('native_balance') or 0.0)
                            except Exception:
                                cur = 0.0
                            prev = _gas.preview_topup(from_address=addr, current_native=float(cur))
                            cached_state['gas_topup_preview'] = prev
                            if prev.get('ok') and (not prev.get('skipped')):
                                res = _gas.execute_topup(private_key=pk, from_address=addr, current_native=float(cur))
                                cached_state['gas_topup'] = res
                                # Also append to ops log for visibility.
                                try:
                                    log = _load_json(POLY_OPS_LOG_STORE, default=[]) or []
                                    if not isinstance(log, list):
                                        log = []
                                    log.append({'ts': int(time.time()), 'action': 'gas_topup_auto', 'result': res})
                                    log = log[-200:]
                                    POLY_OPS_LOG_STORE.save(log)
                                except Exception:
                                    pass
                                save_state(cached_state)
                except Exception as exc:
                    logger.warning('gas top-up failed: %s', exc)
                last_gas_topup = now

            # ── RETRAIN CADENCE (hourly by default) ─────────────────────
            if now - last_train >= cfg.retrain_minutes * 60:
                # Mark as running so the UI doesn't look frozen during long candle fetches.
                STATE_STORE.save({'ts': time.time(), 'status': 'running', 'symbol': cfg.symbol})
                symbols_state: dict[str, dict] = {}
                for sym in (cfg.symbols or [cfg.symbol]):
                    try:
                        st = run_once(cfg.with_symbol(sym))
                    except Exception as e:
                        st = {'ts': time.time(), 'symbol': sym, 'status': 'error', 'error': str(e)}
                    symbols_state[sym] = st

                # Primary symbol = first configured symbol
                primary = (cfg.symbols[0] if cfg.symbols else cfg.symbol)
                state = symbols_state.get(primary, {})
                if isinstance(state, dict):
                    state = dict(state)
                else:
                    state = {}
                state['symbols'] = symbols_state

                # Cache the FitResult for fast per-window predictions.
                fit_obj = state.pop('fit', None)
                if fit_obj is not None and isinstance(fit_obj, FitResult):
                    cached_fit = fit_obj
                    cached_fits[primary] = fit_obj
                    _SHARED_FITS[primary] = fit_obj
                # Cache fits for all symbols and strip from state (not JSON-serializable).
                for _sym_key, _sym_st in symbols_state.items():
                    if isinstance(_sym_st, dict):
                        _sym_fit = _sym_st.pop('fit', None)
                        if _sym_fit is not None and isinstance(_sym_fit, FitResult):
                            cached_fits[_sym_key] = _sym_fit
                            _SHARED_FITS[_sym_key] = _sym_fit

                # Stamp training metadata into state so the UI can show
                # next-retrain countdown and learning phase indicator.
                state['last_train_ts'] = now
                state['retrain_minutes'] = cfg.retrain_minutes
                cached_state = state
                save_state(state)

                last_train = now

            # ── CHAINLINK WINDOW-OPEN CACHE (early in each window) ──────
            # Record the Chainlink oracle price at window open so the snipe
            # prediction compares against the exact oracle used for resolution.
            # IMPORTANT: use fresh wall-clock for window selection.
            # The loop can do slow work (retrain, I/O, network), and using a
            # stale `now` here can cause us to trade the *previous* window.
            for _cl_sym in (cfg.symbols or [cfg.symbol]):
                _cl_asset = _cl_sym.split('-')[0].lower()
                win = current_window(polymarket_now(), asset=_cl_asset)
                if win.slug != last_chainlink_open_by_asset.get(_cl_asset, ''):
                    cl_open = _record_chainlink_window_open(win.start_ts, asset=_cl_asset)
                    if cl_open and cl_open > 0:
                        last_chainlink_open_by_asset[_cl_asset] = win.slug
                        logger.debug('Chainlink window open cached: slug=%s price=%.2f asset=%s', win.slug, cl_open, _cl_asset)

            # ── TRADE CADENCE (every 5-min window, per asset) ──────────
            now_trade = polymarket_now()

            # Quiet hours check: skip ML trades (not snipe) during configured hours.
            _in_quiet_hours = False
            _qh = getattr(cfg, 'quiet_hours_utc', '') or os.getenv('C5_QUIET_HOURS_UTC', '')
            if _qh and '-' in _qh:
                try:
                    _qh_start, _qh_end = [int(x) for x in _qh.split('-', 1)]
                    from datetime import datetime, timezone
                    _cur_hour = datetime.fromtimestamp(float(now_trade), tz=timezone.utc).hour
                    if _qh_start <= _qh_end:
                        _in_quiet_hours = _qh_start <= _cur_hour < _qh_end
                    else:
                        _in_quiet_hours = _cur_hour >= _qh_start or _cur_hour < _qh_end
                    if _in_quiet_hours:
                        logger.debug('Quiet hours active (%s UTC) — skipping ML trade', _qh)
                except Exception:
                    pass

            for _trade_sym in (cfg.symbols or [cfg.symbol]):
                _trade_asset = _trade_sym.split('-')[0].lower()
                _trade_cfg = cfg.with_symbol(_trade_sym) if hasattr(cfg, 'with_symbol') else cfg
                _pa = _load_per_asset_tuning(runtime_config.load_overrides()).get(_trade_sym, {})
                if not isinstance(_pa, dict):
                    _pa = {}
                _thr = float(_pa.get('confidence_threshold', cfg.confidence_threshold) or cfg.confidence_threshold)
                win = current_window(now_trade, asset=_trade_asset)

                if win.slug != last_traded_slug.get(_trade_asset, '') and is_trade_time(cfg.trade_lead_seconds, now_trade) and not _in_quiet_hours:
                    direction = None
                    confidence = 0.0
                    strong = False

                    # Fast prediction with cached model (preferred).
                    # Use per-symbol fit if available, fall back to primary fit.
                    _sym_fit = cached_fits.get(_trade_sym) or cached_fit
                    if _sym_fit is not None:
                        try:
                            pred = predict_latest(_trade_cfg, _sym_fit)
                            if pred.get('status') == 'ok':
                                direction = pred['direction']
                                confidence = pred['confidence']
                                strong = pred.get('strong', False)
                                # Update displayed state with the latest per-window prediction.
                                cached_state.update({
                                    'ts': time.time(),
                                    'status': 'ok',
                                    'direction': direction,
                                    'p_up': pred.get('p_up'),
                                    'confidence': confidence,
                                    'strong': strong,
                                    'price': pred.get('price'),
                                    'window_slug': win.slug,
                                })
                                # Keep per-symbol state fresh too (the Signal panel
                                # prefers state['symbols'][sym] when present).
                                symbols_state = cached_state.get('symbols')
                                if isinstance(symbols_state, dict):
                                    sym = _trade_sym
                                    sym_st = symbols_state.get(sym, {})
                                    if not isinstance(sym_st, dict):
                                        sym_st = {}
                                    sym_st.update({
                                        'ts': time.time(),
                                        'status': 'ok',
                                        'direction': direction,
                                        'p_up': pred.get('p_up'),
                                        'confidence': confidence,
                                        'strong': strong,
                                        'price': pred.get('price'),
                                        'window_slug': win.slug,
                                    })
                                    symbols_state[sym] = sym_st
                                save_state(cached_state)
                        except Exception:
                            pass

                    # Fallback: use the last retrain state if fast predict failed.
                    if direction is None and cached_state.get('status') == 'ok':
                        direction = cached_state.get('direction')
                        confidence = float(cached_state.get('confidence', 0.0))
                        strong = bool(cached_state.get('strong', False))

                    # Trade cadence: arb-first (optional) then directional fallback.
                    if poly_exec and poly_cfg.enabled:
                        try:
                            exec_result = None

                            # Complement arbitrage: buy BOTH sides when sum(asks) < 1.
                            # This is high-confidence when it triggers.
                            if bool(getattr(poly_cfg, 'arb_enabled', False)):
                                exec_result = poly_exec.trade_window_arb(window=win)

                            # ── Delta-first mode (v0.5.0+) ─────────────────
                            # When delta_first=True (default), ML-only directional trades
                            # are DISABLED.  The snipe pass (T-10s) using live Chainlink
                            # delta is the sole entry mechanism.  This avoids the ML model's
                            # poor accuracy (27% observed) while the snipe uses real-time
                            # information that directly determines market resolution.
                            _delta_first = getattr(cfg, 'delta_first', True)

                            # Directional trade: only when delta_first is OFF.
                            if not _delta_first and (exec_result is None or exec_result.get('skipped')):
                                if direction and confidence >= _thr:
                                    exec_result = poly_exec.trade_window(
                                        window=win,
                                        direction=direction,
                                        confidence=confidence,
                                        edge_min_override=(_pa.get('edge_min') if isinstance(_pa.get('edge_min'), (int, float, str)) else None),
                                        mq_max_spread_bps_override=(_pa.get('mq_max_spread_bps') if isinstance(_pa.get('mq_max_spread_bps'), (int, float, str)) else None),
                                        mq_min_depth_usdc_override=(_pa.get('mq_min_depth_usdc') if isinstance(_pa.get('mq_min_depth_usdc'), (int, float, str)) else None),
                                        asset=_trade_asset,
                                    )

                            if isinstance(exec_result, dict):
                                trade_info = (exec_result.get('trade') or {})
                                pm_state = {
                                    'enabled': True,
                                    'dry_run': bool(poly_cfg.dry_run),
                                    'placed': bool(exec_result.get('placed')),
                                    'skipped': bool(exec_result.get('skipped', False)),
                                    'reason': exec_result.get('reason'),
                                    'window_slug': win.slug,
                                    'trade_asset': _trade_asset,
                                }

                                # Directional trade shape (single trade dict).
                                if isinstance(trade_info, dict) and trade_info.get('bundle'):
                                    pm_state['arb'] = True
                                    pm_state['market'] = ((trade_info.get('up') or {}).get('question') if isinstance(trade_info.get('up'), dict) else None)
                                    pm_state['direction'] = 'BUNDLE'
                                    pm_state['confidence'] = 1.0
                                    pm_state['arb_edge_per_share'] = ((trade_info.get('up') or {}).get('arb_edge_per_share') if isinstance(trade_info.get('up'), dict) else None)
                                    pm_state['arb_sum_asks'] = ((trade_info.get('up') or {}).get('arb_sum_asks') if isinstance(trade_info.get('up'), dict) else None)
                                    pm_state['arb_spend_usdc'] = ((trade_info.get('up') or {}).get('arb_spend_usdc') if isinstance(trade_info.get('up'), dict) else None)
                                    pm_state['arb_shares'] = ((trade_info.get('up') or {}).get('arb_shares') if isinstance(trade_info.get('up'), dict) else None)
                                else:
                                    pm_state['arb'] = False
                                    if isinstance(trade_info, dict):
                                        pm_state['market'] = trade_info.get('question')
                                        pm_state['direction'] = trade_info.get('direction') or direction
                                        pm_state['confidence'] = trade_info.get('confidence') or confidence

                                # Market quality metrics (spread/depth) for UI visibility.
                                try:
                                    mq = None
                                    if isinstance(trade_info, dict):
                                        mq = trade_info.get('market_quality')
                                    if mq is None and isinstance(exec_result, dict):
                                        mq = exec_result.get('market_quality')
                                    if mq is not None:
                                        pm_state['market_quality'] = mq
                                except Exception:
                                    pass

                                # If arb skipped, it may carry a small 'arb' diagnostic payload.
                                if exec_result.get('skipped') and isinstance(exec_result.get('arb'), dict):
                                    pm_state['arb_diag'] = exec_result.get('arb')

                                cached_state['polymarket'] = pm_state
                                save_state(cached_state)
                        except Exception as e:
                            logger.exception('Polymarket exec error [%s]: %s', _trade_asset, e)

                    # Avoid burning the whole window on known "skipped" scenarios
                    # that we explicitly want to retry (e.g., transient pricing/balance).
                    # This keeps the bot active without spamming retries for other
                    # deterministic skip reasons (edge gate, kelly_no_edge, etc.).
                    _mark_done = True
                    try:
                        if isinstance(exec_result, dict) and bool(exec_result.get('skipped')):
                            r = str(exec_result.get('reason') or '')
                            rc = str(exec_result.get('reason_code') or '')
                            if r in {'balance_below_minimum', 'delta_gate_expensive'} or rc in {'balance_below_minimum', 'delta_gate_blocked'}:
                                _mark_done = False
                    except Exception:
                        pass

                    if _mark_done:
                        last_traded_slug[_trade_asset] = win.slug

            # ── SNIPE CADENCE (last N seconds of each window, per asset) ─
            # Late-entry strategy: near window close, check if the asset
            # already moved vs window open price.  If delta is large enough,
            # buy the winning token.  Separate slug tracker per asset.
            now_snipe = polymarket_now()
            for _snipe_sym in (cfg.symbols or [cfg.symbol]):
                _snipe_asset = _snipe_sym.split('-')[0].lower()
                _snipe_cfg = cfg.with_symbol(_snipe_sym) if hasattr(cfg, 'with_symbol') else cfg
                win = current_window(now_snipe, asset=_snipe_asset)
                if (cfg.snipe_enabled
                        and win.slug != last_sniped_slug.get(_snipe_asset, '')
                        and is_snipe_time(cfg.snipe_lead_seconds, now_snipe)
                        and poly_exec and poly_cfg.enabled):
                    try:
                        snipe_pred = predict_snipe(_snipe_cfg, win, asset=_snipe_asset)
                        if snipe_pred.get('status') == 'ok':
                            s_dir = snipe_pred['direction']
                            s_conf = snipe_pred['confidence']
                            s_delta = snipe_pred.get('delta_pct', 0.0)
                            s_oracle = snipe_pred.get('oracle_source', 'unknown')
                            s_basis = snipe_pred.get('basis', {})
                            logger.info(
                                'SNIPE [%s] window=%s dir=%s conf=%.4f delta=%.4f%% oracle=%s basis=%.1fbps',
                                _snipe_asset.upper(), win.slug, s_dir, s_conf, s_delta, s_oracle,
                                s_basis.get('basis_bps', 0) if isinstance(s_basis, dict) else 0,
                            )

                            # Update dashboard state with snipe prediction.
                            cached_state.update({
                                'ts': time.time(),
                                'status': 'ok',
                                'direction': s_dir,
                                'p_up': snipe_pred.get('p_up'),
                                'confidence': s_conf,
                                'strong': True,
                                'price': snipe_pred.get('price'),
                                'window_slug': win.slug,
                                'snipe': True,
                                'snipe_delta_pct': s_delta,
                                'snipe_asset': _snipe_asset,
                                'oracle_source': s_oracle,
                                'basis': s_basis,
                            })

                            # ── Delta-first mode: always trade on snipe signal ──
                            # In delta-first mode, snipe IS the primary strategy.
                            # No ML trade was placed, so always attempt the snipe.
                            # In legacy mode, only snipe if ML didn't already trade.
                            _delta_first_snipe = getattr(cfg, 'delta_first', True)
                            already_traded = False
                            if not _delta_first_snipe:
                                pm_st = cached_state.get('polymarket')
                                if isinstance(pm_st, dict):
                                    if pm_st.get('window_slug') == win.slug and pm_st.get('placed'):
                                        already_traded = True

                            if not already_traded:
                                exec_result = poly_exec.trade_window(
                                    window=win,
                                    direction=s_dir,
                                    confidence=s_conf,
                                    snipe=True,
                                    delta_pct=s_delta,
                                    asset=_snipe_asset,
                                )
                                if isinstance(exec_result, dict):
                                    trade_info = exec_result.get('trade') or {}
                                    pm_state = {
                                        'enabled': True,
                                        'dry_run': bool(poly_cfg.dry_run),
                                        'placed': bool(exec_result.get('placed')),
                                        'skipped': bool(exec_result.get('skipped', False)),
                                        'reason': exec_result.get('reason'),
                                        'window_slug': win.slug,
                                        'arb': False,
                                        'snipe': True,
                                        'snipe_delta_pct': s_delta,
                                        'snipe_asset': _snipe_asset,
                                    }
                                    if isinstance(trade_info, dict):
                                        pm_state['market'] = trade_info.get('question')
                                        pm_state['direction'] = trade_info.get('direction') or s_dir
                                        pm_state['confidence'] = trade_info.get('confidence') or s_conf
                                        if trade_info.get('market_quality') is not None:
                                            pm_state['market_quality'] = trade_info.get('market_quality')
                                    cached_state['polymarket'] = pm_state
                            save_state(cached_state)
                        else:
                            logger.debug(
                                'snipe skip [%s]: window=%s status=%s delta=%.4f%%',
                                _snipe_asset, win.slug, snipe_pred.get('status'),
                                snipe_pred.get('delta_pct', 0.0),
                            )
                    except Exception as exc:
                        logger.warning('snipe failed [%s]: %s', _snipe_asset, exc)
                    last_sniped_slug[_snipe_asset] = win.slug

            # ── ORDER RECONCILIATION (~20s cadence) ─────────────────────
            # Only meaningful in live mode; updates poly_trades.json in-place.
            if poly_cfg.enabled and (not poly_cfg.dry_run) and (now - last_reconcile_poll >= 20):
                try:
                    rec = reconcile_recent_orders(poly_cfg, max_trades=50, max_age_hours=24.0)
                    # Save a tiny status block for dashboard/debugging.
                    cached_state['reconcile'] = rec.as_dict()
                    save_state(cached_state)
                except Exception as exc:
                    logger.warning('order reconciliation failed: %s', exc)
                last_reconcile_poll = now

            # ── SIGNAL REFRESH (~15s cadence) ───────────────────────────
            # Refresh signals for ALL active symbols using their cached fits.
            if cached_fit is not None and now - last_signal_refresh >= 15:
                try:
                    _all_syms = list(cfg.symbols or [cfg.symbol])
                    symbols_state = cached_state.get('symbols')
                    if not isinstance(symbols_state, dict):
                        symbols_state = {}
                    for _ref_sym in _all_syms:
                        _ref_fit = cached_fits.get(_ref_sym) or _SHARED_FITS.get(_ref_sym) or (cached_fit if _ref_sym == cfg.symbol else None)
                        if _ref_fit is None:
                            continue
                        try:
                            pred = predict_latest(cfg.with_symbol(_ref_sym), _ref_fit)
                        except Exception:
                            pred = predict_latest(cfg, cached_fit) if _ref_sym == cfg.symbol else {}
                        if pred.get('status') == 'ok':
                            _sym_update = {
                                'ts': time.time(),
                                'status': 'ok',
                                'direction': pred['direction'],
                                'p_up': pred.get('p_up'),
                                'confidence': pred['confidence'],
                                'strong': pred.get('strong', False),
                                'price': pred.get('price'),
                                'symbol': _ref_sym,
                            }
                            # Update primary symbol in top-level state
                            if _ref_sym == cfg.symbol:
                                cached_state.update(_sym_update)
                            # Always update per-symbol dict
                            sym_st = symbols_state.get(_ref_sym, {})
                            if not isinstance(sym_st, dict):
                                sym_st = {}
                            sym_st.update(_sym_update)
                            symbols_state[_ref_sym] = sym_st
                    cached_state['symbols'] = symbols_state
                    save_state(cached_state)
                except Exception as exc:
                    logger.warning('signal refresh failed: %s', exc)
                last_signal_refresh = now

            # ── EARLY EXIT POLLING (~30s cadence) ────────────────────────
            if poly_cfg and poly_cfg.enabled and (now - last_early_exit_poll >= 30):
                try:
                    _overrides = runtime_config.load_overrides()
                    _ee_enabled = str(_overrides.get('C5_EARLY_EXIT_ENABLED') or os.getenv('C5_EARLY_EXIT_ENABLED', 'false')).strip().lower()
                    if _ee_enabled in ('1', 'true', 'yes'):
                        if poly_exec is not None and poly_exec._client is not None:
                            _ee_results = _early_exit.check_early_exits(
                                client=poly_exec._client,
                                clob_url=poly_exec.cfg.clob_url,
                                dry_run=bool(poly_cfg.dry_run),
                                overrides=_overrides,
                            )
                            if _ee_results:
                                cached_state['early_exit'] = _ee_results
                                save_state(cached_state)
                                for _r in _ee_results:
                                    logger.info(
                                        'early_exit: %s %s pnl=%.1f%% trigger=%s success=%s',
                                        _r.get('slug'), _r.get('direction'),
                                        _r.get('pnl_pct', 0), _r.get('trigger'), _r.get('success'),
                                    )
                except Exception as _ee_exc:
                    logger.warning('early_exit poll failed: %s', _ee_exc)
                last_early_exit_poll = now

            # ── RESOLUTION POLLING (~90s cadence) ───────────────────────
            if now - last_resolution_poll >= 90:
                try:
                    _resolution.check_resolutions()
                    # ── Feed results into risk rails ──────────────────────
                    # Risk rails use an internal trade log for consecutive loss /
                    # daily loss checks. Sync resolved filled trades using slug
                    # deduplication so we never double-count across poll cycles.
                    if poly_cfg and poly_cfg.enabled and poly_exec is not None:
                        try:
                            _rt_trades = _load_json(POLY_TRADES_STORE, default=[]) or []
                            # Build set of slugs already recorded in risk state
                            _rr_state  = poly_exec._risk._state
                            _recorded  = set(_rr_state.get('recorded_slugs', []))
                            _new_slugs = []
                            for _rt in (_rt_trades if isinstance(_rt_trades, list) else []):
                                if not isinstance(_rt, dict): continue
                                _rt_slug = _rt.get('window_slug') or _rt.get('market') or ''
                                if not _rt_slug or _rt_slug in _recorded: continue
                                _rt_res = _rt.get('resolved') or ''
                                if _rt_res not in ('win', 'loss'): continue
                                # For wins: require fill evidence (filled_size or redeemed).
                                # For losses: accept resolved='loss' as sufficient — there
                                # is nothing to redeem so redeem_status is never 'success',
                                # and filled_size may be unset. Skipping losses here was
                                # preventing the consecutive-loss circuit breaker from firing.
                                if _rt_res == 'win':
                                    _rt_fs = 0.0
                                    try: _rt_fs = float(_rt.get('filled_size') or 0)
                                    except Exception: pass
                                    if _rt_fs <= 0 and _rt.get('redeem_status') != 'success': continue
                                _rt_pnl = 0.0
                                try: _rt_pnl = float(_rt.get('redeem_profit_usdc') or 0)
                                except Exception: pass
                                poly_exec._risk.record_trade(
                                    result=_rt_res,
                                    pnl_usdc=_rt_pnl,
                                    filled=True,
                                )
                                _new_slugs.append(_rt_slug)
                                _recorded.add(_rt_slug)
                            if _new_slugs:
                                # Persist recorded slugs (keep last 500)
                                _rr_state['recorded_slugs'] = list(_recorded)[-500:]
                                poly_exec._risk._save_state()
                                logger.info('risk_rails: synced %d new resolved trades', len(_new_slugs))
                        except Exception as _rr_exc:
                            logger.debug('risk_rails sync failed: %s', _rr_exc)
                except Exception:
                    pass
                # ── Update ledgers after resolution polling ─────────
                try:
                    from .ledger_manager import update_ledgers as _update_ledgers
                    _update_ledgers()
                except Exception:
                    pass
                last_resolution_poll = now

            # ── RESOLVED CLEANUP (~2 min cadence) ───────────────────────
            # Best-effort: if any resolved trades still have positions, place
            # SELL orders to close. This is not on-chain redeem/claim.
            if poly_cfg.enabled and (now - last_settlement_poll >= 120):
                try:
                    res = _settlement.process_resolved_trades(dry_run=bool(poly_cfg.dry_run))
                    cached_state['settlement'] = res
                    save_state(cached_state)
                except Exception as exc:
                    logger.warning('settlement cleanup failed: %s', exc)
                last_settlement_poll = now

            # ── AUTO REDEEM (~5 min cadence) ─────────────────────────────
            # On-chain claim of resolved winning positions (EOA-only).
            if poly_cfg.enabled and (now - last_redeem_poll >= 300):
                try:
                    redeem_res = _redeem.process_auto_redeem(dry_run=bool(poly_cfg.dry_run))
                    cached_state['redeem'] = redeem_res
                    save_state(cached_state)
                    cnt = redeem_res.get('count', 0)
                    skipped = redeem_res.get('skipped', False)
                    if skipped:
                        logger.debug('auto-redeem: skipped (%s)', redeem_res.get('reason', '?'))
                    elif cnt:
                        all_res = (redeem_res.get('results') or []) + (redeem_res.get('orphan_results') or [])
                        ok_count = sum(1 for r in all_res if r.get('ok'))
                        logger.info('auto-redeem: %d/%d succeeded (dry=%s)', ok_count, cnt, poly_cfg.dry_run)
                    else:
                        logger.debug('auto-redeem: nothing to redeem')
                except Exception as exc:
                    logger.warning('auto redeem failed: %s', exc)
                last_redeem_poll = now

            # Mark submitted redeem txs as success/failure once mined.
            if poly_cfg.enabled and (now - last_redeem_reconcile >= 30):
                try:
                    rec = _redeem.reconcile_redeem_txs(max_trades=25)
                    cached_state['redeem_reconcile'] = rec
                    save_state(cached_state)
                    updated = rec.get('updated', 0)
                    if updated:
                        logger.info('redeem-reconcile: %d tx(s) updated', updated)
                except Exception as exc:
                    logger.warning('redeem reconcile failed: %s', exc)
                last_redeem_reconcile = now

        except Exception as e:
            STATE_STORE.save({'ts': time.time(), 'status': 'error', 'error': str(e)})
            logger.exception('Main loop error: %s', e)
        time.sleep(10)


@app.on_event('startup')
def startup():
    _ensure_env_parent()
    env_p = _env_path()
    load_dotenv(env_p, override=False)
    _setup_file_logging()
    # Diagnostic logging — helps trace "back to setup wizard" issues.
    pw = _get_password()
    logger.info(
        'startup: env_path=%s exists=%s password_configured=%s',
        env_p, env_p.exists(), bool(pw),
    )
    threading.Thread(target=_bg_loop, daemon=True).start()


def run_dev() -> None:
    import uvicorn

    _ensure_env_parent()
    load_dotenv(_env_path(), override=False)
    _load_effective_env()
    cfg = C5Config.from_env()
    uvicorn.run('crypto5min_polytrader.web:app', host=cfg.dashboard_host, port=cfg.dashboard_port, reload=False)


if __name__ == '__main__':
    run_dev()
