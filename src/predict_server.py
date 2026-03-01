"""
predict_server.py — Lightweight prediction microservice.

Exposes runner.py's ML prediction capabilities as HTTP API
for Node.js strategy to consume. No trading execution — signal only.

Start:
  PYTHONPATH=src uvicorn predict_server:app --host 0.0.0.0 --port 8700
  or via docker-compose (see docker-compose.yml)
"""

from __future__ import annotations

import asyncio
import logging
import os
import sys
import time
import traceback
from typing import Any

# Ensure src/ is on the import path (handles both local and Docker runs)
_src_dir = os.path.dirname(os.path.abspath(__file__))
if _src_dir not in sys.path:
    sys.path.insert(0, _src_dir)

from dotenv import load_dotenv

# Load .env from multiple possible locations
for _env_path in [
    os.path.join(os.getcwd(), "config", ".env"),
    os.path.join(os.getcwd(), ".env"),
    os.path.join(_src_dir, "..", "config", ".env"),
    os.path.join(_src_dir, "..", ".env"),
]:
    if os.path.isfile(_env_path):
        load_dotenv(_env_path, override=True)
        break

logging.basicConfig(
    level=os.getenv("C5_LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s [%(name)s] %(levelname)s %(message)s",
)
log = logging.getLogger("predict_server")

from fastapi import FastAPI
from pydantic import BaseModel

# ── Lazy imports for heavy ML modules ──
# Import at module level but catch errors so the server can still start
_import_ok = True
_import_error = ""
try:
    from crypto5min_polytrader.config import C5Config
    from crypto5min_polytrader.runner import run_once, predict_latest, predict_snipe
    from crypto5min_polytrader.window import (
        Window,
        current_window,
        is_trade_time,
        is_snipe_time,
    )
    from crypto5min_polytrader.model import FitResult
except Exception as e:
    _import_ok = False
    _import_error = f"{e}\n{traceback.format_exc()}"
    log.error("Failed to import ML modules: %s", e)

# ── In-memory model cache ──

_fits: dict[str, Any] = {}
_fit_meta: dict[str, dict] = {}
_start_time = time.time()
_training = False
_train_task: asyncio.Task | None = None


def _get_cfg(symbol: str):
    return C5Config.from_env().with_symbol(symbol)


def _get_symbols() -> list[str]:
    raw = os.getenv("C5_SYMBOLS", "") or ""
    symbols = [s.strip() for s in raw.split(",") if s.strip()]
    if not symbols:
        symbols = [(os.getenv("C5_SYMBOL", "BTC-USD") or "BTC-USD").strip()]
    return symbols


def _sanitize(obj: Any) -> Any:
    """Remove non-serializable keys and NaN/Inf floats."""
    import math

    if isinstance(obj, float):
        return None if (math.isnan(obj) or math.isinf(obj)) else obj
    if isinstance(obj, dict):
        return {k: _sanitize(v) for k, v in obj.items() if k != "fit"}
    if isinstance(obj, (list, tuple)):
        return [_sanitize(v) for v in obj]
    return obj


def _train_symbol(symbol: str) -> dict:
    global _training
    _training = True
    try:
        cfg = _get_cfg(symbol)
        result = run_once(cfg)
        if result.get("status") == "ok" and result.get("fit"):
            _fits[symbol] = result["fit"]
            _fit_meta[symbol] = {
                "trained_at": result.get("ts"),
                "direction": result.get("direction"),
                "p_up": float(result.get("p_up", 0)),
                "confidence": float(result.get("confidence", 0)),
                "strong": result.get("strong"),
            }
        return _sanitize(result)
    finally:
        _training = False


# ── Background training ──

async def _initial_train_and_loop(symbols: list[str]):
    """Run initial training + periodic retrain in background."""
    for symbol in symbols:
        try:
            log.info("Initial training %s ...", symbol)
            result = await asyncio.get_event_loop().run_in_executor(
                None, _train_symbol, symbol
            )
            log.info(
                "Initial train %s: status=%s direction=%s p_up=%s",
                symbol,
                result.get("status"),
                result.get("direction"),
                result.get("p_up"),
            )
        except Exception as e:
            log.error("Initial train %s failed: %s", symbol, e)

    interval = max(60, int(os.getenv("C5_RETRAIN_MINUTES", "30") or "30") * 60)
    log.info("Retrain loop started, interval=%ds", interval)
    while True:
        await asyncio.sleep(interval)
        for symbol in symbols:
            try:
                result = await asyncio.get_event_loop().run_in_executor(
                    None, _train_symbol, symbol
                )
                log.info(
                    "Retrain %s: status=%s direction=%s p_up=%s",
                    symbol,
                    result.get("status"),
                    result.get("direction"),
                    result.get("p_up"),
                )
            except Exception as e:
                log.error("Retrain %s failed: %s", symbol, e)


# ── FastAPI app ──

app = FastAPI(title="Crypto15m Predict Server")


@app.on_event("startup")
async def startup():
    global _train_task
    if not _import_ok:
        log.error("ML modules not available, prediction endpoints will fail")
        return
    symbols = _get_symbols()
    log.info("Predict server starting, symbols=%s", symbols)
    _train_task = asyncio.create_task(_initial_train_and_loop(symbols))


@app.on_event("shutdown")
async def shutdown():
    if _train_task:
        _train_task.cancel()


# ── Request models ──

class TrainRequest(BaseModel):
    symbol: str = "BTC-USD"


class PredictRequest(BaseModel):
    symbol: str = "BTC-USD"


class SnipeRequest(BaseModel):
    symbol: str = "BTC-USD"
    asset: str = "btc"
    window_start_ts: int | None = None


# ── Endpoints ──

@app.get("/health")
def health():
    """Service health — responds immediately even during training."""
    return {
        "ok": _import_ok,
        "import_error": _import_error if not _import_ok else None,
        "training": _training,
        "models_ready": list(_fits.keys()),
        "models_meta": {
            sym: {
                "trained_at": meta.get("trained_at"),
                "direction": meta.get("direction"),
                "p_up": meta.get("p_up"),
                "confidence": meta.get("confidence"),
                "strong": meta.get("strong"),
            }
            for sym, meta in _fit_meta.items()
        },
        "symbols": _get_symbols(),
        "uptime_sec": round(time.time() - _start_time),
    }


@app.post("/train")
def train(req: TrainRequest):
    """Run model training for a symbol and cache FitResult in memory."""
    if not _import_ok:
        return {"status": "error", "message": f"Import failed: {_import_error}"}
    try:
        result = _train_symbol(req.symbol)
        return result
    except Exception as e:
        log.error("Train %s error: %s", req.symbol, e)
        return {"status": "error", "message": str(e)}


@app.post("/predict")
def predict(req: PredictRequest):
    """Get ML prediction using cached model."""
    if not _import_ok:
        return {"status": "error", "message": f"Import failed: {_import_error}"}
    fit = _fits.get(req.symbol)
    if fit is None:
        return {
            "status": "no_model",
            "training": _training,
            "message": f"No trained model for {req.symbol}. "
            + ("Training in progress, try again shortly." if _training else "Call POST /train first."),
        }
    try:
        cfg = _get_cfg(req.symbol)
        result = predict_latest(cfg, fit)
        return _sanitize(result)
    except Exception as e:
        log.error("Predict %s error: %s", req.symbol, e)
        return {"status": "error", "message": str(e)}


@app.post("/predict/snipe")
def snipe(req: SnipeRequest):
    """Get delta-based snipe prediction for a window."""
    if not _import_ok:
        return {"status": "error", "message": f"Import failed: {_import_error}"}
    cfg = _get_cfg(req.symbol)
    if req.window_start_ts:
        a = req.asset.lower()
        win = Window(
            start_ts=req.window_start_ts,
            end_ts=req.window_start_ts + 900,
            slug=f"{a}-updown-15m-{req.window_start_ts}",
        )
    else:
        win = current_window(asset=req.asset)
    try:
        result = predict_snipe(cfg, win, asset=req.asset)
        return _sanitize(result)
    except Exception as e:
        log.error("Snipe %s error: %s", req.symbol, e)
        return {"status": "error", "message": str(e)}


@app.get("/window")
def window_info(asset: str = "btc"):
    """Current 15m window timing information."""
    if not _import_ok:
        return {"status": "error", "message": f"Import failed: {_import_error}"}
    now = time.time()
    win = current_window(now=now, asset=asset)
    return {
        "slug": win.slug,
        "start_ts": win.start_ts,
        "end_ts": win.end_ts,
        "elapsed_sec": round(now - win.start_ts, 1),
        "remaining_sec": round(win.end_ts - now, 1),
        "is_trade_time": is_trade_time(now=now),
        "is_snipe_time": is_snipe_time(now=now),
    }
