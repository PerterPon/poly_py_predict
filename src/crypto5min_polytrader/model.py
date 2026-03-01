from __future__ import annotations

import logging
import os
from dataclasses import dataclass

import numpy as np
import pandas as pd
from sklearn.preprocessing import StandardScaler

logger = logging.getLogger(__name__)

# ── Feature columns ──────────────────────────────────────────────────
# Expanded feature set for CNN-LSTM (richer than the old 9-feature LogReg).
FEATURE_COLS = [
    'ret_1',
    'ret_3',
    'ret_6',
    'ret_12',
    'ret_24',
    'ret_48',
    'mom_accel',
    'hl_spread',
    'range_ema',
    'vol_ema',
    'rsi_14',
    'macd',
    'macd_signal',
    'macd_hist',
    'bb_width',
    'bb_pct',
    'atr_ratio',
    'stoch_k',
    'stoch_d',
    'vol_ratio',
    'vol_roc',
    'vol_regime',
    'vwap_dev',
    'body_ratio',
    'rvol_6',
    'rvol_12',
    'rvol_24',
    'pos_12',
    'pos_48',
    'adx',
    'obv_slope',
    'hour_sin',
    'hour_cos',
    'dow_sin',
    'dow_cos',
    'williams_r',
    'cci',
    'rsi_6',
    'rsi_24',
    'rsi_divergence',
    'macd_cross',
    'dist_sma20',
]

# Sliding window length for CNN-LSTM input.
SEQ_LEN = 60

# Suppress TF noise.
os.environ.setdefault('TF_CPP_MIN_LOG_LEVEL', '2')


@dataclass
class FitResult:
    model: object          # keras.Model or LogisticRegression
    scaler: StandardScaler
    seq_len: int = SEQ_LEN
    backend: str = 'cnn_lstm'    # 'cnn_lstm' or 'logistic'
    calibrator: object = None    # sklearn IsotonicRegression (Platt-like calibration)


# ── Helpers ──────────────────────────────────────────────────────────

def _available_feature_cols(df: pd.DataFrame) -> list[str]:
    """Return only feature columns that actually exist in the DataFrame."""
    return [c for c in FEATURE_COLS if c in df.columns]


def _build_sequences(X_scaled: np.ndarray, y: np.ndarray, seq_len: int):
    """Slide a window of `seq_len` across rows to produce 3-D input for CNN-LSTM."""
    Xs, ys = [], []
    for i in range(seq_len, len(X_scaled)):
        Xs.append(X_scaled[i - seq_len:i])
        ys.append(y[i])
    return np.array(Xs, dtype=np.float32), np.array(ys, dtype=np.float32)


# ── Platt-style probability calibration ──────────────────────────────

def _fit_calibrator(model, X_val, y_val, backend: str, seq_len: int = 1):
    """Fit an Isotonic Regression calibrator on validation data.

    Maps raw model output → calibrated probability so that "60% confidence"
    actually means ~60% empirical win rate.  This eliminates the over- and
    under-confidence that causes the bot to trade on false signals.

    Requires at least 30 validation samples; returns None if insufficient.
    """
    from sklearn.isotonic import IsotonicRegression

    if len(y_val) < 30:
        return None

    try:
        if backend == 'cnn_lstm':
            raw = model.predict(X_val, verbose=0).flatten()
        else:
            # sklearn / LightGBM
            raw = model.predict_proba(X_val)[:, 1]

        ir = IsotonicRegression(y_min=0.01, y_max=0.99, out_of_bounds='clip')
        ir.fit(raw, y_val)
        logger.info('Calibrator fitted on %d samples (raw range %.3f–%.3f)',
                    len(y_val), raw.min(), raw.max())
        return ir
    except Exception as exc:
        logger.warning('Calibrator fitting failed: %s', exc)
        return None


# ── CNN-LSTM builder ─────────────────────────────────────────────────

def _build_cnn_lstm(n_features: int, seq_len: int):
    """Improved Conv1D → BatchNorm → MaxPool → Stacked LSTM → Dense.

    Architecture based on findings from Wu et al. (2024) review paper:
    - Conv-LSTM with multivariate features provides best accuracy
    - 100 hidden units optimal for LSTM layers
    - BatchNormalization stabilises training on volatile crypto data
    - Stacked LSTM captures multi-scale temporal patterns
    """
    from tensorflow import keras  # type: ignore
    from tensorflow.keras import layers  # type: ignore

    inp = keras.Input(shape=(seq_len, n_features))

    # Convolutional feature extraction
    x = layers.Conv1D(filters=64, kernel_size=3, activation='relu', padding='same')(inp)
    x = layers.BatchNormalization()(x)
    x = layers.MaxPooling1D(pool_size=2)(x)
    x = layers.Conv1D(filters=64, kernel_size=3, activation='relu', padding='same')(x)
    x = layers.BatchNormalization()(x)
    x = layers.MaxPooling1D(pool_size=2)(x)

    # Stacked LSTM (100 → 50 units, per Wu et al. optimal hidden-unit findings)
    x = layers.LSTM(100, return_sequences=True)(x)
    x = layers.Dropout(0.3)(x)
    x = layers.LSTM(50, return_sequences=False)(x)
    x = layers.Dropout(0.3)(x)

    # Dense head
    x = layers.Dense(32, activation='relu')(x)
    x = layers.Dropout(0.2)(x)
    x = layers.Dense(1, activation='sigmoid')(x)

    model = keras.Model(inp, x)
    model.compile(
        optimizer=keras.optimizers.Adam(learning_rate=0.001),
        loss='binary_crossentropy',
        metrics=['accuracy'],
    )
    return model


# ── Logistic fallback (used when data < SEQ_LEN or TF unavailable) ──

def _fit_logistic_fallback(df: pd.DataFrame) -> FitResult:
    """Gradient-boosted model (LightGBM) — much stronger than bare
    LogisticRegression on tabular data.  Falls back to LogisticRegression
    if lightgbm is not installed.
    """
    fcols = _available_feature_cols(df)
    X = df[fcols].astype(float).values
    y = df['y_up'].astype(int).values
    scaler = StandardScaler()
    Xs = scaler.fit_transform(X)

    try:
        import lightgbm as lgb
        model = lgb.LGBMClassifier(
            n_estimators=1000,
            max_depth=6,
            learning_rate=0.03,
            num_leaves=40,
            min_child_samples=20,
            subsample=0.8,
            bagging_freq=1,
            colsample_bytree=0.8,
            feature_fraction_bynode=0.8,
            reg_alpha=0.1,
            reg_lambda=0.1,
            path_smooth=1.0,
            is_unbalance=True,
            random_state=42,
            verbose=-1,
        )
        backend_note = 'lgbm'
    except ImportError:
        from sklearn.linear_model import LogisticRegression
        # scikit-learn ignores n_jobs for the lbfgs solver (and will warn).
        model = LogisticRegression(solver='lbfgs', max_iter=200)
        backend_note = 'logistic'

    # Early stopping for LightGBM to prevent overfitting.
    if backend_note == 'lgbm' and len(Xs) > 100:
        split = int(len(Xs) * 0.85)
        cal_split = int(len(Xs) * 0.10)
        train_end = split - cal_split
        model.fit(
            Xs[:train_end], y[:train_end],
            eval_set=[(Xs[train_end:split], y[train_end:split])],
            callbacks=[
                lgb.early_stopping(stopping_rounds=50, verbose=False),
                lgb.log_evaluation(period=0),
            ],
        )
        # Platt calibration on held-out slice.
        calibrator = _fit_calibrator(
            model, Xs[split:], y[split:], backend='logistic',
        )
    else:
        model.fit(Xs, y)
        calibrator = None
    logger.info('Fallback model: %s, samples=%d, features=%d',
                backend_note, len(Xs), len(fcols))
    return FitResult(model=model, scaler=scaler, seq_len=1, backend='logistic',
                     calibrator=calibrator)


# ── Public API (same names as before) ────────────────────────────────

def _fit_logistic_fast(df: pd.DataFrame) -> FitResult:
    """Bare LogisticRegression — fast enough for walk-forward backtesting
    where the model is refit hundreds of times."""
    from sklearn.linear_model import LogisticRegression
    fcols = _available_feature_cols(df)
    X = df[fcols].astype(float).values
    y = df['y_up'].astype(int).values
    scaler = StandardScaler()
    Xs = scaler.fit_transform(X)
    # scikit-learn ignores n_jobs for the lbfgs solver (and will warn).
    model = LogisticRegression(solver='lbfgs', max_iter=200)
    model.fit(Xs, y)
    return FitResult(model=model, scaler=scaler, seq_len=1, backend='logistic')


def fit_logistic(df: pd.DataFrame, force_logistic: bool = False,
                 fast: bool = False) -> FitResult:
    """Train the prediction model.

    Tries CNN-LSTM first. Falls back to LightGBM/LogisticRegression when:
    - force_logistic=True (used by walk-forward backtesting for speed)
    - Not enough rows (< SEQ_LEN + 50)
    - TensorFlow not available
    - Training error

    When fast=True, uses bare LogisticRegression for walk-forward speed.
    """
    fcols = _available_feature_cols(df)
    if not fcols or force_logistic:
        return _fit_logistic_fast(df) if fast else _fit_logistic_fallback(df)

    # Need enough rows for at least a few sequences.
    if len(df) < SEQ_LEN + 50:
        logger.info('Not enough data for CNN-LSTM (%d rows), falling back to logistic.', len(df))
        return _fit_logistic_fallback(df)

    try:
        X = df[fcols].astype(float).values
        y = df['y_up'].astype(int).values
        scaler = StandardScaler()
        X_scaled = scaler.fit_transform(X)

        X_seq, y_seq = _build_sequences(X_scaled, y, SEQ_LEN)
        if len(X_seq) < 30:
            return _fit_logistic_fallback(df)

        # Split: 80% train, 10% val (Keras), 10% calibration holdout.
        cal_split = max(20, int(len(X_seq) * 0.10))
        X_cal, y_cal = X_seq[-cal_split:], y_seq[-cal_split:]
        X_train_seq, y_train_seq = X_seq[:-cal_split], y_seq[:-cal_split]

        # Compute balanced class weights to counter UP/DOWN imbalance.
        n_pos = float(y_train_seq.sum())
        n_neg = float(len(y_train_seq) - n_pos)
        if n_pos > 0 and n_neg > 0:
            w0 = len(y_train_seq) / (2.0 * n_neg)
            w1 = len(y_train_seq) / (2.0 * n_pos)
            cw = {0: w0, 1: w1}
        else:
            cw = None

        from tensorflow.keras.callbacks import EarlyStopping, ReduceLROnPlateau  # type: ignore

        model = _build_cnn_lstm(len(fcols), SEQ_LEN)
        model.fit(
            X_train_seq, y_train_seq,
            epochs=30,
            batch_size=32,
            validation_split=0.15,
            class_weight=cw,
            callbacks=[
                EarlyStopping(monitor='val_loss', patience=5,
                              restore_best_weights=True),
                ReduceLROnPlateau(monitor='val_loss', factor=0.5,
                                  patience=3, min_lr=1e-6),
            ],
            verbose=0,
        )

        # Platt calibration on held-out data.
        calibrator = _fit_calibrator(model, X_cal, y_cal, backend='cnn_lstm')

        return FitResult(model=model, scaler=scaler, seq_len=SEQ_LEN,
                         backend='cnn_lstm', calibrator=calibrator)

    except Exception as exc:
        logger.warning('CNN-LSTM training failed (%s), falling back to logistic.', exc)
        return _fit_logistic_fallback(df)


def predict_proba(fit: FitResult, row_or_df: pd.Series | pd.DataFrame) -> float:
    """Return P(UP) for the latest observation.

    For CNN-LSTM: expects the last `seq_len` rows as a DataFrame.
    For logistic: expects a single row (Series).
    Callers from runner.py always pass a Series (latest row of feats);
    we handle both cases gracefully.
    """
    fcols = _available_feature_cols(
        row_or_df.to_frame().T if isinstance(row_or_df, pd.Series) else row_or_df
    )

    if fit.backend == 'logistic':
        if isinstance(row_or_df, pd.DataFrame):
            row_or_df = row_or_df.iloc[-1]
        X = row_or_df[fcols].astype(float).values.reshape(1, -1)
        Xs = fit.scaler.transform(X)
        # LightGBM expects DataFrame with feature names; sklearn doesn't care
        # but warns if given names it wasn't fitted with. Keep numpy for sklearn.
        try:
            import lightgbm as lgb
            is_lgbm = isinstance(
                getattr(fit.model, 'estimator', fit.model),
                lgb.LGBMClassifier,
            )
        except ImportError:
            is_lgbm = False
        if is_lgbm:
            Xs = pd.DataFrame(Xs, columns=fcols)
        raw = float(fit.model.predict_proba(Xs)[0, 1])

        # Apply Platt calibration if available.
        if fit.calibrator is not None:
            try:
                raw = float(fit.calibrator.predict([raw])[0])
            except Exception:
                pass
        return raw

    # CNN-LSTM path.
    # We need a DataFrame with at least seq_len rows. If we get a single Series,
    # we can only produce a logistic-like prediction (1-row scaler transform →
    # repeat to fake a sequence). This is a degraded path.
    if isinstance(row_or_df, pd.Series):
        X = row_or_df[fcols].astype(float).values.reshape(1, -1)
        Xs = fit.scaler.transform(X)
        # Repeat last row to fill the sequence (degraded but functional).
        seq = np.tile(Xs, (fit.seq_len, 1))[np.newaxis, ...]
    else:
        df = row_or_df
        if len(df) < fit.seq_len:
            # Pad with first row.
            pad = pd.concat([df.iloc[:1]] * (fit.seq_len - len(df)) + [df], ignore_index=True)
            X = pad[fcols].astype(float).values
        else:
            X = df[fcols].astype(float).values[-fit.seq_len:]
        Xs = fit.scaler.transform(X)
        seq = Xs[np.newaxis, ...]

    pred = fit.model.predict(seq, verbose=0)
    raw = float(pred[0, 0])

    # Apply Platt calibration if available.
    if fit.calibrator is not None:
        try:
            raw = float(fit.calibrator.predict([raw])[0])
        except Exception:
            pass  # use raw prediction
    return raw
