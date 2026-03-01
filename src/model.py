from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

import numpy as np
import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import StandardScaler


FEATURE_COLS = [
    'ret_1',
    'ret_3',
    'ret_6',
    'hl_spread',
    'range_ema',
    'vol_ema',
    'rsi_14',
    'macd',
    'macd_signal',
]


@dataclass
class FitResult:
    model: LogisticRegression
    scaler: StandardScaler


def _xy(df: pd.DataFrame) -> tuple[np.ndarray, np.ndarray]:
    X = df[FEATURE_COLS].astype(float).values
    y = df['y_up'].astype(int).values
    return X, y


def fit_logistic(df: pd.DataFrame) -> FitResult:
    X, y = _xy(df)
    scaler = StandardScaler()
    Xs = scaler.fit_transform(X)
    model = LogisticRegression(
        solver='lbfgs',
        max_iter=200,
        n_jobs=1,
    )
    model.fit(Xs, y)
    return FitResult(model=model, scaler=scaler)


def predict_proba(fit: FitResult, row: pd.Series) -> float:
    X = row[FEATURE_COLS].astype(float).values.reshape(1, -1)
    Xs = fit.scaler.transform(X)
    proba = float(fit.model.predict_proba(Xs)[0, 1])
    return proba
