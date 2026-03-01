"""Entry-point shim.

This file exists so `python src/dashboard.py` works in Docker and for users.
All application logic lives in `crypto5min_polytrader.web`.
"""

from crypto5min_polytrader.web import run_dev


if __name__ == '__main__':
    run_dev()
