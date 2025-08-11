"""Backtest a listing-based trading strategy with a take-profit target.

This script fetches listing events from CoinMarketCal that occurred in the
past week and evaluates a simple momentum strategy: buy at the first available
Binance price after the listing announcement and sell once the price rises by
a configurable percentage (30% by default) or after seven days if the target
isn't reached. It then reports the percentage profit or loss (P&L) for each
event and the average P&L across all evaluated events.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from typing import List

import argparse

import requests
from dotenv import load_dotenv

load_dotenv()


COINMARKETCAL_API_KEY = os.getenv("COINMARKETCAL_API_KEY")

COINMARKETCAL_URL = "https://developers.coinmarketcal.com/v1/events"
BINANCE_URL = "https://api.binance.com/api/v3/klines"


def get_recent_listings(days: int = 7, limit: int = 10) -> List[dict]:
    """Return listing events from the past ``days`` days."""

    end = datetime.now(timezone.utc)
    start = end - timedelta(days=days)

    headers = {"x-api-key": COINMARKETCAL_API_KEY}
    params = {
        "categories": "listing",
        "sortBy": "date",
        "dateRangeStart": start.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "dateRangeEnd": end.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "max": limit,
    }

    response = requests.get(
        COINMARKETCAL_URL,
        headers=headers,
        params=params,
        timeout=10,
        proxies={"https": ""},
    )
    response.raise_for_status()
    return response.json()


def fetch_klines(symbol: str, start: datetime, end: datetime) -> List[list]:
    """Fetch hourly klines for ``symbol`` between ``start`` and ``end``."""

    params = {
        "symbol": symbol,
        "interval": "1h",
        "startTime": int(start.timestamp() * 1000),
        "endTime": int(end.timestamp() * 1000),
    }
    response = requests.get(BINANCE_URL, params=params, timeout=10, proxies={"https": ""})
    response.raise_for_status()
    return response.json()


def calculate_pnl(klines: List[list], take_profit: float) -> float | None:
    """Return percentage P&L with a take-profit threshold.

    If the price reaches the ``take_profit`` target (e.g. ``0.3`` for 30%) at
    any time within the provided kline series, that profit is returned. If the
    target is not reached, the P&L is calculated between the first open and the
    final close price.
    """

    if not klines:
        return None

    entry = float(klines[0][1])
    target = entry * (1 + take_profit)

    # iterate over subsequent klines, checking the high price for target hits
    for kline in klines[1:]:
        high = float(kline[2])
        if high >= target:
            return take_profit

    final = float(klines[-1][4])
    return (final - entry) / entry


def main(take_profit: float) -> None:
    events = get_recent_listings()
    total = 0.0
    count = 0

    for event in events:
        coin = event.get("coin", {})
        symbol = f"{coin.get('symbol', '').upper()}USDT"
        start = datetime.fromisoformat(event["date"].replace("Z", "+00:00"))
        end = start + timedelta(days=7)

        try:
            klines = fetch_klines(symbol, start, end)
        except requests.HTTPError:
            print(f"Skipping {symbol}: data unavailable")
            continue

        pnl = calculate_pnl(klines, take_profit)
        if pnl is None:
            print(f"Skipping {symbol}: insufficient data")
            continue

        total += pnl
        count += 1
        print(f"{event['date']} - {coin.get('name')} ({symbol}) P&L: {pnl*100:.2f}%")

    if count:
        print(f"\nAverage P&L over {count} events: {total / count * 100:.2f}%")
    else:
        print("No events with available market data")


if __name__ == "__main__":
    if not COINMARKETCAL_API_KEY:
        raise SystemExit("Please set COINMARKETCAL_API_KEY environment variable")

    parser = argparse.ArgumentParser(description="Backtest listing strategy")
    parser.add_argument(
        "--take-profit",
        type=float,
        default=0.3,
        help="Take-profit target as a fraction (0.3 for 30%)",
    )
    args = parser.parse_args()

    main(args.take_profit)

