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

COINMARKETCAL_BASE = os.getenv("COINMARKETCAL_BASE")
EVENTS_URL = f"{COINMARKETCAL_BASE}/events"
CATEGORIES_URL = f"{COINMARKETCAL_BASE}/categories"
BINANCE_URL =  os.getenv("BINANCE_URL")

HEADERS = {
    "x-api-key": COINMARKETCAL_API_KEY,
    "Accept": "application/json",
}


def get_category_ids_for_listings() -> str:
    """
    Возвращает строку ID категорий, связанных с листингами на биржах.
    Подбираем по названию ('list', 'exchang') чтобы не хардкодить числа.
    """
    r = requests.get(CATEGORIES_URL, headers=HEADERS, timeout=15)
    r.raise_for_status()
    cats = r.json()
    wanted_ids = [
        str(c["id"]) for c in cats
        if isinstance(c, dict)
        and "name" in c
        and any(k in c["name"].lower() for k in ("list", "exchang"))
    ]
    return ",".join(wanted_ids)

def get_recent_listings(days: int = 7, limit: int = 75):
    """
    Возвращает ВСЕ события за период, проходя по страницам до пустого body.
    `limit` = размер страницы (параметр API `max`, допускается 1..75).
    """
    if not COINMARKETCAL_API_KEY:
        raise SystemExit("Please set COINMARKETCAL_API_KEY environment variable")

    # страхуем базу/URL'ы на случай пустой переменной окружения
    base = (COINMARKETCAL_BASE or "https://developers.coinmarketcal.com/v1").rstrip("/")
    events_url = f"{base}/events"

    end = datetime.now(timezone.utc)
    start = end - timedelta(days=days)

    per_page = max(1, min(int(limit), 75))
    common_params = {
        "dateRangeStart": start.strftime("%Y-%m-%d"),
        "dateRangeEnd": end.strftime("%Y-%m-%d"),
        "sortBy": "created_desc",
        "max": per_page,
    }

    # попытка ограничиться именно листингами (категории типа Exchange/Listing)
    try:
        listing_ids = get_category_ids_for_listings()
        if listing_ids:
            common_params["categories"] = listing_ids
    except requests.HTTPError:
        pass

    all_events = []
    page = 1
    while True:
        params = dict(common_params, page=page)
        r = requests.get(events_url, headers=HEADERS, params=params, timeout=15)
        try:
            r.raise_for_status()
        except requests.HTTPError as e:
            # выводим тело ответа для диагностики и пробрасываем ошибку
            try:
                print(r.text)
            finally:
                raise e

        data = r.json()
        body = data.get("body") or []
        if not body:
            break

        all_events.extend(body)

        meta = data.get("_metadata") or {}
        page_count = meta.get("page_count")
        # условия остановки: дошли до последней страницы по метаданным
        if isinstance(page_count, int) and page >= page_count:
            break
        # ...или получили "короткую" страницу (< per_page)
        if len(body) < per_page:
            break

        page += 1

    return all_events


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
        # 1) Берём только листинги/релистинги на бирже Binance
        cats = event.get("categories", [])
        if cats and not any("exchange" in (c.get("name","").lower()) or c.get("id") == 4 for c in cats):
            continue

        if not 'binance' in event.get("-").lower():
            continue

        # 2) coins — это список; берём первую монету
        coins = event.get("coins", [])
        if not coins:
            continue
        coin = coins[0]
        symbol = f"{coin.get('symbol','').upper()}USDT"

        # 3) Правильная дата события
        date_str = event.get("date_event") or event.get("created_date")
        if not date_str:
            continue
        start = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
        end = start + timedelta(days=7)
        
        print(f"Processing coin: {coin} with event title: {event.get('-')} and date: {event.get('date_event')}")
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
        title_en = (event.get("title") or {}).get("en", "")
        disp_date = event.get("displayed_date", date_str)
        print(f"{disp_date} - {title_en} / {coin.get('name')} ({symbol}) P&L: {pnl*100:.2f}%")

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

