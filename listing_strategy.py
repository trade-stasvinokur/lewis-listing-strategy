# listing_strategy.py
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
from datetime import datetime, timedelta, timezone, date
from typing import List, Optional

import argparse
import requests
from dotenv import load_dotenv

from sqlalchemy import select, func
from sqlalchemy.exc import IntegrityError
from db import init_db, get_session, CoinEvent

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
        if isinstance(page_count, int) and page >= page_count:
            break
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
    """Return percentage P&L with a take-profit threshold."""
    if not klines:
        return None

    entry = float(klines[0][1])
    target = entry * (1 + take_profit)

    for kline in klines[1:]:
        high = float(kline[2])
        if high >= target:
            return take_profit

    final = float(klines[-1][4])
    return (final - entry) / entry


def _parse_event_date(date_str: Optional[str]) -> Optional[datetime]:
    if not date_str:
        return None
    try:
        return datetime.fromisoformat(date_str.replace("Z", "+00:00"))
    except ValueError:
        return None


def _query_events_for_day(session, day: date) -> list[CoinEvent]:
    """Достаём записи из БД по дате event_date == day (UTC)."""
    stmt = select(CoinEvent).where(func.date(CoinEvent.event_date) == day.isoformat())
    return list(session.execute(stmt).scalars().all())


def _fetch_events_for_window(start_day: date, end_day: date, limit: int = 75) -> list[dict]:
    """Загрузка событий CoinMarketCal в заданном окне (включительно по датам)."""
    if not COINMARKETCAL_API_KEY:
        raise SystemExit("Please set COINMARKETCAL_API_KEY environment variable")

    per_page = max(1, min(int(limit), 75))
    common_params = {
        "dateRangeStart": start_day.strftime("%Y-%m-%d"),
        "dateRangeEnd": end_day.strftime("%Y-%m-%d"),
        "sortBy": "created_desc",
        "max": per_page,
    }
    try:
        listing_ids = get_category_ids_for_listings()
        if listing_ids:
            common_params["categories"] = listing_ids
    except requests.HTTPError:
        pass

    all_events: list[dict] = []
    page = 1
    while True:
        params = dict(common_params, page=page)
        r = requests.get(EVENTS_URL, headers=HEADERS, params=params, timeout=15)
        r.raise_for_status()
        data = r.json()
        body = data.get("body") or []
        if not body:
            break
        all_events.extend(body)

        meta = data.get("_metadata") or {}
        page_count = meta.get("page_count")
        if isinstance(page_count, int) and page >= page_count:
            break
        if len(body) < per_page:
            break
        page += 1

    return all_events


def _is_binance_listing(event: dict) -> bool:
    """Фильтр «Binance listing»: по категориям и по названию ивента."""
    cats = event.get("categories", [])
    is_exchange = (not cats) or any(
        ("exchange" in (c.get("name", "").lower())) or (c.get("id") == 4) for c in cats
    )
    title = (event.get("-") or "").lower()
    return is_exchange and ("binance" in title)


def _save_api_event(session, event: dict) -> bool:
    """Сохранение одного события из API в таблицу coins."""
    coins = event.get("coins") or []
    if not coins:
        return False
    coin = coins[0]
    event_name = event.get("-") or ((event.get("title") or {}).get("en")) or None
    event_dt = _parse_event_date(event.get("date_event") or event.get("created_date"))

    row = CoinEvent(
        coin_id=coin.get("id") or "",
        coin_name=coin.get("name"),
        coin_symbol=coin.get("symbol"),
        coin_fullname=coin.get("fullname"),
        event_name=event_name,
        event_date=event_dt,
    )
    try:
        session.add(row)
        session.commit()
        print(f"Saved for tomorrow: {event_name} / {coin.get('symbol')} @ {event_dt}")
        return True
    except IntegrityError:
        session.rollback()
        print(f"Duplicate skipped: {event_name} / {coin.get('symbol')} @ {event_dt}")
        return False


def main(take_profit: float) -> None:
    # 1) Открываем БД
    init_db()
    session = get_session()

    now_utc = datetime.now(timezone.utc)
    yesterday = (now_utc - timedelta(days=1)).date()

    # 2) Проверяем, есть ли ивенты за вчера
    y_events = _query_events_for_day(session, yesterday)

    if y_events:
        total = 0.0
        count = 0
        for row in y_events:
            if not row.coin_symbol or not row.event_date:
                continue
            symbol = f"{row.coin_symbol.upper()}USDT"
            start = row.event_date
            end = start + timedelta(days=7)

            print(f"Processing coin from DB: {row.coin_fullname} ({row.coin_symbol}) "
                  f"event '{row.event_name}' at {start.isoformat()}")

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
            print(f"{yesterday.isoformat()} - {row.event_name or ''} / {row.coin_name} "
                  f"({symbol}) P&L: {pnl*100:.2f}%")

        if count:
            print(f"\nAverage P&L over {count} events: {total / count * 100:.2f}%")
        else:
            print("No events with available market data for yesterday")
        return

    # 3) Если ивентов за вчера нет — сохраняем ближайшие (на завтра) Binance-листинги
    tomorrow = (now_utc + timedelta(days=1)).date()
    api_events = _fetch_events_for_window(tomorrow, tomorrow, limit=75)
    saved = 0
    for ev in api_events:
        if _is_binance_listing(ev):
            if _save_api_event(session, ev):
                saved += 1

    if saved:
        print(f"Saved {saved} Binance listing event(s) for {tomorrow.isoformat()} into lewis.db.")
    else:
        print(f"No Binance listing events found for {tomorrow.isoformat()}.")


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