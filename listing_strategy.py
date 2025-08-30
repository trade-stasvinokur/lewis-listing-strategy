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
from pathlib import Path
import csv
import tempfile
import shutil
from decimal import Decimal, ROUND_HALF_UP  # ← для точного форматирования

load_dotenv()


COINMARKETCAL_API_KEY = os.getenv("COINMARKETCAL_API_KEY")

COINMARKETCAL_BASE = os.getenv("COINMARKETCAL_BASE")
EVENTS_URL = f"{COINMARKETCAL_BASE}/events"
CATEGORIES_URL = f"{COINMARKETCAL_BASE}/categories"

BINANCE_ALPHA_BASE = os.getenv("BINANCE_ALPHA_BASE", "https://www.binance.com").rstrip("/")
ALPHA_AGG_KLINES_URL = f"{BINANCE_ALPHA_BASE}/bapi/defi/v1/public/alpha-trade/agg-klines"
ALPHA_TOKEN_LIST_URL = f"{BINANCE_ALPHA_BASE}/bapi/defi/v1/public/wallet-direct/buw/wallet/cex/alpha/all/token/list"

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


# ─────────────────────────────────────────────────────────────────────────────
#  Alpha helpers (fallback вместо GeckoTerminal)
# ─────────────────────────────────────────────────────────────────────────────

def _alpha_token_id_by_symbol(symbol: str) -> Optional[dict]:
    """
    Ищет запись токена в Token List по символьному имени.
    Возвращает dict токена (включая chainId и contractAddress) или None.
    """
    r = requests.get(ALPHA_TOKEN_LIST_URL, timeout=20, proxies={"https": ""})
    r.raise_for_status()
    payload = r.json() or {}
    tokens = payload.get("data") or payload.get("body") or []
    if not isinstance(tokens, list):
        return None

    sym_u = (symbol or "").upper().strip()

    for t in tokens:
        try:
            t_sym = (t.get("symbol") or "").upper().strip()
            if t_sym == sym_u:
                return t
        except Exception:
            continue
    return None


def _alpha_fetch_klines(token: dict,
                        start: datetime,
                        end: datetime,
                        interval: str = "1h") -> List[list]:
    """
    Забирает свечи через Binance Alpha agg-klines по адресу контракта.
    Возвращает список вида [[open_ms, open, high, low, close], ...],
    отфильтрованный по окну [start, end] и отсортированный по времени.
    """
    allowed = {
        "1s","15s","1m","3m","5m","15m","30m",
        "1h","2h","4h","6h","8h","12h","1d","3d","1w","1M"
    }
    if interval not in allowed:
        raise ValueError(f"Unsupported interval {interval!r} for Alpha agg-klines")

    params = {
        "chainId": token["chainId"],
        "tokenAddress": (token.get("contractAddress") or "").lower(),
        "interval": interval,
    }

    r = requests.get(ALPHA_AGG_KLINES_URL, params=params, timeout=20, proxies={"https": ""})
    r.raise_for_status()
    payload = r.json() or {}
    data = payload.get("data") or {}

    klines: List[list] = []
    start_ms = int(start.timestamp() * 1000)
    end_ms = int(end.timestamp() * 1000)

    for row in (data.get("klineInfos") or []):
        try:
            # row: [openTimeMs, open, high, low, close, volume, closeTimeMs]
            open_ms = int(row[0])
            if open_ms < start_ms or open_ms > end_ms:
                continue

            o = float(row[1])
            h = float(row[2])
            l = float(row[3])
            c = float(row[4])

            klines.append([open_ms, o, h, l, c])
        except (TypeError, ValueError):
            continue

    klines.sort(key=lambda x: x[0])
    return klines


def _aware_utc(dt: datetime) -> datetime:
    """Возвращает datetime c tzinfo=UTC. Naive трактуем как UTC."""
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def fetch_klines(symbol: str, start: datetime, end: datetime) -> List[list]:
    """
    Берём ончейн-свечи через Binance Alpha (официальный API).
    """
    # Alpha (он-чейн рынок Binance)
    alpha_token = _alpha_token_id_by_symbol(symbol)
    if not alpha_token:
        return []
    return _alpha_fetch_klines(alpha_token, start, end, interval="1h")


def calculate_pnl(klines: List[list]) -> float | None:
    """Return absolute P&L as (max(high) - entry_open), без take_profit."""
    if not klines:
        return None
    start_idx = 1 if len(klines) > 1 else 0  # вход по open второй свечи, если есть
    entry = float(klines[start_idx][1])
    high_val = max(float(k[2]) for k in klines[start_idx:])
    return high_val - entry


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


# ───────────────── formatting helpers ──────────────────

def _fmt_price(x: float | None) -> str:
    """
    Больше точности для мелких цен.
    ≥1      → 4 знака
    ≥0.1    → 6 знаков
    ≥0.01   → 8 знаков
    ≥0.001  → 9 знаков
    <0.001  → 10 знаков
    """
    if x is None:
        return ""
    d = Decimal(str(x))
    if d >= 1:
        q = Decimal("0.0001")
    elif d >= Decimal("0.1"):
        q = Decimal("0.000001")
    elif d >= Decimal("0.01"):
        q = Decimal("0.00000001")
    elif d >= Decimal("0.001"):
        q = Decimal("0.000000001")
    else:
        q = Decimal("0.0000000001")
    return format(d.quantize(q, rounding=ROUND_HALF_UP), "f")


def _write_strategy_results(rows: list[dict]) -> str:
    """
    Пишет/обновляет reports/strategy_results.csv с заголовком.
    Дедуп по ключу (Date, Ticker, Strategy).
    """
    reports_dir = Path(__file__).resolve().parent / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    file_path = reports_dir / "strategy_results.csv"

    fieldnames = [
        "Date", "Ticker", "Open", "High",
        "Strategy", "Status", "Entry", "Stop", "Target", "P/L",
    ]

    # читаем предыдущее содержимое (если есть)
    old: list[dict] = []
    if file_path.exists():
        with file_path.open("r", newline="", encoding="utf-8") as f:
            r = csv.DictReader(f)
            if r.fieldnames:
                old = list(r)

    # удаляем старые строки для тех же ключей
    new_keys = {(r["Date"], r["Ticker"], r["Strategy"]) for r in rows}
    old = [r for r in old if (r.get("Date"), r.get("Ticker"), r.get("Strategy")) not in new_keys]

    # атомарная запись: новые строки первыми + старые ниже
    with tempfile.NamedTemporaryFile("w", newline="", encoding="utf-8", delete=False,
                                    dir=str(file_path.parent)) as tmp:
        w = csv.DictWriter(tmp, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)
        w.writerows(old)
        tmp_name = tmp.name

    shutil.move(tmp_name, file_path)
    return str(file_path)


def main() -> None:
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
        csv_rows: list[dict] = []

        for row in y_events:
            if not row.coin_symbol or not row.event_date:
                continue
            symbol = row.coin_symbol
            start = _aware_utc(row.event_date)
            end = start + timedelta(days=7)

            print(
                f"Processing coin from DB: {row.coin_fullname} ({row.coin_symbol}) "
                f"event '{row.event_name}' at {start.isoformat()}"
            )

            try:
                klines = fetch_klines(symbol, start, end)
            except requests.HTTPError:
                print(f"Skipping {symbol}: data unavailable")
                continue

            pnl = calculate_pnl(klines)
            if pnl is None or not klines:
                print(f"Skipping {symbol}: insufficient data")
                continue

            total += pnl
            count += 1
            print(
                f"{yesterday.isoformat()} - {row.event_name or ''} / {row.coin_name} "
                f"({symbol}) P&L: {pnl:.10f}"
            )

            # NB: твоя логика входа — вторая свеча
            entry = float(klines[1][1])
            high_val = max(float(k[2]) for k in klines) if klines else None
            stop_price = entry * (1 - 0.01)

            # Статус: достигли 30% или нет — как было
            status = "✅" if pnl > 0 else "❌"

            # В отчёте отображаем фактическую «цель/выход» как High — чтобы
            # видеть реальную максимальную достижимую цену (как ты и просишь).
            target_out = high_val

            csv_rows.append({
                "Date": yesterday.isoformat(),
                "Ticker": symbol,
                "Open": _fmt_price(entry),
                "High": _fmt_price(high_val),
                "Strategy": "lewis-listing",
                "Status": status,
                "Entry": _fmt_price(entry),
                "Stop": _fmt_price(stop_price),
                "Target": _fmt_price(target_out),
                "P/L": _fmt_price(pnl),
            })

        if csv_rows:
            out_path = _write_strategy_results(csv_rows)
            print(f"CSV saved: {out_path}")

        if count:
            print(f"\nAverage P&L over {count} events: {total / count:.10f}")
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

    main()
