import asyncio
import json
import time

import requests
import websockets
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo

import config

ET = ZoneInfo("America/New_York")


class State:
    def __init__(self):
        self.bids: list[tuple[float, float]] = []
        self.asks: list[tuple[float, float]] = []
        self.mid: float = 0.0

        self.trades: list[dict] = []

        self.klines: list[dict] = []
        self.cur_kline: dict | None = None

        # Polymarket — current window
        self.pm_slug:   str | None = None
        self.pm_up_id:  str | None = None
        self.pm_dn_id:  str | None = None
        self.pm_gen:    int = 0          # bumped on market rollover → pm_feed resubscribes
        self.pm_book:   dict[str, dict] = {}   # asset_id → top of book + price→size L2 maps
        self.win_start: float | None = None    # unix ts
        self.win_end:   float | None = None
        self.price_to_beat: float | None = None
        self.ptb_source: str | None = None     # "polymarket" | "binance≈"
        self.chainlink:  float | None = None   # live Chainlink price (5m/15m/4h settle on this)
        self.resolves_on: str = "chainlink"
        self.edge_summary: str = "collecting forecasts"

    @property
    def ref_price(self):
        """Live price in the same terms the market settles on."""
        return self.chainlink if self.resolves_on == "chainlink" else self.mid

    def _pm_side(self, asset, key):
        return (self.pm_book.get(asset) or {}).get(key) if asset else None

    @property
    def pm_up(self):     return self._pm_side(self.pm_up_id, "ask")
    @property
    def pm_dn(self):     return self._pm_side(self.pm_dn_id, "ask")
    @property
    def pm_up_bid(self): return self._pm_side(self.pm_up_id, "bid")
    @property
    def pm_dn_bid(self): return self._pm_side(self.pm_dn_id, "bid")


OB_POLL_INTERVAL = 2


def _get_json(url, params=None, timeout=5):
    return requests.get(url, params=params, timeout=timeout).json()


async def _aget_json(url, params=None, timeout=5):
    # run blocking HTTP off the event loop so websocket feeds keep flowing
    return await asyncio.to_thread(_get_json, url, params, timeout)


def select_binance_endpoint():
    """api.binance.com returns HTTP 451 from US IPs — fall back to binance.us."""
    try:
        r = requests.get(f"{config.BINANCE_REST}/ping", timeout=5)
        if r.status_code == 200:
            return
    except Exception:
        pass
    config.BINANCE_REST = config.BINANCE_US_REST
    config.BINANCE_WS   = config.BINANCE_US_WS
    print("  [Binance] api.binance.com unavailable – using binance.us")


async def ob_poller(symbol: str, state: State):
    url = f"{config.BINANCE_REST}/depth"
    print(f"  [Binance OB] polling {symbol} every {OB_POLL_INTERVAL}s")
    while True:
        try:
            resp = await _aget_json(url, {"symbol": symbol, "limit": 20}, 3)
            state.bids = [(float(p), float(q)) for p, q in resp["bids"]]
            state.asks = [(float(p), float(q)) for p, q in resp["asks"]]
            if state.bids and state.asks:
                state.mid = (state.bids[0][0] + state.asks[0][0]) / 2
        except Exception:
            pass
        await asyncio.sleep(OB_POLL_INTERVAL)


async def binance_feed(symbol: str, kline_iv: str, state: State):
    sym = symbol.lower()
    streams = "/".join([
        f"{sym}@trade",
        f"{sym}@kline_{kline_iv}",
    ])
    url = f"{config.BINANCE_WS}?streams={streams}"

    while True:
        try:
            async with websockets.connect(
                url,
                ping_interval=20,
                ping_timeout=60,
                close_timeout=10
            ) as ws:
                print(f"  [Binance WS] connected – {symbol}")

                while True:
                    try:
                        data   = json.loads(await ws.recv())
                        stream = data.get("stream", "")
                        pay    = data["data"]

                        if "@trade" in stream:
                            state.trades.append({
                                "t":      pay["T"] / 1000.0,
                                "price":  float(pay["p"]),
                                "qty":    float(pay["q"]),
                                "is_buy": not pay["m"],
                            })
                            if len(state.trades) > 5000:
                                cut = time.time() - config.TRADE_TTL
                                state.trades = [t for t in state.trades if t["t"] >= cut]

                        elif "@kline" in stream:
                            k = pay["k"]
                            candle = {
                                "t": k["t"] / 1000.0,
                                "o": float(k["o"]), "h": float(k["h"]),
                                "l": float(k["l"]), "c": float(k["c"]),
                                "v": float(k["v"]),
                            }
                            state.cur_kline = candle
                            if k["x"]:
                                state.klines.append(candle)
                                state.klines = state.klines[-config.KLINE_MAX:]

                    except websockets.exceptions.ConnectionClosed:
                        print(f"  [Binance WS] connection closed, reconnecting...")
                        break

        except Exception as e:
            print(f"  [Binance WS] connection error: {e}, reconnecting in 5s...")
            await asyncio.sleep(5)


async def bootstrap(symbol: str, interval: str, state: State):
    resp = await _aget_json(
        f"{config.BINANCE_REST}/klines",
        {"symbol": symbol, "interval": interval, "limit": config.KLINE_BOOT},
        10,
    )
    state.klines = [
        {
            "t": r[0] / 1e3,
            "o": float(r[1]), "h": float(r[2]),
            "l": float(r[3]), "c": float(r[4]),
            "v": float(r[5]),
        }
        for r in resp
    ]
    print(f"  [Binance] loaded {len(state.klines)} historical candles")


_MONTHS = ["", "january", "february", "march", "april", "may", "june",
           "july", "august", "september", "october", "november", "december"]

TF_SECS = {"5m": 300, "15m": 900, "1h": 3600, "4h": 14400, "daily": 86400}


def _to_12h(hour24: int) -> str:
    if hour24 == 0:
        return "12am"
    if hour24 < 12:
        return f"{hour24}am"
    if hour24 == 12:
        return "12pm"
    return f"{hour24 - 12}pm"


def _window_start(tf: str, now: datetime) -> datetime:
    """Start (ET-aware) of the window containing `now`."""
    et = now.astimezone(ET)
    if tf in ("5m", "15m"):
        secs = TF_SECS[tf]
        ts = (int(now.timestamp()) // secs) * secs
        return datetime.fromtimestamp(ts, ET)
    if tf == "1h":
        return et.replace(minute=0, second=0, microsecond=0)
    if tf == "4h":
        # 4h windows are aligned to ET midnight (00, 04, 08 … ET)
        return et.replace(hour=et.hour - et.hour % 4, minute=0, second=0, microsecond=0)
    if tf == "daily":
        # daily windows run noon ET → noon ET; slug is named for the end date
        noon = et.replace(hour=12, minute=0, second=0, microsecond=0)
        return noon if et >= noon else noon - timedelta(days=1)
    raise ValueError(tf)


def _build_slug(coin: str, tf: str, start: datetime) -> str:
    short, long_ = config.COIN_PM[coin], config.COIN_PM_LONG[coin]
    ts = int(start.timestamp())
    if tf in ("5m", "15m", "4h"):
        return f"{short}-updown-{tf}-{ts}"
    if tf == "1h":
        return (f"{long_}-up-or-down-{_MONTHS[start.month]}-{start.day}-"
                f"{start.year}-{_to_12h(start.hour)}-et")
    if tf == "daily":
        end = start + timedelta(days=1)
        return f"{long_}-up-or-down-on-{_MONTHS[end.month]}-{end.day}-{end.year}"
    raise ValueError(tf)


def _iso_ts(s: str | None) -> float | None:
    if not s:
        return None
    return datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp()


def fetch_pm_market(coin: str, tf: str, now: datetime | None = None) -> dict | None:
    """Resolve the live Up/Down market for the current window."""
    now   = now or datetime.now(timezone.utc)
    start = _window_start(tf, now)
    slug  = _build_slug(coin, tf, start)
    try:
        data = _get_json(config.PM_GAMMA, {"slug": slug, "limit": 1})
    except Exception as e:
        print(f"  [PM] event fetch failed ({slug}): {e}")
        return None
    if not data or data[0].get("slug") != slug:
        print(f"  [PM] no market for slug: {slug}")
        return None
    try:
        m   = data[0]["markets"][0]
        ids = json.loads(m["clobTokenIds"])
        outcomes = json.loads(m.get("outcomes") or '["Up","Down"]')
        if outcomes and outcomes[0].lower() != "up":
            ids = ids[::-1]
        return {
            "slug":   slug,
            "up_id":  ids[0],
            "dn_id":  ids[1],
            "start":  _iso_ts(m.get("eventStartTime")) or start.timestamp(),
            "end":    _iso_ts(m.get("endDate")) or start.timestamp() + TF_SECS[tf],
            "closed": bool(m.get("closed")),
        }
    except Exception as e:
        print(f"  [PM] token extraction failed ({slug}): {e}")
        return None


def _iso(ts: float) -> str:
    return datetime.fromtimestamp(ts, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _fetch_pm_price_to_beat(coin: str, tf: str, start: float, end: float) -> float | None:
    try:
        r = _get_json(config.PM_CRYPTO_PRICE,
                      {"symbol": coin, "eventStartTime": _iso(start),
                       "variant": config.PM_VARIANT[tf], "endDate": _iso(end)})
        v = r.get("openPrice")
        return float(v) if v else None
    except Exception:
        return None


def _fetch_open_price(symbol: str, ts: float) -> float | None:
    """Binance 1m candle open at window start ≈ the market's price to beat."""
    try:
        r = _get_json(f"{config.BINANCE_REST}/klines",
                      {"symbol": symbol, "interval": "1m",
                       "startTime": int(ts * 1000), "limit": 1})
        if r and abs(r[0][0] / 1000 - ts) < 60:
            return float(r[0][1])
    except Exception:
        pass
    return None


async def market_manager(coin: str, tf: str, symbol: str, state: State):
    """Keep state pointed at the live window; roll to the next one at expiry."""
    while True:
        now = time.time()
        need_market = state.win_end is None or now >= state.win_end
        if need_market:
            mkt = await asyncio.to_thread(fetch_pm_market, coin, tf)
            if mkt and mkt["slug"] != state.pm_slug:
                state.pm_slug   = mkt["slug"]
                state.pm_up_id  = mkt["up_id"]
                state.pm_dn_id  = mkt["dn_id"]
                state.win_start = mkt["start"]
                state.win_end   = mkt["end"]
                state.pm_book   = {}
                state.price_to_beat = None
                state.pm_gen   += 1
        if (state.price_to_beat is None and state.win_start
                and time.time() >= state.win_start + 1):
            ptb = await asyncio.to_thread(_fetch_pm_price_to_beat, coin, tf,
                                          state.win_start, state.win_end)
            if ptb:
                state.price_to_beat, state.ptb_source = ptb, "polymarket"
            elif state.resolves_on == "binance":
                ptb = await asyncio.to_thread(_fetch_open_price, symbol, state.win_start)
                if ptb:
                    state.price_to_beat, state.ptb_source = ptb, "binance≈"
        await asyncio.sleep(2 if need_market else 1)


async def chainlink_feed(coin: str, state: State):
    sym = f"{config.COIN_PM[coin]}/usd"
    sub = {"action": "subscribe",
           "subscriptions": [{"topic": "crypto_prices_chainlink", "type": "*", "filters": ""}]}
    while True:
        try:
            async with websockets.connect(config.PM_RTDS, ping_interval=20,
                                          ping_timeout=60, close_timeout=10) as ws:
                await ws.send(json.dumps(sub))
                async for raw in ws:
                    try:
                        msg = json.loads(raw)
                    except ValueError:
                        continue
                    pay = msg.get("payload") or {}
                    if pay.get("symbol") == sym and pay.get("value"):
                        state.chainlink = float(pay["value"])
        except Exception as e:
            print(f"  [Chainlink] connection error: {e}, reconnecting in 5s...")
            await asyncio.sleep(5)


async def pm_feed(state: State):
    while True:
        if not state.pm_up_id:
            await asyncio.sleep(1)
            continue

        gen    = state.pm_gen
        assets = [state.pm_up_id, state.pm_dn_id]
        try:
            async with websockets.connect(
                config.PM_WS,
                ping_interval=20,
                ping_timeout=60,
                close_timeout=10
            ) as ws:
                await ws.send(json.dumps({"assets_ids": assets, "type": "market"}))

                while state.pm_gen == gen:
                    try:
                        raw = json.loads(await asyncio.wait_for(ws.recv(), 1))
                    except asyncio.TimeoutError:
                        continue
                    except websockets.exceptions.ConnectionClosed:
                        break

                    for msg in raw if isinstance(raw, list) else [raw]:
                        if not isinstance(msg, dict):
                            continue
                        ev = msg.get("event_type")
                        if ev in (None, "book") and ("bids" in msg or "asks" in msg):
                            _pm_book(msg, state)
                        elif ev == "price_change":
                            for ch in msg.get("price_changes", []):
                                _pm_change(ch, state)
                        elif ev == "best_bid_ask":
                            _pm_top(msg.get("asset_id"), msg.get("best_bid"),
                                    msg.get("best_ask"), state)

        except Exception as e:
            print(f"  [PM] connection error: {e}, reconnecting in 5s...")
            await asyncio.sleep(5)


def _pm_book(msg, state):
    asset = msg.get("asset_id")
    if asset not in (state.pm_up_id, state.pm_dn_id):
        return
    book = state.pm_book.setdefault(asset, {})
    book["bids"] = {float(level["price"]): float(level["size"])
                     for level in msg.get("bids", []) if float(level.get("size", 0)) > 0}
    book["asks"] = {float(level["price"]): float(level["size"])
                     for level in msg.get("asks", []) if float(level.get("size", 0)) > 0}
    _refresh_pm_top(book)


def _refresh_pm_top(book):
    bids, asks = book.get("bids", {}), book.get("asks", {})
    book["bid"] = max(bids) if bids else None
    book["ask"] = min(asks) if asks else None
    book["bid_size"] = bids.get(book["bid"]) if book["bid"] is not None else None
    book["ask_size"] = asks.get(book["ask"]) if book["ask"] is not None else None


def _pm_change(change, state):
    asset = change.get("asset_id")
    if asset not in (state.pm_up_id, state.pm_dn_id):
        return
    book = state.pm_book.setdefault(asset, {"bids": {}, "asks": {}})
    side = str(change.get("side", "")).upper()
    if side not in ("BUY", "SELL"):
        return
    levels_key = "bids" if side == "BUY" else "asks"
    levels = book.setdefault(levels_key, {})
    price, size = float(change["price"]), float(change["size"])
    if size <= 0:
        levels.pop(price, None)
    else:
        levels[price] = size
    _refresh_pm_top(book)


def _pm_top(asset, bid, ask, state):
    if asset not in (state.pm_up_id, state.pm_dn_id):
        return
    book = state.pm_book.setdefault(asset, {"bid": None, "ask": None})
    if bid not in (None, ""):
        book["bid"] = float(bid)
    if ask not in (None, ""):
        book["ask"] = float(ask)
