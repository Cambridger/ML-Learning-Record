from dataclasses import dataclass, field
from typing import Dict, Any, List, Optional, Callable, Literal
import threading, time, random, math, json, argparse, traceback
import datetime as dt

import pandas as pd
import requests

# sseclient is optional when simulation mode is used
try:
    from sseclient import SSEClient
    SSE_AVAILABLE = True
except Exception:
    SSE_AVAILABLE = False

# -----------------------
STANDARD_HEADERS = {"Content-Type": "application/json; charset=utf-8"}

def _get_headers(token: str) -> Dict[str, str]:
    return {**STANDARD_HEADERS, "Authorization": token}

# -----------------------
@dataclass
class CMIBot:
    username: str
    password: str
    _cmi_url: str = "http://ec2-34-249-163-3.eu-west-1.compute.amazonaws.com"
    _flight_data_url: str = "http://localhost:8000"
    flight_data: pd.DataFrame = field(default_factory=lambda: pd.DataFrame())
    simulate_mode: bool = False  # can be forced from CLI

    # runtime state
    session: requests.Session = field(default_factory=requests.Session, init=False)
    auth_token: Optional[str] = field(default=None, init=False)
    products: List[str] = field(default_factory=list, init=False)
    inventory: Dict[str, int] = field(default_factory=dict, init=False)
    open_orders_local: Dict[str, List[Dict[str, Any]]] = field(default_factory=dict, init=False)
    recent_mid: Dict[str, List[float]] = field(default_factory=dict, init=False)
    _lock = threading.RLock()

    def __post_init__(self):
        # session headers
        self.session.headers.update(STANDARD_HEADERS)
        # try authenticate
        if not self.simulate_mode:
            try:
                self.auth_token = self._authenticate()
                self.session.headers.update({"Authorization": self.auth_token})
                print("[CMIBot] authenticated")
            except Exception as e:
                print(f"[CMIBot] authentication failed: {e}")
                print("[CMIBot] switching to simulate mode")
                self.simulate_mode = True
                self.auth_token = None
        else:
            print("[CMIBot] simulate_mode forced ON")

        # get products
        try:
            self.products = self.get_all_products()
        except Exception:
            self.products = ["EQUATOR", "FLIGHTS"]
        for p in self.products:
            self.inventory.setdefault(p, 0)
            self.open_orders_local.setdefault(p, [])
            self.recent_mid.setdefault(p, [])

        # launch flight data thread
        t = threading.Thread(target=self._pull_flight_data, daemon=True)
        t.start()

        # launch SSE threads only if not simulate and SSE available
        if (not self.simulate_mode) and SSE_AVAILABLE:
            for product in self.products:
                url = f"{self._cmi_url}/api/order/{product}/stream"
                thr = SSEThread(bearer=self.auth_token, url=url, handler_method=self._on_orderbook_wrapper)
                thr.daemon = True
                thr.start()
            trades_url = f"{self._cmi_url}/api/trade/stream"
            thr = SSEThread(bearer=self.auth_token, url=trades_url, handler_method=self.on_trade)
            thr.daemon = True
            thr.start()
        else:
            # in simulate mode we will generate fake mid updates for testing
            sim_thread = threading.Thread(target=self._simulate_market_updates, daemon=True)
            sim_thread.start()

    # ---------------- API helpers ----------------
    def _authenticate(self) -> str:
        url = f"{self._cmi_url}/api/user/authenticate"
        r = requests.post(url, headers=STANDARD_HEADERS, json={"username": self.username, "password": self.password}, timeout=5)
        r.raise_for_status()
        return r.headers["Authorization"]

    def get_all_products(self) -> List[str]:
        if self.simulate_mode:
            return ["EQUATOR", "FLIGHTS"]
        url = f"{self._cmi_url}/api/product"
        r = self.session.get(url, timeout=5)
        r.raise_for_status()
        data = r.json()
        return [item.get("symbol") for item in data]

    def send_order(self, product: str, price: float, side: Literal["BUY", "SELL"], volume: int) -> Optional[Dict]:
        payload = {"product": product, "side": side, "price": float(price), "volume": int(volume)}
        if self.simulate_mode:
            # simulate order -> local orderbook and possible immediate match logic
            oid = f"sim-{int(time.time()*1000)}-{random.randint(0,999)}"
            order = {"id": oid, **payload, "status": "OPEN"}
            with self._lock:
                self.open_orders_local[product].append(order)
            print(f"[SIM SEND] {side} {volume} {product}@{price} (id={oid})")
            # simulate fills probabilistically: if price is aggressive vs mid, fill immediately
            mid = self._get_sim_mid(product)
            if mid is not None:
                # if a marketable order (price better than contra mid) -> immediate fill
                if (side == "BUY" and price >= mid) or (side == "SELL" and price <= mid):
                    self._simulate_fill(product, order)
            return order
        else:
            try:
                url = f"{self._cmi_url}/api/order"
                r = self.session.post(url, json=payload, timeout=5)
                if r.status_code == 200:
                    data = r.json()
                    with self._lock:
                        self.open_orders_local[product].append(data)
                    return data
                else:
                    print(f"[SEND_ORDER ERROR] {r.status_code} {r.text}")
            except Exception as e:
                print(f"[SEND_ORDER EXCEPT] {e}")
            return None

    def cancel_order(self, order_id: str) -> bool:
        if self.simulate_mode:
            with self._lock:
                for p, lst in self.open_orders_local.items():
                    before = len(lst)
                    self.open_orders_local[p] = [o for o in lst if o.get("id") != order_id]
                    if len(self.open_orders_local[p]) < before:
                        print(f"[SIM CANCEL] {order_id}")
                        return True
            return False
        else:
            try:
                url = f"{self._cmi_url}/api/order/{order_id}"
                r = self.session.delete(url, timeout=5)
                return r.status_code == 200
            except Exception:
                return False

    def cancel_all_orders(self):
        if self.simulate_mode:
            with self._lock:
                cnt = sum(len(v) for v in self.open_orders_local.values())
                self.open_orders_local = {k:[] for k in self.products}
            print(f"[SIM CANCEL_ALL] removed {cnt}")
            return
        # remote cancel
        orders = []
        try:
            url = f"{self._cmi_url}/api/order/current-user"
            r = self.session.get(url, timeout=5)
            if r.status_code == 200:
                orders = r.json()
        except Exception:
            pass
        for o in orders:
            try:
                oid = o.get("id")
                if oid:
                    self.cancel_order(oid)
            except Exception:
                pass

    # --------------- flight data & theos ---------------
    def _pull_flight_data(self):
        while True:
            try:
                if self.simulate_mode:
                    # produce synthetic flight_data
                    now = dt.datetime.now(dt.timezone.utc)
                    df = pd.DataFrame([
                        {"flight_id": f"F{i}", "last_update_time": now.isoformat(), "on_ground": random.choice([True, False]),
                         "has_crossed_equator": random.choice([True, False])}
                        for i in range(20)
                    ])
                    self.flight_data = df
                else:
                    r = requests.get(self._flight_data_url, timeout=5)
                    if r.status_code == 200:
                        self.flight_data = pd.DataFrame(r.json())
                # compute theos is property-based so no action
            except Exception:
                pass
            time.sleep(5)

    @property
    def number_of_flights_in_air(self) -> float:
        if self.flight_data is None or len(self.flight_data) == 0:
            return 0.0
        now = dt.datetime.now(dt.timezone.utc)
        last_times = pd.to_datetime(self.flight_data["last_update_time"], errors="coerce")
        return int(len(self.flight_data[(~self.flight_data.get("on_ground", False)) & ((now - last_times) < dt.timedelta(minutes=5))]))

    @property
    def number_crossed_equator(self) -> int:
        if self.flight_data is None or len(self.flight_data) == 0:
            return 0
        try:
            return int(self.flight_data.get("has_crossed_equator", False).sum())
        except Exception:
            return 0

    @property
    def theos(self) -> Dict[str, float]:
        # simple mapping; user can override in subclass or replace logic
        return {"EQUATOR": 100.0 + 0.5 * self.number_crossed_equator, "FLIGHTS": 200.0 + 0.2 * self.number_of_flights_in_air}

    # --------------- helper simulate & mid tracking ---------------
    def _simulate_market_updates(self):
        # periodically push synthetic mid-price observations into recent_mid
        while True:
            for p in self.products:
                theo = self.theos.get(p, 100.0)
                # simulate mid fluctuating around theo
                mid = theo * (1.0 + random.normalvariate(0, 0.002))
                with self._lock:
                    self.recent_mid[p].append(mid)
                    if len(self.recent_mid[p]) > 200:
                        self.recent_mid[p].pop(0)
            time.sleep(1)

    def _get_vol_est(self, product: str, window: int = 20) -> float:
        with self._lock:
            arr = list(self.recent_mid.get(product, [])[-window:])
        if len(arr) < 2:
            return 0.0
        # use log returns std as vol proxy
        logs = [math.log(arr[i+1]/arr[i]) for i in range(len(arr)-1) if arr[i] > 0 and arr[i+1] > 0]
        if not logs:
            return 0.0
        return float(pd.Series(logs).std())

    def _get_sim_mid(self, product: str) -> Optional[float]:
        with self._lock:
            arr = self.recent_mid.get(product, [])
            return arr[-1] if arr else None

    def _simulate_fill(self, product: str, order: Dict[str, Any]):
        # simple fill: remove order and update inventory
        with self._lock:
            # remove the order
            self.open_orders_local[product] = [o for o in self.open_orders_local[product] if o.get("id") != order.get("id")]
            # update inventory
            side = order.get("side").upper()
            vol = int(order.get("volume", 0))
            if side == "BUY":
                self.inventory[product] = self.inventory.get(product, 0) + vol
            else:
                self.inventory[product] = self.inventory.get(product, 0) - vol
            print(f"[SIM FILL] {side} {vol} {product} -> inventory {self.inventory[product]}")

    # --------------- orderbook wrapper (for real SSE) ---------------
    def _on_orderbook_wrapper(self, raw: Dict[str, Any]):
        try:
            ob = dict(raw)
            ob["product"] = ob.pop("productsymbol", ob.get("product", "UNKNOWN"))
            def transform(side):
                out = []
                for price_s, vi in side.items():
                    try:
                        price = float(price_s)
                    except Exception:
                        price = float(vi.get("price", 0))
                    market_v = int(vi.get("marketVolume", vi.get("volume", 0)))
                    user_v = int(vi.get("userVolume", 0))
                    out.append({"price": price, "volume": market_v, "own_volume": user_v})
                return out
            ob["buy_orders"] = sorted(transform(ob.get("buyOrders", {})), key=lambda d: -d["price"])
            ob["sell_orders"] = sorted(transform(ob.get("sellOrders", {})), key=lambda d: d["price"])
            # derive mid if possible
            if ob["buy_orders"] and ob["sell_orders"]:
                mid = 0.5*(ob["buy_orders"][0]["price"] + ob["sell_orders"][0]["price"])
                with self._lock:
                    self.recent_mid[ob["product"]].append(mid)
                    if len(self.recent_mid[ob["product"]]) > 200:
                        self.recent_mid[ob["product"]].pop(0)
            self.on_orderbook(ob)
        except Exception as e:
            print("[ON_ORDERBOOK_WRAPPER ERR]", e)

    def on_trade(self, trade: Dict[str, Any]):
        # default: do nothing; subclasses can override
        # In simulate mode, trades are handled via _simulate_fill
        pass

# -------------------------
# SSEThread simplified
# -------------------------
class SSEThread(threading.Thread):
    def __init__(self, bearer: Optional[str], url: str, handler_method: Callable[[Dict], Any], reconnect_delay: float = 2.0):
        super().__init__()
        self.bearer = bearer
        self.url = url
        self.handler = handler_method
        self.reconnect_delay = reconnect_delay
        self.daemon = True

    def run(self):
        if not SSE_AVAILABLE:
            print("[SSEThread] SSEClient not available; thread exiting.")
            return
        headers = {"Accept": "text/event-stream; charset=utf-8"}
        if self.bearer:
            headers["Authorization"] = self.bearer
        while True:
            try:
                messages = SSEClient(self.url, headers=headers)
                for msg in messages:
                    if msg and msg.data:
                        try:
                            payload = json.loads(msg.data)
                        except Exception:
                            payload = msg.data
                        self.handler(payload)
            except Exception as e:
                print("[SSEThread] exception:", e)
                time.sleep(self.reconnect_delay)

# -------------------------
# Smart Market Making Bot
# -------------------------
@dataclass
class SmartMarketMakingBot(CMIBot):
    min_tick_width: float = 2.0         # base spread in ticks
    base_volume: int = 10
    refresh_interval: float = 3.0
    inventory_limit: int = 100
    skew_coeff: float = 0.03            # price shift per unit inventory
    vol_coeff: float = 200.0            # scales volatility to ticks
    levels: int = 2                     # number of levels per side
    jitter: float = 0.2                 # jitter in ticks to avoid predictability
    aggressive_threshold: int = 0       # not used by default; can set >0 to trigger active crossing

    def __post_init__(self):
        super().__post_init__()
        self.tick_sizes = {p: 1.0 for p in self.products}
        self._mm_thread = threading.Thread(target=self._market_make_loop, daemon=True)
        self._mm_thread.start()
        print("[SmartMM] started")

    def _price_to_tick(self, price: float, tick: float) -> float:
        return round(price / tick) * tick

    def _compute_half_spread_ticks(self, product: str) -> float:
        # estimate vol (log-return std)
        vol = self._get_vol_est(product, window=30)
        # map vol (small) to ticks: vol * vol_coeff
        tick = self.tick_sizes.get(product, 1.0)
        dynamic_ticks = max(self.min_tick_width, 1.0) + (vol * self.vol_coeff)
        # inventory penalty
        inv = abs(self.inventory.get(product, 0))
        dynamic_ticks += 0.02 * inv
        return dynamic_ticks / 2.0  # return half-spread in ticks

    def _market_make_loop(self):
        while True:
            try:
                with self._lock:
                    for product in self.products:
                        theo = self.theos.get(product, 100.0)
                        tick = self.tick_sizes.get(product, 1.0)
                        inv = self.inventory.get(product, 0)

                        half_ticks = self._compute_half_spread_ticks(product)
                        half_spread_price = half_ticks * tick

                        # inventory skew (positive inv -> we want to sell -> push buy down, sell up)
                        skew = self.skew_coeff * inv * tick

                        # base center price (rounded theo)
                        center = self._price_to_tick(theo, tick) - skew

                        # determine volumes: reduce if inventory large
                        if abs(inv) > self.inventory_limit:
                            level_qty = max(1, int(self.base_volume * 0.2))
                        else:
                            level_qty = self.base_volume

                        # if inventory dangerously high, do aggressive cross to reduce quickly
                        aggressive = abs(inv) > (self.inventory_limit * 0.8)

                        # cancel local orders then place new multi-level quotes
                        # For remote API: we call cancel_all_orders (costly) — here we manage local orders and send new ones
                        # Cancel remote/local orders
                        self.cancel_all_orders()

                        levels = []
                        for lvl in range(1, self.levels+1):
                            # price offset increases per level
                            offset = half_spread_price * (1 + 0.5*(lvl-1))
                            # add small jitter
                            j = (random.random() - 0.5) * self.jitter * tick
                            buy_px = center - offset + j
                            sell_px = center + offset + j

                            # possible aggressive crossing
                            if aggressive:
                                # move buy up and sell down to execute faster
                                buy_px += 0.5 * offset
                                sell_px -= 0.5 * offset

                            buy_px = self._price_to_tick(buy_px, tick)
                            sell_px = self._price_to_tick(sell_px, tick)

                            levels.append((buy_px, sell_px))

                        # submit orders
                        for (buy_px, sell_px) in levels:
                            if buy_px < sell_px:
                                self.send_order(product, buy_px, "BUY", level_qty)
                                self.send_order(product, sell_px, "SELL", level_qty)

                        # print debug
                        print(f"[SmartMM] {product} center={center:.2f} inv={inv} levels={levels} qty={level_qty}")

                time.sleep(self.refresh_interval)
            except Exception as e:
                print("[SmartMM] loop exception:", e)
                traceback.print_exc()
                time.sleep(2)

    def on_orderbook(self, orderbook: Dict[str, Any]):
        # optional: could adapt levels/qty based on orderbook depth
        # e.g., if book very thin, widen spread; if deep, tighten
        pass

    def on_trade(self, trade: Dict[str, Any]):
        # in real mode, parse trades for fills and update inventory if possible
        # in simulate mode, fills handled by _simulate_fill
        print("[SmartMM] trade event:", trade)

# -------------------------
# Main
# -------------------------
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bot", choices=["mm", "test"], default="mm")
    parser.add_argument("--username", default="test_user")
    parser.add_argument("--password", default="test_pass")
    parser.add_argument("--simulate", action="store_true", help="force simulate mode")
    args = parser.parse_args()

    kwargs = {"username": args.username, "password": args.password}
    if args.bot == "mm":
        bot = SmartMarketMakingBot(**kwargs)
    else:
        # fallback to the simple MarketMakingBot in base; here we use SmartMM
        bot = SmartMarketMakingBot(**kwargs)

    if args.simulate:
        bot.simulate_mode = True
        print("[MAIN] simulate forced ON")

    print("Bot running. Ctrl+C to stop.")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Shutting down, cancelling orders...")
        bot.cancel_all_orders()
        print("Stopped.")

if __name__ == "__main__":
    main()