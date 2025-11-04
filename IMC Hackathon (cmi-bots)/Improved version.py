import json
import time
import random
import threading
import datetime as dt
from dataclasses import dataclass
from typing import Dict, List, Any, Optional, Literal, Callable
from abc import ABC, abstractmethod

import pandas as pd
import requests
from sseclient import SSEClient

# ======================================
# ======== 通用配置 =====================
# ======================================
STANDARD_HEADERS = {"Content-Type": "application/json; charset=utf-8"}


def _get_headers(auth_token: str) -> Dict[str, str]:
    return {**STANDARD_HEADERS, "Authorization": auth_token}


# ======================================
# ========== 基础 CMI Bot ===============
# ======================================
@dataclass
class CMIBot(ABC):
    username: str
    password: str
    flight_data: pd.DataFrame = pd.DataFrame()
    _cmi_url: str = "http://ec2-34-249-163-3.eu-west-1.compute.amazonaws.com"
    _flight_data_url: str = "http://localhost:8000"

    def __post_init__(self):
        print("Authenticating with CMI server...")
        try:
            self.auth_token = self._authenticate()
            print("Authenticated successfully.")
        except Exception as e:
            print(f"Authentication failed: {e}")
            print("Running in SIMULATION MODE (no live trading).")
            self.auth_token = None

        self.products = self.get_all_products()
        print(f"Available products: {self.products}")

        # 启动行情与成交监听
        for product in self.products:
            product_url = f"{self._cmi_url}/api/order/{product}/stream"
            thread = SSEThread(
                bearer=self.auth_token,
                url=product_url,
                handler_method=self._on_orderbook_wrapper,
            )
            thread.daemon = True
            thread.start()
            print(f"Started SSE thread for {product}")

        trades_url = f"{self._cmi_url}/api/trade/stream"
        trades_thread = SSEThread(
            bearer=self.auth_token, url=trades_url, handler_method=self.on_trade
        )
        trades_thread.daemon = True
        trades_thread.start()
        print("Started trade stream listener")

        # 启动航班数据拉取线程
        flight_thread = threading.Thread(target=self._pull_flight_data)
        flight_thread.daemon = True
        flight_thread.start()

    # ========== 抽象回调 ==========
    @abstractmethod
    def on_orderbook(self, orderbook):
        pass

    @abstractmethod
    def on_trade(self, trade):
        pass

    # ========== 下单逻辑 ==========
    def send_order(self, product: str, price: float, side: Literal["BUY", "SELL"], volume: int):
        if not self.auth_token:
            print("[SIMULATION] Would send order:", product, side, price, volume)
            return
        payload = {"product": product, "side": side, "price": price, "volume": volume}
        url = f"{self._cmi_url}/api/order"
        response = requests.post(url, json=payload, headers=_get_headers(self.auth_token))
        if response.status_code == 200:
            return response.json()
        else:
            print(f"Failed to send order {payload}: {response.text}")

    def get_all_orders(self) -> Optional[List[Dict]]:
        if not self.auth_token:
            return []
        url = f"{self._cmi_url}/api/order/current-user"
        response = requests.get(url, headers=_get_headers(self.auth_token))
        return response.json() if response.status_code == 200 else []

    def cancel_all_orders(self):
        if not self.auth_token:
            print("[SIMULATION] No live orders to cancel.")
            return
        for order in self.get_all_orders():
            url = f'{self._cmi_url}/api/order/{order["id"]}'
            requests.delete(url, headers=_get_headers(self.auth_token))

    # ========== 产品、仓位 ==========
    def get_all_products(self) -> List[str]:
        url = f"{self._cmi_url}/api/product"
        try:
            response = requests.get(url, headers=STANDARD_HEADERS)
            if response.status_code == 200:
                return [p["symbol"] for p in json.loads(response.text)]
        except:
            pass
        print("Failed to fetch product list, using defaults.")
        return ["EQUATOR", "FLIGHTS"]

    @property
    def positions(self) -> Dict[str, int]:
        if not self.auth_token:
            return {}
        url = f"{self._cmi_url}/api/position/current-user"
        response = requests.get(url, headers=_get_headers(self.auth_token))
        if response.status_code == 200:
            return {p["product"]: p["volume"] for p in response.json()}
        return {}

    # ========== 航班数据与理论价 ==========
    def _pull_flight_data(self):
        while True:
            try:
                response = requests.get(self._flight_data_url)
                if response.status_code == 200:
                    self.flight_data = pd.DataFrame(response.json())
                else:
                    self._simulate_flight_data()
            except:
                self._simulate_flight_data()
            time.sleep(5)

    def _simulate_flight_data(self):
        # 模拟一些航班信息
        self.flight_data = pd.DataFrame({
            "on_ground": [random.choice([True, False]) for _ in range(30)],
            "has_crossed_equator": [random.choice([True, False]) for _ in range(30)],
            "last_update_time": [dt.datetime.now(dt.timezone.utc) for _ in range(30)]
        })

    @property
    def number_of_flights_in_air(self) -> float:
        if self.flight_data.empty:
            return 0.0
        now = dt.datetime.now(dt.timezone.utc)
        recent = self.flight_data[
            (~self.flight_data["on_ground"]) &
            ((now - pd.to_datetime(self.flight_data["last_update_time"])) < dt.timedelta(minutes=5))
        ]
        return len(recent)

    @property
    def number_crossed_equator(self) -> int:
        if self.flight_data.empty:
            return 0
        return len(self.flight_data.loc[self.flight_data["has_crossed_equator"]])

    @property
    def theos(self) -> Dict[str, float]:
        return {
            "EQUATOR": self.number_crossed_equator,
            "FLIGHTS": self.number_of_flights_in_air,
        }

    # ========== 登录 ==========
    def _authenticate(self) -> str:
        auth = {"username": self.username, "password": self.password}
        url = f"{self._cmi_url}/api/user/authenticate"
        response = requests.post(url, headers=STANDARD_HEADERS, json=auth)
        response.raise_for_status()
        return response.headers["Authorization"]

    # ========== 行情包装 ==========
    def _on_orderbook_wrapper(self, orderbook: Dict[str, Any]):
        orderbook["product"] = orderbook.pop("productsymbol", "UNKNOWN")
        orderbook["buy_orders"] = sorted(
            [{"price": float(price), "volume": volumes["marketVolume"], "own_volume": volumes["userVolume"]}
             for price, volumes in orderbook.get("buyOrders", {}).items()],
            key=lambda d: -d["price"]
        )
        orderbook["sell_orders"] = sorted(
            [{"price": float(price), "volume": volumes["marketVolume"], "own_volume": volumes["userVolume"]}
             for price, volumes in orderbook.get("sellOrders", {}).items()],
            key=lambda d: d["price"]
        )
        self.on_orderbook(orderbook)


# ======================================
# ========== 做市机器人 ================
# ======================================
@dataclass
class MarketMakingBot(CMIBot):
    min_tick_width: float = 2
    volume: int = 10

    def __post_init__(self):
        super().__post_init__()
        self.tick_sizes = {"EQUATOR": 1, "FLIGHTS": 1}
        self.lock = threading.Lock()
        self.thread = threading.Thread(target=self._market_make)
        self.thread.daemon = True
        self.thread.start()

    def on_orderbook(self, orderbook):
        pass

    def on_trade(self, trade):
        print(f"Trade detected: {trade}")
        print(f"Current positions: {self.positions}")

    def _market_make(self):
        while True:
            with self.lock:
                self.cancel_all_orders()
                for product, theo in self.theos.items():
                    tick = self.tick_sizes.get(product, 1)
                    rounded = round(theo / tick) * tick
                    buy_price = rounded - (self.min_tick_width // 2) * tick
                    sell_price = rounded + (self.min_tick_width // 2) * tick
                    print(f"[{product}] Posting BUY@{buy_price}, SELL@{sell_price}")
                    self.send_order(product, buy_price, "BUY", self.volume)
                    self.send_order(product, sell_price, "SELL", self.volume)
            time.sleep(3)


# ======================================
# ========== SSE 监听线程 ==============
# ======================================
class SSEThread(threading.Thread):
    def __init__(self, bearer: str, url: str, handler_method: Callable[[Dict], Any]):
        super().__init__()
        self.bearer = bearer
        self.url = url
        self.handler_method = handler_method

    def run(self):
        while True:
            try:
                headers = {"Authorization": self.bearer, "Accept": "text/event-stream; charset=utf-8"}
                messages = SSEClient(self.url, headers=headers)
                for msg in messages:
                    if msg.data:
                        self.handler_method(json.loads(msg.data))
            except Exception as e:
                print(f"SSE error: {e}, retrying in 3s...")
                time.sleep(3)


# ======================================
# ========== 主程序入口 =================
# ======================================
if __name__ == "__main__":
    bot = MarketMakingBot(
        username="test_user",
        password="test_pass",
        min_tick_width=2,
        volume=5
    )

    print("\n============================================")
    print(" Market Making Bot Running ")
    print("============================================\n")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nShutting down... Cancelling all orders...")
        bot.cancel_all_orders()
        print("Stopped successfully.")