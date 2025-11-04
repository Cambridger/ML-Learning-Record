import json
from functools import cached_property
from random import uniform

from sseclient import SSEClient
import requests
import threading
import pandas as pd
from dataclasses import dataclass
import datetime as dt
from threading import Thread
from typing import Optional, List, Any, Literal, Dict, Callable
from abc import ABC, abstractmethod
import time

STANDARD_HEADERS = {"Content-Type": "application/json; charset=utf-8"}


def _get_headers(auth_token: str) -> Dict[str, str]:
    return {**STANDARD_HEADERS, "Authorization": auth_token}


@dataclass
class CMIBot(ABC):
    username: str
    password: str
    flight_data: pd.DataFrame = pd.DataFrame()
    _cmi_url: str = "http://ec2-34-249-163-3.eu-west-1.compute.amazonaws.com"
    _flight_data_url: str = (
        "http://localhost:8000"
    )

    @cached_property
    def auth_token(self):
        return self._authenticate()

    def __post_init__(self):
        self.products = self.get_all_products()
        for product in self.products:
            product_url = f"{self._cmi_url}/api/order/{product}/stream"
            product_thread = SSEThread(
                bearer=self.auth_token,
                url=product_url,
                handler_method=self._on_orderbook_wrapper,
            )
            print("About to start SSEThread " + product)
            product_thread.start()

        trades_url = f"{self._cmi_url}/api/trade/stream"
        trades_thread = SSEThread(
            bearer=self.auth_token, url=trades_url, handler_method=self.on_trade
        )
        trades_thread.start()
        flight_data_thread = threading.Thread(target=self._pull_flight_data)
        flight_data_thread.daemon = True
        flight_data_thread.start()

    @abstractmethod
    def on_orderbook(self, orderbook):
        raise NotImplementedError("You must implement the on_orderbook method!")

    @abstractmethod
    def on_trade(self, trade):
        raise NotImplementedError("You must implement the on_trade method!")

    def send_order(
        self, product: str, price: float, side: Literal["BUY", "SELL"], volume: int
    ) -> Dict:
        payload = {"product": product, "side": side, "price": price, "volume": volume}
        url = f"{self._cmi_url}/api/order"
        response = requests.post(
            url, json=payload, headers=_get_headers(self.auth_token)
        )
        if response.status_code == 200:
            return response.json()
        else:
            print(f"Failed to send order, {payload}")
            print(response.json())

    def get_all_orders(self) -> Optional[List[Dict]]:
        url = f"{self._cmi_url}/api/order/current-user"
        response = requests.get(url, headers=_get_headers(self.auth_token))
        if response.status_code == 200:
            return response.json()
        else:
            print(f"Failed to get all orders: {response.json()}")

    def cancel_order(self, product, price) -> Dict:
        url = f"{self._cmi_url}/api/order?product={product}&price={price}"
        response = requests.delete(url, headers=_get_headers(self.auth_token))
        if response.status_code == 200:
            return response.json()
        else:
            print(f"Failed to delete order: {response.json()}")

    def cancel_all_orders(self):
        for order in self.get_all_orders():
            url = f'{self._cmi_url}/api/order/{order["id"]}'
            response = requests.delete(url, headers=_get_headers(self.auth_token))
            if response.status_code != 200:
                print("Failed to cancel order")
                print(response.json())

    def get_all_products(self) -> List[str]:
        url = f"{self._cmi_url}/api/product"
        response = requests.get(url, headers=STANDARD_HEADERS)
        if response.status_code == 200:
            return [product["symbol"] for product in json.loads(response.text)]
        else:
            print("Failed to get all products")
            print(response.json())

    @property
    def positions(self) -> Dict[str, int]:
        url = f"{self._cmi_url}/api/position/current-user"
        response = requests.get(url, headers=_get_headers(self.auth_token))
        if response.status_code == 200:
            return {
                position["product"]: position["volume"] for position in response.json()
            }
        else:
            print("Failed to get positions")
            print(response.json())
            return dict()

    @property
    def number_of_flights_in_air(self) -> float:
        if len(self.flight_data) == 0:
            return 0.0
        now = dt.datetime.now(dt.timezone.utc)
        return len(
            self.flight_data[
                (~self.flight_data["on_ground"])
                & (
                    (now - pd.to_datetime(self.flight_data["last_update_time"]))
                    < dt.timedelta(minutes=5)
                )
            ]
        )

    @property
    def number_crossed_equator(self) -> int:
        if len(self.flight_data) == 0:
            return 0
        return len(self.flight_data.loc[self.flight_data["has_crossed_equator"]])

    def _authenticate(self) -> str:
        auth = {"username": self.username, "password": self.password}
        url = f"{self._cmi_url}/api/user/authenticate"
        response = requests.post(url, headers=STANDARD_HEADERS, json=auth)
        response.raise_for_status()
        return response.headers["Authorization"]

    def _pull_flight_data(self):
        pass

    def _on_orderbook_wrapper(self, orderbook: Dict[str, Any]):
        orderbook["product"] = orderbook.pop("productsymbol")
        orderbook["buy_orders"] = sorted(
            [
                {
                    "price": float(price),
                    "volume": volumes["marketVolume"],
                    "own_volume": volumes["userVolume"],
                }
                for price, volumes in orderbook["buyOrders"].items()
            ],
            key=lambda d: -d["price"],
        )
        orderbook["sell_orders"] = sorted(
            [
                {
                    "price": float(price),
                    "volume": int(volumes["marketVolume"]),
                    "own_volume": int(volumes["userVolume"]),
                }
                for price, volumes in orderbook["sellOrders"].items()
            ],
            key=lambda d: d["price"],
        )
        del orderbook["buyOrders"]
        del orderbook["sellOrders"]
        del orderbook["midPrice"]
        self.on_orderbook(orderbook)


class SSEThread(Thread):
    bearer: str
    url: str
    handler_method: Callable[[Dict], Any]

    def __init__(self, bearer: str, url: str, handler_method: Callable[[Dict], Any]):
        super().__init__()
        self.bearer = bearer
        self.url = url
        self.handler_method = handler_method

    def run(self):
        while True:
            try:
                self._start_sse_client()
            except:
                pass

    def _start_sse_client(self):
        headers = {
            "Authorization": self.bearer,
            "Accept": "text/event-stream; charset=utf-8",
        }
        messages = SSEClient(self.url, headers=headers)
        for msg in messages:
            if msg.id is not None:
                self.handler_method(json.loads(msg.event))
