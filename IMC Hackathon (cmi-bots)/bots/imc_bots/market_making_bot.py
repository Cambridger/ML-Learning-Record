import threading
import time
from dataclasses import dataclass

from bots.imc_bots.imc_bot import IMCBot


@dataclass
class MarketMakingBot(IMCBot):
    min_tick_width: float = 3
    volume: int = 100

    def __post_init__(self):
        super().__post_init__()
        self.tick_sizes = {"EQUATOR": 1, "FLIGHTS": 1}
        threading.Lock()
        self.market_making_thread = threading.Thread(target=self._market_make)
        self.market_making_thread.daemon = True
        self.market_making_thread.start()

    def on_orderbook(self, orderbook):
        pass

    def on_trade(self, trade):
        print("trade detected")
        print(self.positions)

    def _market_make(self):
        while True:
            self.cancel_all_orders()
            for product, theo in self.theos.items():
                tick_size = self.tick_sizes[product]
                rounded_theo = round(theo / tick_size) * tick_size
                buy_price = rounded_theo - (self.min_tick_width // 2) * tick_size
                sell_price = rounded_theo + (self.min_tick_width // 2) * tick_size
                self.send_order(product, buy_price, "BUY", self.volume)
                self.send_order(product, sell_price, "SELL", self.volume)
            time.sleep(3)
