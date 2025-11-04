import random
from dataclasses import dataclass

from bots.imc_bots.imc_bot import IMCBot


@dataclass
class LiquidityTakingBot(IMCBot):
    random_order_chance: float = 0.15
    random_order_percent_threshold: float = 0.1

    def on_orderbook(self, orderbook):
        if random.uniform(0, 1) < self.random_order_chance:
            order_side = random.choice(["BUY", "SELL"])
            orderbook_side = f"{order_side.lower()}_orders"
            if len(orderbook[orderbook_side]) == 0:
                return
            best_level = orderbook[orderbook_side][0]
            product = orderbook["product"]
            price = best_level["price"]
            if best_level["own_volume"] != 0:
                return
            if (
                not abs((price - self.theos[product]) / price)
                < self.random_order_percent_threshold
            ):
                return
            volume = max(random.randrange(int(best_level["volume"])), 5)
            side = "BUY" if order_side == "SELL" else "SELL"
            self.send_order(product=product, price=price, side=side, volume=volume)

    def on_trade(self, trade):
        pass
