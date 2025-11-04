from dataclasses import dataclass
from bots.imc_bots.imc_bot import IMCBot


@dataclass
class TestOrderBot(IMCBot):
    """Simple bot that connects to CMI AWS instance and posts test orders"""

    def __post_init__(self):
        print(f"Connecting to CMI instance at: {self._cmi_url}")
        super().__post_init__()
        print("Bot initialized successfully!")
        self.order_count = 0

    def on_orderbook(self, orderbook):
        """Called when orderbook updates are received"""
        product = orderbook["product"]
        print(f"\n=== Orderbook update for {product} ===")

        # Print best bid and ask
        if orderbook["buy_orders"]:
            best_bid = orderbook["buy_orders"][0]
            print(f"Best Bid: {best_bid['price']} (volume: {best_bid['volume']})")

        if orderbook["sell_orders"]:
            best_ask = orderbook["sell_orders"][0]
            print(f"Best Ask: {best_ask['price']} (volume: {best_ask['volume']})")

        # Post a test order every few orderbook updates
        if self.order_count < 5:  # Limit to 5 orders to avoid spam
            self.order_count += 1

            # Try to post a buy order below best bid
            if orderbook["buy_orders"]:
                best_bid_price = orderbook["buy_orders"][0]["price"]
                # Post order 1 tick below best bid
                test_price = best_bid_price - 1
                print(f"Attempting to post BUY order: {product} @ {test_price} for 1 unit")
                result = self.send_order(product, test_price, "BUY", 1)
                if result:
                    print(f"Order posted successfully: {result}")
            else:
                print("No buy orders in book to reference")

    def on_trade(self, trade):
        """Called when trades occur"""
        print(f"Trade: {trade}")


if __name__ == "__main__":
    # Create bot instance with credentials
    # Using a test username - adjust as needed
    bot = TestOrderBot(
        username="test_bot_user",
        password="test_password_123"
    )

    print("\nBot is running... Press Ctrl+C to stop")

    # Keep the main thread alive
    try:
        import time
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nShutting down bot...")
