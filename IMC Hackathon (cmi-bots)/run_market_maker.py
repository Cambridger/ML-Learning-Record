from bots.imc_bots.market_making_bot import MarketMakingBot
import time

if __name__ == "__main__":
    bot = MarketMakingBot(
        username="Eabo",
        password="12345678",
        min_tick_width=2, 
        volume=10
    )

    print("\n" + "="*50)
    print("Market Making Bot is running...")
    print("="*50)
    print("\nThe bot will:")
    print("- Post quotes on EQUATOR and FLIGHTS")
    print("- Update quotes every 3 seconds")
    print("- Use theoretical values based on flight data")
    print("\nPress Ctrl+C to stop\n")

    # Keep the main thread alive
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n\nShutting down bot...")
        print("Cancelling all orders...")
        bot.cancel_all_orders()
        print("Bot stopped successfully.")
