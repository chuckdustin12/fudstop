from .models import OptionsData, Profeed
import httpx


class BlotterSDK:
    def __init__(self):

        pass

    def create_payload(
            self,
        symbol: str,
        trade_thesis: str,
        instrument: str,
        longshort: str,
        paid: str,
        time_horizon: str,
        options_type: str,
        options_expiration: str,
        strike_price: int,
        symbol_price_hidden: float,
        contract: str,
        entry_date: str = None,
        exit_date: str = None,
    ):
        """
        Create a parameterized payload for a submission.

        Args:
            symbol (str): Stock symbol.
            trade_thesis (str): Trade thesis description.
            instrument (str): Type of instrument (e.g., options, stocks).
            longshort (str): Long or short position.
            paid (str): Payment type (free/paid).
            image_input (str): Image input file or URL.
            time_horizon (str): Time horizon for the trade (e.g., swing, day).
            options_type (str): Type of option (call/put).
            options_expiration (str): Expiration date for the option.
            strike_price (int): Strike price of the option.
            symbol_price_hidden (float): Hidden price of the symbol.
            contract (str): Option contract ID.
            entry_date (str): Trade entry date.
            exit_date (str): Trade exit date.

        Returns:
            dict: Parameterized payload.
        """
        payload = {
            "submission_symbol": symbol,
            "submission_trade_thesis": trade_thesis,
            "submission_instument": instrument,
            "submission_longshort": longshort,
            "submission_paid": paid,
            "submission_time_horizon": time_horizon,
            "submission_options_type": options_type,
            "submission_options_expiration": options_expiration,
            "submission_strike_price": strike_price,
            "symbol_price_hidden": symbol_price_hidden,
            "submission_contract": contract,
            "entry_date": entry_date,
            "exit_date": exit_date,
        }
        payload = {k: v for k, v in payload.items() if v is not None}
        return payload


    # Async function to fetch and display combined DataFrame
    async def options_chains(self, ticker):
        """
        Fetch options chain data for a given ticker, parse it, and display combined options.
        Args:
            ticker (str): Stock symbol.
        """
        async with httpx.AsyncClient() as client:
            try:
                # Fetch data
                response = await client.get(f"https://blotter.fyi/get_options_chains_api?symbol={ticker}")
                response.raise_for_status()
                raw_data = response.json()

                # Process data using OptionsData
                options_data = OptionsData(raw_data)

    


                return options_data

            except httpx.HTTPStatusError as e:
                print(f"HTTP error occurred: {e}")
            except Exception as e:
                print(f"An error occurred: {e}")


    async def get_blotter_trades(self):
        """Gets pro-feed trade data."""

        async with httpx.AsyncClient() as client:
            data = await client.get("https://blotter.fyi/get_top_trades_for_feed_web?feed_name=pro")

            data = data.json()
            data = data['data']

            return Profeed(data)



    async def post_trade(self, ticker):
        """
        Main async function to fetch option chains, process data, and submit a trade.
        """
        # Step 1: Fetch options chain data
        options_data = await self.options_chains(ticker)

        # Step 2: Combine options data and process
        combined_df = options_data.get_combined_options()

        # Step 3: Sort options by implied volatility (IV) ascending
        combined_df = combined_df.sort_values('iv', ascending=True)

        print("\nCombined Options DataFrame Sorted by IV:")
        print(combined_df)

        # Step 4: Extract the option with the lowest IV
        lowest_iv_option = combined_df.iloc[0]

        # Step 5: Create the payload dynamically
        payload = self.create_payload(
            symbol=ticker,
            strike_price=lowest_iv_option['strike'],
            trade_thesis="Trading the skew. Current IV is lowest at the strike price traded here.",
            instrument="options",
            longshort='long',
            paid='free',
            time_horizon='Swing',
            options_type=lowest_iv_option['option_type'].lower(),
            options_expiration=lowest_iv_option['expiry'],
            contract=lowest_iv_option['option_symbol'],
            symbol_price_hidden=lowest_iv_option['price'],
            entry_date="",
            exit_date=""
        )

        print("\nPayload for Submission:")
        print(payload)

        # Step 6: Submit the form-data payload
        try:
            async with httpx.AsyncClient() as client:
                data = await client.post("https://blotter.fyi/submit_trade_api", json=payload)
                if data.status_code == 200:

                    print(f"SUCCESS!: {data.text}")
                
        except Exception as e:
            print(f"Error submitting payload: {e}")

