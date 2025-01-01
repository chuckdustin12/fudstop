from collections import defaultdict
from datetime import datetime, timedelta
class EquityOptionTradeMonitor:
    def __init__(self):
        self.symbol_timestamps = defaultdict(list)
        self.timespan = timedelta(minutes=30)  # Example timespan of 30 minutes
        self.last_ticker = None
        self.consecutive_count = 0

        self.last_five_data = []

    async def repeated_hits(self, data):
        ticker = data.get('ticker')

        # Check if the current ticker is the same as the last ticker
        if ticker == self.last_ticker:
            self.consecutive_count += 1
            self.last_five_data.append(data)
            # If the count exceeds 5, remove the oldest entry
            if len(self.last_five_data) > 5:
                self.last_five_data.pop(0)
        else:
            # Reset the count and update the last ticker
            self.consecutive_count = 1
            self.last_ticker = ticker
            self.last_five_data = [data]

        # Return the last five data entries if the count is 5
        if self.consecutive_count == 5:
            return self.last_five_data
        
        