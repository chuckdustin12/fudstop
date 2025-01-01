import os
from dotenv import load_dotenv

from datetime import datetime, timedelta
import pytz
from typing import Union


class DateTimeSDK:
    def __init__(self):
        """Helps with date-time related conversions and work."""
        pass


    def convert_to_standard_timestamp(self, timestamp: Union[str, int]) -> str:
        """
        Converts any given timestamp into the standard format 'YYYY-MM-DD HH:MM:SS' if possible.
        The function tries different common date formats to parse the input timestamp and also handles integer UNIX timestamps.

        Parameters:
        timestamp (Union[str, int]): The timestamp string or integer UNIX timestamp to convert.

        Returns:
        str: The timestamp in 'YYYY-MM-DD HH:MM:SS' format or an error message if conversion is not possible.
        """
        # Check if the input is an integer (UNIX timestamp)
        if isinstance(timestamp, int):
            try:
                # Convert UNIX timestamp to datetime, ensure timestamp is non-negative
                if timestamp < 0:
                    return "Invalid UNIX timestamp: Timestamp cannot be negative."
                return datetime.utcfromtimestamp(timestamp).strftime("%Y-%m-%d %H:%M:%S")
            except OverflowError as oe:
                return f"Error converting UNIX timestamp: {oe} (timestamp may be too large or too small)"
            except OSError as oserr:
                return f"Error converting UNIX timestamp: {oserr} (invalid argument)"
            except Exception as e:
                return f"Error converting UNIX timestamp: {e}"

        # If input is not an integer, it should be a string of date
        elif isinstance(timestamp, str):
            # Common timestamp formats to try
            date_formats = [
                "%Y-%m-%d %H:%M:%S",   # 2023-04-14 12:00:00
                "%Y-%m-%dT%H:%M:%S",   # 2023-04-14T12:00:00
                "%m/%d/%Y %H:%M:%S",   # 04/14/2023 12:00:00
                "%m/%d/%y %H:%M:%S",   # 04/14/23 12:00:00
                "%m-%d-%Y %H:%M:%S",   # 04-14-2023 12:00:00
                "%d-%m-%Y %H:%M:%S",   # 14-04-2023 12:00:00
                "%Y/%m/%d %H:%M:%S",   # 2023/04/14 12:00:00
                "%B %d, %Y %H:%M:%S",  # April 14, 2023 12:00:00
                "%b %d, %Y %H:%M:%S",  # Apr 14, 2023 12:00:00
                "%Y%m%d%H%M%S",        # 20230414120000
                "%Y-%m-%d",            # 2023-04-14 (date only)
                "%m/%d/%Y",            # 04/14/2023 (date only)
                "%Y%m%d"               # 20230414 (date only)
            ]
            
            for fmt in date_formats:
                try:
                    # Attempt to parse the timestamp using the current format
                    parsed_date = datetime.strptime(timestamp, fmt)
                    # Successfully parsed, now format it to the standard format
                    return parsed_date.strftime("%Y-%m-%d %H:%M:%S")
                except ValueError:
                    # If the format does not match, continue to the next format
                    continue

            # If no format matched, return an error message
            return "Invalid timestamp format provided. Unable to convert."
        else:
            return "Unsupported type for timestamp. Please provide an integer or string."


    # Example usage: