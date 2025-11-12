import asyncio
from polygon.websocket import WebSocketClient, Market, CryptoTrade
from polygon.websocket.models import WebSocketMessage
from typing import List
import asyncio
import os
from dotenv import load_dotenv
load_dotenv()
import logging

class LiveCrypto:
    """Set key in .env file as YOUR_POLYGON_KEY"""
    def __init__(self):
        api_key = os.environ.get('YOUR_POLYGON_KEY')
        if not api_key:
            logging.error("Polygon API Key is missing!")
        self.c = WebSocketClient(subscriptions=["XT.*"], market="crypto", api_key=api_key)  # Adjust as necessary
        logging.basicConfig(level=logging.DEBUG)  # Enable debug logging

    async def handle_msg(self, msgs):
        """Process the messages received"""
        for m in msgs:
            # Log or process the received message
            logging.debug(f"Received message: {m}")
            if isinstance(m, CryptoTrade):
                # Example of handling data: logging or processing it
                logging.info(f"Trade pair: {m.pair}, Price: {m.price}, Size: {m.size}")
                return m
    async def run_crypto(self):
        """Establish WebSocket connection and continuously listen for messages"""
        try:
            logging.debug("Connecting to Polygon WebSocket...")
            await self.c.connect(self.handle_msg)  # Connect and start listening
        except Exception as e:
            logging.error(f"Error connecting or receiving messages: {e}")

