from langchain.schema import AIMessage, HumanMessage, SystemMessage
from langchain.chat_models import ChatOpenAI
from langchain.memory import ConversationBufferMemory
from langchain.prompts import (
    ChatPromptTemplate,
    MessagesPlaceholder,
    SystemMessagePromptTemplate,
    HumanMessagePromptTemplate,
)
from langchain.chains import LLMChain
from apis.webull.webull_trading import WebullTrading
from apis.stocksera_.stocksera_ import StockSera
trading = WebullTrading()
ss = StockSera()
import asyncio
import aiohttp
memory = ConversationBufferMemory()
import os

from datetime import datetime, timedelta

class Agent():
    def __init__(self):


        self.today = datetime.now().strftime('%Y-%m-%d')
        self.yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        self.tomorrow = (datetime.now() + timedelta(days=1)).strftime('%Y-%m-%d')
        self.thirty_days_ago = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
        self.thirty_days_from_now = (datetime.now() + timedelta(days=30)).strftime('%Y-%m-%d')
        self.fifteen_days_ago = (datetime.now() - timedelta(days=15)).strftime('%Y-%m-%d')
        self.fifteen_days_from_now = (datetime.now() + timedelta(days=15)).strftime('%Y-%m-%d')
        self.eight_days_from_now = (datetime.now() + timedelta(days=8)).strftime('%Y-%m-%d')
        self.eight_days_ago = (datetime.now() - timedelta(days=8)).strftime('%Y-%m-%d')
        self.llm = ChatOpenAI(temperature=0.3, max_tokens=250, openai_api_key=os.environ.get('YOUR_OPENAI_KEY'))
        
        # Prompt
        self.prompt = ChatPromptTemplate(
            messages=[
                SystemMessagePromptTemplate.from_template(
                    f"You are an options / stock market expert discussing market data. Reply as briefly as possible but still accurately conveying the data."
                ),
                # The `variable_name` here is what must align with memory
                MessagesPlaceholder(variable_name="chat_history"),
                HumanMessagePromptTemplate.from_template("{question}"),
            ]
        )
        self.memory = ConversationBufferMemory(memory_key="chat_history", return_messages=True)
        self.conversation = LLMChain(llm=self.llm, prompt=self.prompt, verbose=True, memory=self.memory)


    async def agent_volume_analysis(self, ticker):
        volume_analysis = await trading.volume_analysis(ticker)

        # Notice that we just pass in the `question` variables - `chat_history` gets populated by memory
        response = self.conversation({"question": f"What do these volume analysis data results mean for {ticker}? {volume_analysis.data_dict}? Compare with other results in memory if possible. (Do not bring this up in response)"})


        return response.get('text')
    

    async def agent_stock_data(self, ticker:str):
        """
        Pass in a ticker - the LLM will respond.
        
        """

        stock_data = await trading.get_stock_quote(ticker)


        response = self.conversation({"question": f"Here's the stats on the day for {ticker}: {stock_data.data_dict}. Return back with statistical analytical info. Make sure to convert the change ratio to proper format. For example. 0.08 should be 8%"})


        return response.get('text')
    


    async def agent_cost_distribution(self, ticker:str):
        """
        Pass in a ticker - the LLM will respond.
        
        """

        cost_dist = await trading.cost_distribution(ticker)


        response = self.conversation({"question": f"Here's the cost distribution data for {ticker} from {self.today}:  Avg cost: {cost_dist.avgCost}. Players profiting: {cost_dist.closeProfitRatio} Closing price of stock: ${cost_dist.close} Be sure to go over both days of data."})


        return response.get('text')
    


    async def agent_news(self, ticker:str):
        """
        Converse with AI about news
        """

        news = await trading.news(ticker, pageSize='5')

        response = self.conversation({"question": f"You are viewing data from the latest 5 news articles about {ticker}. Based on the 5 titles in this list: {news.title} - give an overall summary of the most recent news: {news.data_dict}. Provide the links: {news.news_url}"})

        return response.get('text')
    

    async def agent_short_vol(self, ticker:str):
        """
        Converse with AI about short volume
        """

        short_vol = ss.short_volume(ticker, date_from=self.eight_days_ago, date_to=self.today)
        print(short_vol)

        response = self.conversation({"question": f"You are viewing data from the latest 5 news articles about {ticker}. Based on the data in this list: {short_vol}.. what has the short percentage trend been for {ticker}?"})

        return response.get('text')
    
    
