from webull import webull as wb
from typing import List, Any
import subprocess
from plotly.subplots import make_subplots
from fudstop4.apis.polygonio.polygon_options import PolygonOptions
db = PolygonOptions(database='fudstop3')
poly = PolygonOptions(user='chuck', database='charlie', host='localhost', port=5432, password='fud')
from kaleido.scopes.plotly import PlotlyScope
from concurrent.futures import ThreadPoolExecutor
import asyncio
PLT_3DMESH_HOVERLABEL = dict(bgcolor="gold")
PLT_3DMESH_STYLE_TEMPLATE = "plotly_dark"

scope = PlotlyScope(
    plotlyjs="plotly.js"),
# Chart Plots Settings
PLT_CANDLE_STYLE_TEMPLATE = "plotly_dark"
PLT_CANDLE_INCREASING = "#00ACFF"
PLT_CANDLE_DECREASING = "#e4003a"
PLT_CANDLE_VOLUME = "#fdc708"
PLT_CANDLE_NEWS_MARKER = "rgba(255, 215, 0, 0.9)"
PLT_CANDLE_NEWS_MARKER_LINE = "gold"
PLT_CANDLE_YAXIS_TEXT_COLOR = "#fdc708"
PLT_SCAT_STYLE_TEMPLATE = "plotly_dark"
PLT_SCAT_INCREASING = "#00ACFF"
PLT_SCAT_DECREASING = "#e4003a"
PLT_SCAT_PRICE = "#fdc708"
PLT_TA_STYLE_TEMPLATE = "plotly_dark"
PLT_FONT = dict(
    family="Fira Code",
    size=12,
)
PLT_TA_COLORWAY = [
    "#fdc708",
    "#d81aea",
    "#00e6c3",
    "#9467bd",
    "#e250c3",
    "#d1fa3d",
]
PLT_FIB_COLORWAY: List[Any] = [
    "rgba(195, 50, 69, 1)",  # 0
    "rgba(130, 38, 96, 1)",  # 0.235
    "rgba(72, 39, 124, 1)",  # 0.382
    "rgba(0, 93, 168, 1)",  # 0.5
    "rgba(173, 0, 95, 1)",  # 0.618
    "rgba(253, 199, 8, 1)",  # 0.65 Golden Pocket
    "rgba(147, 103, 188, 1)",  # 1
    dict(
        family="Fire Code",
        size=16,
    ),  # Fib's Text
    dict(color="rgba(0, 230, 195, 1)", width=2, dash="dash"),  # Fib Trendline
]

import plotly.graph_objects as go
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import os
import matplotlib.pyplot as plt
import seaborn as sns
from mpl_toolkits.mplot3d import Axes3D
import numpy as np

import pandas as pd
import aiohttp
from aiohttp.client_exceptions import ClientConnectorError

# Import necessary libraries
import plotly.express as px
# Function to abbreviate numbers
def abbreviate_number(num):
    if num >= 1000000:
        return f"{num/1000000:.1f}M"
    elif num >= 1000:
        return f"{num/1000:.1f}K"
    else:
        return str(num)

async def plot_td9_chart(df, interval):
    df.index = df['Timestamp']
    df.index = pd.to_datetime(df['Timestamp'])
    fig = go.Figure()
    fig = make_subplots(rows=2, cols=1, shared_xaxes=True, row_heights=[0.8, 0.2], vertical_spacing=0.02)


    # Add Candlestick trace
    fig.add_trace(
        go.Candlestick(
            x=df.index.strftime('%Y-%m-%d %H:%M:%S'),  # Format timestamp
            open=df['Open'],
            high=df['High'],
            low=df['Low'],
            close=df['Close'],
            increasing_line_color=PLT_CANDLE_INCREASING,
            decreasing_line_color=PLT_CANDLE_DECREASING,
            name='OHLC'
        )
    )
    ticker = df['ticker']

    # Add Volume as a bar chart
    fig.add_trace(
        go.Bar(
            x=df.index.strftime('%Y-%m-%d %H:%M:%S'),  # Format timestamp
            y=df['Volume'],
            name='Volume',
            marker_color=PLT_CANDLE_VOLUME,
            yaxis='y2'
        )
    )



    # Apply your plot styles
    fig.update_layout(
        template=PLT_CANDLE_STYLE_TEMPLATE,
        title=f'TD9 Chart with RSI - {ticker} | {interval}',
        yaxis=dict(domain=[0.3, 1]),
        yaxis2=dict(domain=[0, 0.2]),
    )

    

    fig.write_html('temp_plot.html')
    # Get the absolute path of the HTML file
    file_path = os.path.abspath('temp_plot.html')
    # Selenium to capture image
    options = webdriver.ChromeOptions()


    options.add_argument('--headless')  # This argument enables headless mode
    options.add_argument('--ignore-certificate-errors')
    options.add_argument('--allow-running-insecure-content')
    options.add_argument('--proxy-bypass-list=<-loopback>;www.sec.gov')
    options.add_argument('--ignore-ssl-errors=yes')
    options.add_argument('--ignore-certificate-errors-spki-list')
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36")
    # Initialize WebDriver
    browser = webdriver.Chrome(options=options)


    browser.get(f'file://{file_path}')

    # Take screenshot
    screenshot_bytes = browser.get_screenshot_as_png()

    # Save the screenshot to a file (optional)
    with open('screenshot.jpg', 'wb') as f:
        f.write(screenshot_bytes)

    # Close the browser
    browser.quit()
    return screenshot_bytes


async def plot_oi_volume(all_options_df):
    # Apply the abbreviation function to the 'vol' and 'OI' columns
    agg_data = all_options_df.groupby('strike').agg({'vol': 'sum', 'OI': 'sum'}).reset_index()
    agg_data['vol_abbrev'] = agg_data['vol'].apply(abbreviate_number)
    agg_data['OI_abbrev'] = agg_data['OI'].apply(abbreviate_number)

    # Create the bar chart using Plotly
    fig = go.Figure()

    # Add Volume bars
    fig.add_trace(go.Bar(
        x=agg_data['strike'],
        y=agg_data['vol'],
        name='Volume',
        text=agg_data['vol_abbrev'],
        textposition='inside',
        marker=dict(color='blue')
    ))

    # Add OI bars
    fig.add_trace(go.Bar(
        x=agg_data['strike'],
        y=agg_data['OI'],
        name='Open Interest',
        text=agg_data['OI_abbrev'],
        textposition='inside',
        marker=dict(color='green')
    ))

    # Update the layout
    fig.update_layout(
        title='Consolidated OI and Volume by Strike',
        titlefont=dict(size=20),
        xaxis_title='Strike',
        yaxis_title='Aggregated Value',
        barmode='group',
        bargap=0.9,  # gap between bars of adjacent location coordinates
        bargroupgap=0.1,  # gap between bars of the same location coordinate
        template='plotly_dark'
    )



async def plot_calls_and_puts(ticker, underlying_price):
    call_all_options_df = await poly.get_option_chain_all(ticker, contract_type='call')
    put_all_options_df = await poly.get_option_chain_all(ticker, contract_type='put')


    call_data = call_all_options_df.df
    put_data = put_all_options_df.df

    # Calculate the underlying price (assuming it's a constant for all rows, hence taking the first row's value)
  

    # Calculate the range for 25% from the money
    lower_bound = underlying_price * 0.75
    upper_bound = underlying_price * 1.25

    # Filter the call and put data to include only the strikes within the specified range
    call_data_filtered = call_data[(call_data['strike'] >= lower_bound) & (call_data['strike'] <= upper_bound)]
    put_data_filtered = put_data[(put_data['strike'] >= lower_bound) & (put_data['strike'] <= upper_bound)]

    # Create the zoomed-in bar chart
    fig = go.Figure()

    # Add Call Volume bars
    fig.add_trace(go.Bar(
        x=call_data_filtered['strike'],
        y=call_data_filtered['vol'],
        name='Call Volume',
        text=call_data_filtered['vol_abbrev'],
        textposition='inside',
        marker=dict(color='blue')
    ))

    # Add Put Volume bars
    fig.add_trace(go.Bar(
        x=put_data_filtered['strike'],
        y=put_data_filtered['vol'],
        name='Put Volume',
        text=put_data_filtered['vol_abbrev'],
        textposition='inside',
        marker=dict(color='red')
    ))

    # Update the layout
    fig.update_layout(
        title=f'Call vs Put Volume by Strike (Zoomed-in on {lower_bound:.2f}-{upper_bound:.2f})',
        titlefont=dict(size=20),
        xaxis_title='Strike',
        yaxis_title='Volume',
        barmode='group',
        bargap=0.15,
        bargroupgap=0.1,
        template='plotly_dark'
    )

    # Save as HTML
    fig.write_html('/mnt/data/call_vs_put_volume_zoomed.html')
    fig.write_html('calls_vs_put.html')
    # Get the absolute path of the HTML file
    file_path = os.path.abspath('calls_vs_put.html')
    # Selenium to capture image
    options = webdriver.ChromeOptions()


    options.add_argument('--headless')  # This argument enables headless mode
    options.add_argument('--ignore-certificate-errors')
    options.add_argument('--allow-running-insecure-content')
    options.add_argument('--ignore-ssl-errors=yes')
    options.add_argument('--ignore-certificate-errors-spki-list')
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36")
    # Initialize WebDriver
    browser = webdriver.Chrome(options=options)


    browser.get(f'file://{file_path}')

    # Take screenshot
    screenshot_bytes = browser.get_screenshot_as_png()

    # Save the screenshot to a file (optional)
    with open('calls_vs_put.jpg', 'wb') as f:
        f.write(screenshot_bytes)

    # Close the browser
    browser.quit()

    return screenshot_bytes




async def volume_histogram(ticker):
    # Fetch data asynchronously
    options_df = await poly.get_option_chain_all(ticker)
    options_df = options_df.df
    
    # Check if the DataFrame is empty
    if options_df.empty:
        print("No data available for the given ticker.")
        return
    
    # Check if 'open interest' or 'vol' column exists, else return
    focus_column = 'vol' if 'OI' not in options_df.columns else 'OI'
    
    # Filter data for the specific ticker (optional)
    filtered_df = options_df[options_df['sym'] == ticker]
    
    # Check if the DataFrame is empty after filtering
    if filtered_df.empty:
        print(f"No data available for the ticker {ticker}.")
        return



    # Visualization 1: Histogram using Plotly with custom colors
    fig = px.histogram(
        filtered_df, 
        x="strike", 
        color="cp",
        nbins=30,
        title=f"Histogram of {focus_column.capitalize()} Across Different Strike Prices",
        labels={"strike": "Strike Price", "count": focus_column.capitalize()},
        color_discrete_map={"call": PLT_CANDLE_INCREASING, "put": PLT_CANDLE_DECREASING}
    )
    fig.update_layout(template=PLT_SCAT_STYLE_TEMPLATE)
   


async def plot_iv_surface(ticker):
    # Filter data for the specific ticker (optional)
    df = await poly.get_option_chain_all(ticker)
    df = df.df

    filtered_df = df[df['ticker'] == ticker]
    
    # Check if the DataFrame is empty after filtering
    if filtered_df.empty:
        print(f"No data available for the ticker {ticker}.")
        return
    
    # Convert the 'exp' column to datetime format for numerical conversion
    filtered_df['expiry'] = pd.to_datetime(filtered_df['expiry'])
    
    # Convert datetime to numerical format (e.g., days since the first date in the data)
    first_date = filtered_df['expiry'].min()
    filtered_df['expiry'] = (filtered_df['expiry'] - first_date).dt.days

    # Create the 3D surface plot
    surface_data = filtered_df.pivot_table(values='iv', index='strike', columns='expiry', aggfunc='mean', fill_value=0)
    
    fig = go.Figure(data=[go.Surface(z=surface_data.values, x=surface_data.columns, y=surface_data.index, colorscale='Viridis')])
    
    fig.update_layout(
        title=f'IV Surface for {ticker}',
        scene=dict(
            xaxis_title='Days to Expiration',
            yaxis_title='Strike Price',
            zaxis_title='Implied Volatility'
        ),
        template=PLT_SCAT_STYLE_TEMPLATE
    )
    

    fig.write_html('iv_surface.html')
    # Get the absolute path of the HTML file
    file_path = os.path.abspath('iv_surface.html')
    # Selenium to capture image
    options = webdriver.ChromeOptions()


    options.add_argument('--headless')  # This argument enables headless mode
    options.add_argument('--ignore-certificate-errors')
    options.add_argument('--allow-running-insecure-content')
    options.add_argument('--proxy-bypass-list=<-loopback>;www.sec.gov')
    options.add_argument('--ignore-ssl-errors=yes')
    options.add_argument('--ignore-certificate-errors-spki-list')
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36")
    # Initialize WebDriver
    browser = webdriver.Chrome(options=options)


    browser.get(f'file://{file_path}')

    # Take screenshot
    screenshot_bytes = browser.get_screenshot_as_png()

    # Save the screenshot to a file (optional)
    with open('iv_surface.jpg', 'wb') as f:
        f.write(screenshot_bytes)

    # Close the browser
    browser.quit()
    return screenshot_bytes



# Function to plot consolidated gamma exposure across all strikes with custom styling and additional title information
async def plot_greek_exposure(ticker: str, greek:str):
    df = await poly.get_option_chain_all(ticker)
    df = df.df
    
    # Filter data for the specific ticker (optional)
    filtered_df = df[df['ticker'] == ticker]
    
    # Check if the DataFrame is empty after filtering
    if filtered_df.empty:
        print(f"No data available for the ticker {ticker}.")
        return
    
    # Group data by strike and sum up gamma
    greek_data = filtered_df.groupby(['strike'])[greek].sum().reset_index()
    
    # Find the strike with the most gamma
    max_greek_strike = greek_data.loc[greek_data[greek].idxmax()]['strike']
    

    # Create the plot with a color gradient based on gamma levels
    fig = px.bar(
        greek_data, 
        x='strike', 
        y=greek, 
        labels={'strike': 'Strike Price', greek: f'{greek} Exposure'},
        color=f'{greek}',  # Color gradient based on gamma levels
        color_continuous_scale='Viridis'
    )

    # Add a vertical line at the strike with maximum gamma
    fig.add_shape(
        go.layout.Shape(
            type="line",
            x0=max_greek_strike,
            x1=max_greek_strike,
            y0=0,
            y1=greek_data[greek].max(),
            line=dict(color="Gold", width=3, dash="dot")
        )
    )

    # Add annotations for gamma levels
    annotations = [
        dict(
            x=strike,
            y=gamma,
            xref="x",
            yref="y",
            text=f"{gamma:.2f}",
            showarrow=True,
            arrowhead=1,
            ax=0,
            ay=-40,
            font=dict(size=12, color='yellow' if strike == max_greek_strike else 'white')
        )
        for strike, gamma in zip(greek_data['strike'], greek_data[greek])
    ]

    # Update layout
    fig.update_layout(
        template='plotly_dark',
        title={
            'text': f"Consolidated {greek} Exposure for {ticker} (Max @ {max_greek_strike})",
            'y':0.95,
            'x':0.5,
            'xanchor': 'center',
            'yanchor': 'top'
        },
        font=dict(
            family="Courier New, monospace",
            size=14,
            color='white'
        ),
        annotations=annotations  # Add annotations to layout
    )





    fig.write_html(f'files/{greek}.html')
    # Get the absolute path of the HTML file
    file_path = os.path.abspath(f'files/{greek}.html')
    # Selenium to capture image
    options = webdriver.ChromeOptions()


    options.add_argument('--headless')  # This argument enables headless mode
    options.add_argument('--ignore-certificate-errors')
    options.add_argument('--allow-running-insecure-content')
    options.add_argument('--proxy-bypass-list=<-loopback>;www.sec.gov')
    options.add_argument('--ignore-ssl-errors=yes')
    options.add_argument('--ignore-certificate-errors-spki-list')
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36")
    # Initialize WebDriver
    browser = webdriver.Chrome(options=options)


    browser.get(f'file://{file_path}')

    # Take screenshot
    screenshot_bytes = browser.get_screenshot_as_png()

    # Save the screenshot to a file (optional)
    with open(f'files/{greek}.jpg', 'wb') as f:
        f.write(screenshot_bytes)

    # Close the browser
    browser.quit()
    return screenshot_bytes



async def plot_upside_downside(df, ticker):
  
    # Create subplots
    fig = make_subplots(rows=2, cols=1, shared_xaxes=True,
                        subplot_titles=('MACD & Signal', 'RSI & Histogram'),
                        vertical_spacing=0.1)

    # Plot MACD and Signal on the first subplot
    fig.add_trace(go.Scatter(x=df['timestamp'], y=df['macd'], mode='lines', name='MACD'), row=1, col=1)
    fig.add_trace(go.Scatter(x=df['timestamp'], y=df['signal'], mode='lines', name='Signal'), row=1, col=1)
    
    # Plot RSI on the second subplot with yellow color
    fig.add_trace(go.Scatter(x=df['timestamp'], y=df['rsi'], mode='lines', name='RSI', line=dict(color='yellow')), row=2, col=1)
    
    # Plot Histogram as bar on the second subplot
    fig.add_trace(go.Bar(x=df['timestamp'], y=df['histogram'], name='Histogram'), row=2, col=1)

    # Add horizontal lines at y=30 and y=70 on the second subplot
    fig.add_trace(go.Scatter(x=df['timestamp'], y=[30]*len(df), mode='lines', name='RSI 30', line=dict(color='green', dash='dash')), row=2, col=1)
    fig.add_trace(go.Scatter(x=df['timestamp'], y=[70]*len(df), mode='lines', name='RSI 70', line=dict(color='red', dash='dash')), row=2, col=1)
    
    # Update layout
    fig.update_layout(
        title=f'MACD, Signal, RSI & Histogram for {ticker}',
        template=PLT_SCAT_STYLE_TEMPLATE,
        xaxis_title='Timestamp',
        yaxis1_title='Value',
        yaxis2_title='Value',
    )

    fig.write_html('upside_downside.html')
    file_path = os.path.abspath('upside_downside.html')
    # Get the absolute path of the HTML file

    # Selenium to capture image
    options = webdriver.ChromeOptions()


    options.add_argument('--headless')  # This argument enables headless mode
    options.add_argument('--ignore-certificate-errors')
    options.add_argument('--allow-running-insecure-content')
    options.add_argument('--proxy-bypass-list=<-loopback>;www.sec.gov')
    options.add_argument('--ignore-ssl-errors=yes')
    options.add_argument('--ignore-certificate-errors-spki-list')
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36")
    # Initialize WebDriver
    browser = webdriver.Chrome(options=options)

    browser.get(f'file://{file_path}')

    # Take screenshot
    screenshot_bytes = browser.get_screenshot_as_png()

    # Save the screenshot to a file (optional)
    with open('chart.jpg', 'wb') as f:
        f.write(screenshot_bytes)

    # Close the browser
    browser.quit()
 
    return screenshot_bytes



async def plot_total_volume_by_expiry_async(df_calls: pd.DataFrame, df_puts: pd.DataFrame):




    
    fig = go.Figure(layout=go.Layout(
        template=PLT_CANDLE_STYLE_TEMPLATE,
        font=PLT_FONT,
    ))

    # Add bar for total call volume
    fig.add_trace(go.Bar(
        x=df_calls['Expiry'],
        y=df_calls['Total Call Volume'],
        name='Total Call Volume',
        marker_color='#00ACFF',  # PLT_CANDLE_INCREASING
        yaxis='y',
    ))

    # Add bar for total put volume
    fig.add_trace(go.Bar(
        x=df_puts['Expiry'],
        y=df_puts['Total Put Volume'],
        name='Total Put Volume',
        marker_color='#e4003a',  # PLT_CANDLE_DECREASING
        yaxis='y',
    ))

    # Add title, labels, and customize layout
    fig.update_layout(
        title='Total Call and Put Volume and Open Interest by Expiration Date',
        xaxis_title='Expiration Date',
        yaxis=dict(
            title='Total Volume',
            tickfont=dict(
                color="#fdc708"  # PLT_CANDLE_YAXIS_TEXT_COLOR
            )
        ),
        yaxis2=dict(
            title='Total Open Interest',
            overlaying='y',
            side='right',
        ),
    )


    fig.write_html('data/vol_calls_vs_puts.html')
    file_path = os.path.abspath('data/vol_calls_vs_puts.html')
    # Get the absolute path of the HTML file

    # Selenium to capture image
    options = webdriver.ChromeOptions()


    options.add_argument('--headless')  # This argument enables headless mode
    options.add_argument('--ignore-certificate-errors')
    options.add_argument('--allow-running-insecure-content')
    options.add_argument('--proxy-bypass-list=<-loopback>;www.sec.gov')
    options.add_argument('--ignore-ssl-errors=yes')
    options.add_argument('--ignore-certificate-errors-spki-list')
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36")
    # Initialize WebDriver
    browser = webdriver.Chrome(options=options)

    browser.get(f'file://{file_path}')

    # Take screenshot
    screenshot_bytes = browser.get_screenshot_as_png()

    # Save the screenshot to a file (optional)
    with open('data/vol_calls_vs_puts.jpg', 'wb') as f:
        f.write(screenshot_bytes)

    # Close the browser
    browser.quit()
 
    return screenshot_bytes





# async def main():

#     df = pd.read_csv('all_options.csv')

#     await plot_gamma_exposure('AAPL')

# asyncio.run(main())