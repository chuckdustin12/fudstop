import pandas as pd
import matplotlib.pyplot as plt



file_path = 'data/oic/options_monitor.csv'

def plot_options_data(file_path):

    options_data = pd.read_csv(file_path)

    # Setting the style for darker visualizations
    plt.style.use('dark_background')

    # Visualization of Volume and Theoretical Price
    plt.figure(figsize=(12, 6))

    # Volume (represented by open interest) vs Theoretical Price
    plt.scatter(options_data['call_theoprice'], options_data['put_openinterest_eod'], label='Put Open Interest vs Theoretical Price', alpha=0.7, color='cyan')

    plt.title('Put Open Interest vs Theoretical Price')
    plt.xlabel('Theoretical Price')
    plt.ylabel('Open Interest')
    plt.legend()
    plt.grid(True, color='gray')
    plt.show()

    # Visualization of Greeks - Theta and Vega
    plt.figure(figsize=(12, 6))

    # Theta and Vega
    plt.scatter(options_data['call_days'], options_data['put_theta_eod'], label='Put Theta EOD', alpha=0.7, color='magenta')
    plt.scatter(options_data['call_days'], options_data['call_vega_eod'], label='Call Vega EOD', alpha=0.7, color='yellow')

    plt.title('Put Option Theta and Call Option Vega vs Days to Expiration')
    plt.xlabel('Days to Expiration')
    plt.ylabel('Greek Value')
    plt.legend()
    plt.grid(True, color='gray')
    plt.show()

    # Visualization of Price Changes
    plt.figure(figsize=(12, 6))

    # Call and Put price changes at end of day
    plt.scatter(options_data['call_days'], options_data['call_change_eod'], label='Call Change EOD', alpha=0.7, color='lightgreen')
    plt.scatter(options_data['call_days'], options_data['put_alpha_eod'], label='Put Alpha EOD', alpha=0.7, color='orange')

    plt.title('Call and Put Price Changes vs Days to Expiration')
    plt.xlabel('Days to Expiration')
    plt.ylabel('Price Change')
    plt.legend()
    plt.grid(True, color='gray')
    plt.savefig('/mnt/data/')
    plt.show()


