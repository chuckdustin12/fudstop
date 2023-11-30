import pandas as pd
import matplotlib.pyplot as plt

# Define the visualization function
def plot_volume_by_strike_for_date(df, target_date, dark_background=True):
    # Extract data for the specific date
    target_df = df[df['expiry'] == pd.to_datetime(target_date)]
    
    # If no data available for the target_date, inform and return without plotting
    if target_df.empty:
        print(f"No data available for the date: {target_date}")
        return

    plt.style.use('dark_background' if dark_background else 'default')
    
    # Create a plot with dark background
    fig, ax = plt.subplots(figsize=(12, 6), facecolor='black' if dark_background else 'white')
    
    # Plot buy and sell volumes
    ax.bar(target_df['strike'], target_df['buy'], label='Buy Volume', color='green', alpha=0.7)
    ax.bar(target_df['strike'], -target_df['sell'], label='Sell Volume', color='red', alpha=0.7)
    
    # Set title and axes labels with gold color and high readability
    ax.set_title(f'Buy/Sell Volume by Strike for Expiry: {pd.to_datetime(target_date).strftime("%Y-%m-%d")}', color='gold')
    ax.set_xlabel('Strike Price', color='gold')
    ax.set_ylabel('Volume', color='gold')
   
    # Tick parameters for readability
    ax.tick_params(axis='x', colors='gold')
    ax.tick_params(axis='y', colors='gold')

    # Adding grid for better readability, with a lighter color
    ax.grid(True, color='dimgray')
    
    # Show legend with readable font color
    ax.legend(facecolor='darkgray', edgecolor='gold', fontsize='large')
    
    # Show the plot
    plt.show()

# Example usage:
# Load the data (make sure to parse the 'date' and 'expiry' columns correctly)
df = pd.read_csv('volume_analysis.csv', parse_dates=['date', 'expiry'])

# Filter out rows with missing values in the volume, strike, or expiry columns if necessary
# df = df.dropna(subset=['buy_volume', 'sell_volume', 'strike', 'expiry'])

# Call the function with the chosen date
plot_volume_by_strike_for_date(df, '2023-12-15', dark_background=True)