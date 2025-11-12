import os
from dotenv import load_dotenv
load_dotenv()
option_conditions_hooks = { 

    'Automatic Execution': os.environ.get('automatic'),
    'Intermarket Sweep Order': os.environ.get('intermarket_sweep'),
    'Single Leg Auction Non ISO': os.environ.get('single_auction_non_iso'),
    'Multi Leg auto-electronic trade against single leg(s)': os.environ.get('multi_auto_vs_single'),
    'Multi Leg auto-electronic trade': os.environ.get('multi_leg_auto_electronic_trade'),
    'Multi Leg Cross':os.environ.get('multi_leg_cross'),
    'Single Leg Floor Trade': os.environ.get('single_auction_non_iso'),
    'Stock Options floor trade': os.environ.get('stock_options_floor_trade'),
    'Multi Leg Floor Trade of Proprietary Products': os.environ.get('multi_floor_proprietary'),
    'Multi Leg Auction': os.environ.get('multi_leg_auction'),
    'Single Leg Cross Non ISO': os.environ.get('single_leg_cross_non_iso'),
    'Last and Canceled': os.environ.get('last_and_canceled'),
    'Late': os.environ.get('late'),
    'Stock Options Cross': os.environ.get('stock_options_cross'),
    'Stock Options auto-electronic trade': os.environ.get('stock_options_auto'),
    'Multi Leg floor trade': os.environ.get('multi_leg_floor'),
    'Multi Leg floor trade against single leg(s)':os.environ.get('multi_floor_vs_single'),
    'Single Leg Auction ISO': os.environ.get('single_leg_iso'),
    'Multi Leg auto-electronic trade': os.environ.get('multi_leg_auto_electronic_trade'),
    'Canceled': os.environ.get('canceled'),
    'Late and Out Of Sequence': os.environ.get('late_and_out_of_sequence'),
    'Opening Trade and Canceled': os.environ.get('opening_trade_and_canceled'),
    'Odd Lot Trade': os.environ.get('odd_lot_trade'),
    'Automatic Execution': os.environ.get('automatic_execution'),
    'Stock Options Auction': os.environ.get('stock_options_auction')

}