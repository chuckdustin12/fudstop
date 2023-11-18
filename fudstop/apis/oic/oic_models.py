import pandas as pd

class OICOptionsMonitor:
    def __init__(self, data):
        # Initialize attributes from the 'data' dictionary
        self.call_change_eod = [i.get('call_change_eod', 'N/A') for i in data]
        self.call_ivbid = [i.get('call_ivbid', 'N/A') for i in data]
        self.call_iv_eod = [i.get('call_iv_eod', 'N/A') for i in data]
        self.put_theta_eod = [i.get('put_theta_eod', 'N/A') for i in data]
        self.call_ivask = [i.get('call_ivask', 'N/A') for i in data]
        self.call_days = [i.get('call_days', 'N/A') for i in data]
        self.call_mean_eod = [i.get('call_mean_eod', 'N/A') for i in data]
        self.call_ivint = [i.get('call_ivint', 'N/A') for i in data]
        self.put_asksize = [i.get('put_asksize', 'N/A') for i in data]
        self.call_delta_eod = [i.get('call_delta_eod', 'N/A') for i in data]
        self.call_bid_eod = [i.get('call_bid_eod', 'N/A') for i in data]
        self.call_theoprice_eod = [i.get('call_theoprice_eod', 'N/A') for i in data]
        self.put_iv = [i.get('put_iv', 'N/A') for i in data]
        self.call_ivint_eod = [i.get('call_ivint_eod', 'N/A') for i in data]
        self.call_ask_eod = [i.get('call_ask_eod', 'N/A') for i in data]
        self.call_iv = [i.get('call_iv', 'N/A') for i in data]
        self.put_days = [i.get('put_days', 'N/A') for i in data]
        self.put_iv_eod = [i.get('put_iv_eod', 'N/A') for i in data]
        self.call_volume_eod = [i.get('call_volume_eod', 'N/A') for i in data]
        self.put_change_eod = [i.get('put_change_eod', 'N/A') for i in data]
        self.call_ask = [i.get('call_ask', 'N/A') for i in data]
        self.call_bidtime = [i.get('call_bidtime', 'N/A') for i in data]
        self.call_rho = [i.get('call_rho', 'N/A') for i in data]
        self.call_forwardprice_eod = [i.get('call_forwardprice_eod', 'N/A') for i in data]
        self.call_mean = [i.get('call_mean', 'N/A') for i in data]
        self.put_bid_eod = [i.get('put_bid_eod', 'N/A') for i in data]
        self.call_bid = [i.get('call_bid', 'N/A') for i in data]
        self.call_volume = [i.get('call_volume', 'N/A') for i in data]
        self.call_alpha = [i.get('call_alpha', 'N/A') for i in data]
        self.call_vega = [i.get('call_vega', 'N/A') for i in data]
        self.put_bidtime = [i.get('put_bidtime', 'N/A') for i in data]
        self.put_theta = [i.get('put_theta', 'N/A') for i in data]
        self.put_optionsymbol = [i.get('put_optionsymbol', 'N/A') for i in data]
        self.put_ivask = [i.get('put_ivask', 'N/A') for i in data]
        self.put_changepercent_eod = [i.get('put_changepercent_eod', 'N/A') for i in data]
        self.put_ask = [i.get('put_ask', 'N/A') for i in data]
        self.put_rho = [i.get('put_rho', 'N/A') for i in data]
        self.call_openinterest_eod = [i.get('call_openinterest_eod', 'N/A') for i in data]
        self.put_ivint = [i.get('put_ivint', 'N/A') for i in data]
        self.put_theoprice = [i.get('put_theoprice', 'N/A') for i in data]
        self.call_asktime = [i.get('call_asktime', 'N/A') for i in data]
        self.put_bid = [i.get('put_bid', 'N/A') for i in data]
        self.call_gamma_eod = [i.get('call_gamma_eod', 'N/A') for i in data]
        self.put_ask_eod = [i.get('put_ask_eod', 'N/A') for i in data]
        self.call_optionsymbol = [i.get('call_optionsymbol', 'N/A') for i in data]
        self.put_paramvolapercent_eod = [i.get('put_paramvolapercent_eod', 'N/A') for i in data]
        self.call_asksize = [i.get('call_asksize', 'N/A') for i in data]
        self.put_volume = [i.get('put_volume', 'N/A') for i in data]
        self.call_alpha_eod = [i.get('call_alpha_eod', 'N/A') for i in data]
        self.put_volume_eod = [i.get('put_volume_eod', 'N/A') for i in data]
        self.put_ivbid = [i.get('put_ivbid', 'N/A') for i in data]
        self.call_pos = [i.get('call_pos', 'N/A') for i in data]
        self.put_delta_eod = [i.get('put_delta_eod', 'N/A') for i in data]
        self.put_changepercent = [i.get('put_changepercent', 'N/A') for i in data]
        self.put_mean_eod = [i.get('put_mean_eod', 'N/A') for i in data]
        self.call_changepercent = [i.get('call_changepercent', 'N/A') for i in data]
        self.put_asktime = [i.get('put_asktime', 'N/A') for i in data]
        self.put_pos = [i.get('put_pos', 'N/A') for i in data]
        self.put_theoprice_eod = [i.get('put_theoprice_eod', 'N/A') for i in data]
        self.put_gamma = [i.get('put_gamma', 'N/A') for i in data]
        self.call_days_eod = [i.get('call_days_eod', 'N/A') for i in data]
        self.call_bidsize = [i.get('call_bidsize', 'N/A') for i in data]
        self.call_delta = [i.get('call_delta', 'N/A') for i in data]
        self.put_change = [i.get('put_change', 'N/A') for i in data]
        self.call_paramvolapercent_eod = [i.get('call_paramvolapercent_eod', 'N/A') for i in data]
        self.call_theta_eod = [i.get('call_theta_eod', 'N/A') for i in data]
        self.call_change = [i.get('call_change', 'N/A') for i in data]
        self.put_ivint_eod = [i.get('put_ivint_eod', 'N/A') for i in data]
        self.call_theta = [i.get('call_theta', 'N/A') for i in data]
        self.put_vega = [i.get('put_vega', 'N/A') for i in data]
        self.put_days_eod = [i.get('put_days_eod', 'N/A') for i in data]
        self.put_forwardprice = [i.get('put_forwardprice', 'N/A') for i in data]
        self.call_rho_eod = [i.get('call_rho_eod', 'N/A') for i in data]
        self.quotetime = [i.get('quotetime', 'N/A') for i in data]
        self.put_vega_eod = [i.get('put_vega_eod', 'N/A') for i in data]
        self.strike = [i.get('strike', 'N/A') for i in data]
        self.put_mean = [i.get('put_mean', 'N/A') for i in data]
        self.put_forwardprice_eod = [i.get('put_forwardprice_eod', 'N/A') for i in data]
        self.expirationdate = [i.get('expirationdate', 'N/A') for i in data]
        self.call_forwardprice = [i.get('call_forwardprice', 'N/A') for i in data]
        self.call_gamma = [i.get('call_gamma', 'N/A') for i in data]
        self.put_alpha_eod = [i.get('put_alpha_eod', 'N/A') for i in data]
        self.put_delta = [i.get('put_delta', 'N/A') for i in data]
        self.put_openinterest_eod = [i.get('put_openinterest_eod', 'N/A') for i in data]
        self.call_changepercent_eod = [i.get('call_changepercent_eod', 'N/A') for i in data]
        self.put_gamma_eod = [i.get('put_gamma_eod', 'N/A') for i in data]
        self.put_bidsize = [i.get('put_bidsize', 'N/A') for i in data]
        self.call_vega_eod = [i.get('call_vega_eod', 'N/A') for i in data]
        self.put_rho_eod = [i.get('put_rho_eod', 'N/A') for i in data]
        self.put_alpha = [i.get('put_alpha', 'N/A') for i in data]
        self.call_theoprice = [i.get('call_theoprice', 'N/A') for i in data]

        # Create a data dictionary for the class
        self.data_dict = {
            'call_change_eod': self.call_change_eod,
            'call_ivbid': self.call_ivbid,
            'call_iv_eod': self.call_iv_eod,
            'put_theta_eod': self.put_theta_eod,
            'call_ivask': self.call_ivask,
            'call_days': self.call_days,
            'call_mean_eod': self.call_mean_eod,
            'call_ivint': self.call_ivint,
            'put_asksize': self.put_asksize,
            'call_delta_eod': self.call_delta_eod,
            'call_bid_eod': self.call_bid_eod,
            'call_theoprice_eod': self.call_theoprice_eod,
            'put_iv': self.put_iv,
            'call_ivint_eod': self.call_ivint_eod,
            'call_ask_eod': self.call_ask_eod,
            'call_iv': self.call_iv,
            'put_days': self.put_days,
            'put_iv_eod': self.put_iv_eod,
            'call_volume_eod': self.call_volume_eod,
            'put_change_eod': self.put_change_eod,
            'call_ask': self.call_ask,
            'call_bidtime': self.call_bidtime,
            'call_rho': self.call_rho,
            'call_forwardprice_eod': self.call_forwardprice_eod,
            'call_mean': self.call_mean,
            'put_bid_eod': self.put_bid_eod,
            'call_bid': self.call_bid,
            'call_volume': self.call_volume,
            'call_alpha': self.call_alpha,
            'call_vega': self.call_vega,
            'put_bidtime': self.put_bidtime,
            'put_theta': self.put_theta,
            'put_optionsymbol': self.put_optionsymbol,
            'put_ivask': self.put_ivask,
            'put_changepercent_eod': self.put_changepercent_eod,
            'put_ask': self.put_ask,
            'put_rho': self.put_rho,
            'call_openinterest_eod': self.call_openinterest_eod,
            'put_ivint': self.put_ivint,
            'put_theoprice': self.put_theoprice,
            'call_asktime': self.call_asktime,
            'put_bid': self.put_bid,
            'call_gamma_eod': self.call_gamma_eod,
            'put_ask_eod': self.put_ask_eod,
            'call_optionsymbol': self.call_optionsymbol,
            'put_paramvolapercent_eod': self.put_paramvolapercent_eod,
            'call_asksize': self.call_asksize,
            'put_volume': self.put_volume,
            'call_alpha_eod': self.call_alpha_eod,
            'put_volume_eod': self.put_volume_eod,
            'put_ivbid': self.put_ivbid,
            'call_pos': self.call_pos,
            'put_delta_eod': self.put_delta_eod,
            'put_changepercent': self.put_changepercent,
            'put_mean_eod': self.put_mean_eod,
            'call_changepercent': self.call_changepercent,
            'put_asktime': self.put_asktime,
            'put_pos': self.put_pos,
            'put_theoprice_eod': self.put_theoprice_eod,
            'put_gamma': self.put_gamma,
            'call_days_eod': self.call_days_eod,
            'call_bidsize': self.call_bidsize,
            'call_delta': self.call_delta,
            'put_change': self.put_change,
            'call_paramvolapercent_eod': self.call_paramvolapercent_eod,
            'call_theta_eod': self.call_theta_eod,
            'call_change': self.call_change,
            'put_ivint_eod': self.put_ivint_eod,
            'call_theta': self.call_theta,
            'put_vega': self.put_vega,
            'put_days_eod': self.put_days_eod,
            'put_forwardprice': self.put_forwardprice,
            'call_rho_eod': self.call_rho_eod,
            'quotetime': self.quotetime,
            'put_vega_eod': self.put_vega_eod,
            'strike': self.strike,
            'put_mean': self.put_mean,
            'put_forwardprice_eod': self.put_forwardprice_eod,
            'expirationdate': self.expirationdate,
            'call_forwardprice': self.call_forwardprice,
            'call_gamma': self.call_gamma,
            'put_alpha_eod': self.put_alpha_eod,
            'put_delta': self.put_delta,
            'put_openinterest_eod': self.put_openinterest_eod,
            'call_changepercent_eod': self.call_changepercent_eod,
            'put_gamma_eod': self.put_gamma_eod,
            'put_bidsize': self.put_bidsize,
            'call_vega_eod': self.call_vega_eod,
            'put_rho_eod': self.put_rho_eod,
            'put_alpha': self.put_alpha,
            'call_theoprice': self.call_theoprice,
        }


        self.as_dataframe = pd.DataFrame(self.data_dict)