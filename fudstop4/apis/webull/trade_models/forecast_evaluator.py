import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import aiohttp
class ForecastEvaluator:
    def __init__(self, tickerId, symbol=None):
        self.tickerId = tickerId
        self.mape_values = {}
        self.symbol = symbol

    def calculate_mape(self, actual, forecast):
        actual, forecast = np.array(actual), np.array(forecast)
        mape = np.mean(np.abs((actual - forecast) / actual)) * 100
        return mape

    def plot_metric(self, metric_data, metric_name, report_type):
        dates = [point['xAxis'] for point in metric_data]
        actual_values = [point.get('valueActual', None) for point in metric_data]
        forecast_values = [point.get('valueForecast', None) for point in metric_data]

        fig, ax = plt.subplots()
        ax.plot(dates, actual_values, label='Actual')
        ax.plot(dates, forecast_values, label='Forecast')
        ax.set_xlabel('Date', color='white')
        ax.set_ylabel(metric_name, color='white')
        ax.set_title(f'{metric_name} - Actual vs Forecast {self.symbol}', color='white')
        ax.legend()
        
        ax.spines['bottom'].set_color('white')
        ax.spines['top'].set_color('white')
        ax.spines['left'].set_color('white')
        ax.spines['right'].set_color('white')
        ax.xaxis.label.set_color('white')
        ax.yaxis.label.set_color('white')
        ax.tick_params(axis='x', colors='white')
        ax.tick_params(axis='y', colors='white')

        ax.grid(True, linestyle='--', linewidth=0.5, alpha=0.7)
        plt.xticks(rotation=45)

        # Set background color
        fig.patch.set_facecolor('black')
        ax.set_facecolor('black')
        plt.savefig(f"{metric_name}_{report_type}.png", dpi=300, facecolor='black')
        plt.close()

    async def fetch_data(self, session, url):
        async with session.get(url) as response:
            return await response.json()

    async def get_metric_data(self, statement_type, report_type):
        base_url = f"https://quotes-gw.webullfintech.com/api/information/financial/forecast?tickerId={self.tickerId}&statementType={statement_type}&reportType={report_type}"
        async with aiohttp.ClientSession() as session:
            data = await self.fetch_data(session, base_url)
            return data

    async def evaluate(self):
        report_types = {
            1: 'EPS, EBIT, Net Income, & Revenue',
            2: 'BVPS, Net Asset Value, & Net Debt',
            3: 'Capital Expenditure & CFPS',
            4: 'ROE, ROA',
        }

        for statement_type in report_types.keys():
            for report_type in ['annual', 'quarterly']:
                data = await self.get_metric_data(statement_type, report_type)
                datas = [i['datas'] for i in data]

                for report in reversed(datas):
                    for data in report:
                        id = data['id']
                        title = data['title']
                        points = data['points']
                        actual_forecast_pairs = [(float(point.get('valueActual')), float(point.get('valueForecast'))) for point in points if point.get('valueActual') not in (None, 'N/A') and point.get('valueForecast') not in (None, 'N/A')]
                        if len(actual_forecast_pairs) < 1:
                            continue
                        actual, forecast = zip(*actual_forecast_pairs)
                        actual, forecast = np.array(actual), np.array(forecast)

                        mape = self.calculate_mape(actual, forecast)
                        self.mape_values[title] = mape

                        print(f"ID: {id} TITLE: {title} {report_type.capitalize()} MAPE: {mape}")

                        self.plot_metric(points, title, report_type)





  