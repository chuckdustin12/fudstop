import disnake
import pandas as pd

from disnake.ext import commands
from apis.earnings_whisper.ew_sdk import EarningsWhisper
from apis.polygonio.async_polygon_sdk import Polygon

from discord_.bot_menus.pagination import AlertMenus,PageSelect




class EarningsCOG(commands.Cog):
    def __init__(self, bot):
        self.bot=bot

        self.ew = EarningsWhisper()
        self.poly = Polygon()


    @commands.slash_command()
    async def earnings(self, inter):
        pass


    



    @earnings.sub_command()
    async def sentiment(self, inter:disnake.AppCmdInter):
        """Gets upcoming earnings based on sentiment"""
        await inter.response.defer()
        df = self.ew.get_top_sentiment()

        df = df.as_dataframe
        
        # Convert 'next_er' to datetime if it's not already
        df['next_er'] = pd.to_datetime(df['next_er'])

        # Sort by 'release_time' and 'avg_sentiment'
        df_sorted = df.sort_values(by=['next_er', 'avg_sentiment'], ascending=[True, True])

        embeds = []
        
        for i,row in df_sorted.head(25).iterrows():
            logo = await self.poly.get_polygon_logo(row['ticker'])
            embed = disnake.Embed(title=f"{row['ticker']} || {row['sentiment']}", description=f"```py\n{row['company']} reports earnings at {row['next_er']}```")
            embed.add_field(name=f"Sentiment:", value=f"> **{row['avg_sentiment']}")
            embed.set_thumbnail(logo)
            embed.set_footer(text=f"Sentiment for {row['ticker']}: {row['sentiment']} | Data by EarningsWhisper")
            embeds.append(embed)


        df.to_csv('earnings_sentiment.csv', index=False)

        button = disnake.ui.Button(style=disnake.ButtonStyle.blurple, label='Download', custom_id='buttonSentiment')
        button.callback = lambda interaction: interaction.response.send_message(file=disnake.File('earnings_sesntiment.csv'))

        await inter.edit_original_message(embed=embeds[0], view=AlertMenus(embeds).add_item(EarningsSelect()).add_item(PageSelect(embeds)).add_item(button))

    @earnings.sub_command()
    async def calendar(self, inter:disnake. AppCmdInter, date:str='20231215'):
        """
        Look-up earnings up to a certain date must be in YYYYMMDD!
        """
        await inter.response.defer()
        data = self.ew.calendar(date=date)

        embeds = []
        for i, row in data.as_dataframe.iterrows():
            time_dict = {  
                3: 'afterMarket',
                1: 'preMarket',
                2: 'day'
            }
            company = row['company']
            confirmation_date = row['confirmation_date']
            earnings_time = row['earnings_time']
            next_er_date = row['next_er_date']
            q1_estimate_eps = row['q1_estimate_eps']
            q1_revenue_est = row['q1_revenue_est']
            q_date = row['q_date']
            q_sales = row['q_sales']
            quarter_date = row['quarter_date']
            release_time = row['release_time']
            ticker = row['ticker']
            total = row['total']
            logo = None
            logo = await self.poly.get_polygon_logo(row['ticker'])
            embed = disnake.Embed(title=f"{company} | {next_er_date}", description=f"```py\n{ticker} is expected to report earnings on {next_er_date} - set to report @ {time_dict.get(release_time)}```", color=disnake.Colour.dark_gold())
            embed.add_field(name=f"Revenue EST:", value=f"> **{q1_revenue_est}**")
            embed.add_field(name=f"EPS EST:", value=f"> **{q1_estimate_eps}**")
            embed.set_footer(text=f"{ticker} | ER: {next_er_date} | Data by EarningsWhisper")
            if logo is not None:
                embed.set_thumbnail(logo)
            embeds.append(embed)
        data.as_dataframe.to_csv('earnings.csv', index=False)

        button = disnake.ui.Button(label='Download', style=disnake.ButtonStyle.blurple)
        button.callback = lambda interaction: interaction.response.send_message(file=disnake.File('earnings.csv'))

        await inter.edit_original_message(embed=embeds[0], view=AlertMenus(embeds).add_item(PageSelect(embeds)).add_item(EarningsSelect()).add_item(button))



    @earnings.sub_command()
    async def pivots(self, inter:disnake.AppCmdInter):
        """
        Gets the earnings pivot list with support/resistance.
        """
        await inter.response.defer()
        data = self.ew.pivot_list()

        embeds = []
        for i,row in data.as_dataframe.head(24).iterrows():

            ticker = row['ticker']
            company = row['company']
            list_type = row['list_type']
            last_trade = row['last_trade']
            pivot_point = row['pivot_point']
            resistance_1 = row['resistance_1']
            support_1 = row['support_1']
            resistance_2 = row['resistance_2']
            support_2 = row['support_2']
            resistance_3 = row['resistance_3']
            support_3 = row['support_3']
            logo = await self.poly.get_polygon_logo(ticker)
            embed = disnake.Embed(title=f"Pivot List - {ticker}", description=f"```py\nShowing {company}'s pivot points heading into earnings. This ticker's list type is: {list_type}.```", color=disnake.Colour.dark_gold())

            embed.add_field(name='Support/Res 1', value=f"> R: **{resistance_1}**\n> S: **{support_1}**")

            embed.add_field(name=f'Support/Res 2:', value=f"> R: **{resistance_2}**\n> S: **{support_2}**")
            embed.add_field(name=f"Support/Res 3:", value=f"> R: **{resistance_3}**\n> S: **{support_3}**")
            embed.set_footer(text=f"Pivots: {ticker} | Data by EarningsWhisper")
            embed.set_thumbnail(logo)
            embeds.append(embed)

        data.as_dataframe.to_csv('pivots.csv', index=False)

        await inter.edit_original_message(embed=embeds[0], view=AlertMenus(embeds).add_item(EarningsSelect()).add_item(PageSelect(embeds)), file=disnake.File('pivots.csv'))



    @earnings.sub_command()
    async def today(self, inter:disnake.AppCmdInter):
        """
        Get earnings results for today - revenue, growth, etc.
        
        """

        await inter.response.defer()

        data = self.ew.todays_results()

        df = data.as_dataframe
        embeds = []

        for i, row in df.iterrows():
            earnings_growth = row['earnings_growth']
            earnings_surprise = row['earnings_surprise']
            eps = row['eps']
            er_date = row['er_date']
            estimate = row['estimate']
            ew_grade = row['ew_grade']
            file_name = row['file_name']
            high_estimate = row['high_estimate']
            low_estimate = row['low_estimate']
            name = row['name']
            prev_earnings_growth = row['prev_earnings_growth']
            prev_revenue_growth = row['prev_revenue_growth']
            pwrRating = row['pwrRating']
            quarter = row['quarter']
            revenue = row['revenue']
            revenue_estimate = row['revenue_estimate']
            revenue_growth = row['revenue_growth']
            revenue_surprise = row['revenue_surprise']
            subject = row['subject']
            summary = row['summary']
            ticker = row['ticker']
            whisper = row['whisper']
            logo = await self.poly.get_polygon_logo(ticker)
            embed = disnake.Embed(title=f"{ticker} | Reported: {er_date}", description=f"```py\n{name} Summary for quarter {quarter}:\n{summary}```", color=disnake.Colour.dark_gold())
            embed.add_field(name=f"Estimates:", value=f"> EST: **{estimate}**\n> Low EST: **{low_estimate}**\n> High EST: **{high_estimate}**\n Rev. EST: **{revenue_estimate}**")
            embed.add_field(name=f"Earnings Actual:", value=f"> EPA: **{eps}**\n> Earnings Growth: **{earnings_growth}**\n> Surprise: **{earnings_surprise}**")
            embed.add_field(name=f"Revenue Actual:", value=f"> Revenue: **{revenue}**\n> Growth: **{revenue_growth}**\n> Surprise: **{revenue_surprise}**")
            embed.set_thumbnail(logo)
            embed.add_field(name=f"Previous Growth:", value=f"> Earnings: **{prev_earnings_growth}**\n> Revenue: **{prev_revenue_growth}**")
            embed.add_field(name=f"Grades:", value=f"> PWR: **{pwrRating}**\n> Whisper: **{whisper}**\n> EW Grade: **{ew_grade}**")
            embed.add_field(name=f"Subject:", value=f"> **{subject}**\n> **{file_name}**")
            embed.set_footer(text=f'{ticker} | EPS: {eps} | Data by EarningsWhisper')
            
          
            embed.add_field(name=f"Surprise:", value=f"Earnings: **{earnings_surprise}**\n> Revenue: **{revenue_surprise}**")
            embeds.append(embed)

        file = 'todays_results.csv'
        df.to_csv('todays_results.csv', index=False)


    
        await inter.edit_original_message(embed=embeds[0], view=AlertMenus(embeds).add_item(EarningsSelect()).add_item(PageSelect(embeds)), file=disnake.File(file))



    @earnings.sub_command()
    async def upcoming_etfs(self, inter:disnake.AppCmdInter):
        """
        View upcoming ETFs that have tickers reporting earnings.
        
        """
        await inter.response.defer()
        data = self.ew.upcoming_sectors()

        embeds = []
        for i,row in data.as_dataframe.head(25).iterrows():
            earnings_date = row['earnings_date']
            eps_growth = row['eps_growth']
            max_date = row['max_date']
            revenue_growth = row['revenue_growth']
            sector_id = row['sector_id']
            sector = row['sector']
            sector_name = row['sector_name']
            surprise = row['surprise']
            total = row['total']
            week = row['week']

            embed = disnake.Embed(title=f"Upcoming ETF: | {sector}", description=f"```py\nViewing upcoming earnings for the {sector} sector.", color=disnake.Colour.dark_gold())
            embed.add_field(name=f"Date:", value=f"> **{earnings_date}**")
            embed.add_field(name=f"EPS Growth:", value=f"> **{eps_growth}**")
            embed.add_field(name=f"Surprise:", value=f"> **{surprise}**")
            embed.add_field(name=f"Report Week:", value=f"> **{week}**")
            embed.add_field(name=f"Max Date:", value=f"> **{max_date}**")
            embed.add_field(name=f"Revenue Growth:", value=f"> **{revenue_growth}**")
            embed.add_field(name=f"Total Reporting:", value=f"> **{total}**")

            embeds.append(embed)
            embed.set_footer(text=f'ETF Earnings: {sector} | Data by EarningsWhisper')

            embeds.append(embed)
        data.as_dataframe.to_csv('upcoming_etf_earnings.csv', index=False)
        await inter.edit_original_message(embed=embeds[0], view=AlertMenus(embeds).add_item(EarningsSelect()).add_item(PageSelect(embeds)), file=disnake.File('upcoming_etf_earnings.csv'))
cmds = EarningsCOG(commands.Bot)


class EarningsSelect(disnake.ui.Select):
    def __init__(self):
        super().__init__( 
            placeholder='Choose A Command -->',
            min_values=1,
            max_values=1,
            custom_id='earningCOMMANDselect',
            options = [ 
                disnake.SelectOption(label='Todays Results', value=f'today', description=f'View ranings results on the day.'),
                disnake.SelectOption(label=f"Calendar Date", value='cal', description=f"View earnings up to a certain date YYYYMMDD."),
                disnake.SelectOption(label='Sentiment', value='sentiment', description=f"View upcoming earnings by sentiment rank."),
                disnake.SelectOption(label='Pivot Points', value=f"pivots", description=f"Get support/resistance levels."),
                disnake.SelectOption(label='Upcoming ETFs', value=f"etfs", description=f"ETFs with tickers reporting earnings."),
                disnake.SelectOption(label='Upcoming Russell', value=f"russell", description=f"Upcoming RUSSELL earnings."),

                
            ],
            row=2
        )


    async def callback(self, inter: disnake.AppCommandInter):
        if self.values[0] == 'sentiment':
            await cmds.sentiment(inter)

        elif self.values[0] == 'cal':
            await cmds.calendar(inter, date='20231215')

        elif self.values[0] == 'pivots':
            await cmds.pivots(inter)


        elif self.values[0] == 'today':
            await cmds.today(inter)

        elif self.values[0] == 'etfs':
            await cmds.upcoming_etfs(inter)


def setup(bot: commands.Bot):
    bot.add_cog(EarningsCOG(bot))

    print(f'Earnings commands - ready.')