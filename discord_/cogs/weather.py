import os
import requests
import disnake
from disnake.ext import commands
from apis.misc.weather import Weather as W
from apis.misc.unsplash import Unsplash
un = Unsplash()
w = W()

class Weather(commands.Cog):
    """
    Weather commands for discord.
    
    """

    def __init__(self, bot):
        self.bot=bot
        self.key = os.environ.get('YOUR_WEATHER_KEY')



    @commands.slash_command()
    async def weather(self, inter):
        pass




    @weather.sub_command()
    async def location(self, inter:disnake.AppCmdInter, city:str, state:str, country:str, unit:str='F'):
        """
        >>> Enter a city, state, and country code - recieve the weather info!
        
        """
        await inter.response.defer()
        query_string = city + " " + state + " " + country
        weather_data = await w.get_weather(city,state,country,limit='1')
        image_data = await un.get_image(query_string)
        image_data2 = await un.get_image(query=f"{country}_flag")


        embed = disnake.Embed(title=f"Weather | {city} | {state} | {country}", description=f'> 🌡️ Temperature: **{round(float(weather_data.temp),0)}**\n> Feels Like: **{round(float(weather_data.feels_like),0)}**\n\n> 💧 Humidity: **{weather_data.humidity}**\n> 💦 Dewpoint: **{weather_data.dew_point}**')
        embed.add_field(name=f"☁️ Clouds:", value=f"> **{weather_data.clouds}**\n> UVI: **{weather_data.uvi}**\n> Visibility: **{weather_data.visibility}**")
        embed.add_field(name=f"🌬️ Wind:", value=f"> Speed: **{weather_data.wind_speed}**\n> Degree: **{weather_data.wind_degree}**")
   
        embed.add_field(name=f"Pressure:", value=f"> **{weather_data.pressure}**")
        embed.add_field(name=f"Time:", value=f"> Current: **{weather_data.current_time}**\n> Timezone: **{weather_data.timezone}**", inline=False)
        embed.add_field(name=f"Sun:", value=f"> 🌄 Sunrise: **{weather_data.sunrise}**\n> 🌅 Sunset: **{weather_data.sunset}**", inline=False)
        embed.add_field(name=f"Summary:", value=f"> **{weather_data.weather_summary[0]}**\n> **{weather_data.weather_desc[0]}**")
        embed.set_image(url=image_data.small)
        embed.set_thumbnail(url=image_data2.thumbnail)
        embed.set_footer(text=f'longitude: {weather_data.lon} | latitude: {weather_data.lat} | {country.upper()}, {state.upper()}')

        await inter.edit_original_message(embed=embed)

    @weather.sub_command()
    async def get_coordinates(self, inter:disnake.AppCmdInter, city:str, state:str, country:str):
        """Get coordinates for a location by typing city, state , and country.
        
        
        """

        lat, lon = await w.get_coordinates(city, state, country)

        await inter.response.defer()


        await inter.edit_original_message(f"> The coordinates for **{city}**, **{state}**, **{country}** are:\n\n> **{lat}, {lon}**")
           
    

def setup(bot: commands.Bot):
    bot.add_cog(Weather(bot))

    print(f"Weather commands - ready!")