from .imps import *
from .models.udio_models import MySongs
class UdioSDK:
    def __init__(self):
        self.db=db

        

    async def get_client(self, endpoint):
        async with httpx.AsyncClient(headers=headers) as client:
            data = await client.get(endpoint)
            if data.status_code == 200:

                return data.json()
            

    async def post_client(self, endpoint, payload):

        async with httpx.AsyncClient(headers=headers) as client:
            data = await client.post(endpoint, json=payload)

            return data
         
    async def get_songs(self, search_term:str='', page_size:str='100'):
        """Get Udio songs
        
        Args:
        
        - page_size:

        >>> The amount of songs to return per page.
        """

        endpoint=f"https://www.udio.com/api/songs/me?likedOnly=false&publishedOnly=false&searchTerm={search_term}&pageParam=0&pageSize={page_size}"
        data = await self.get_client(endpoint)
        data = data['data'] if 'data' in data else None
        if data is not None:
                
            return MySongs(data)





    async def insert_to_db(self, df,table:str='songs',return_df:bool=False, ) -> pd.DataFrame :
        """Inserts data by batches into POSTGRES database. Requires table named 'songs'."""
        await self.db.connect()
        try:

            await self.db.batch_insert_dataframe(df, table_name=table, unique_columns='id')

        except Exception as e:
            print(f"Error: {e}")

        if return_df == True:
            return df
        


    async def create_song(self, tags:list=['rock melody', 'ballad', 'masterpiece']):

        searchtags = {"partialTag":"violin","currentTags":["piano rock","guitarist","male singer","soft ballad","progressive","spiraling progressive","slow intro","piano","harp"]}

        endpoint = f"https://www.udio.com/api/searchtags"


        data = await self.post_client(endpoint=endpoint, payload=searchtags)
        print(data)

        return data


        




