import requests
from asyncpg import create_pool
import os
import xml.etree.ElementTree as ET
from xml.etree.ElementTree import ElementTree, Element
from .cfr_helpers import clean_string
from typing import List, Dict, Any

import os
connection_string = os.environ.get('CFR_STRING')

# Adding methods to create tables in the DatabaseManager class
class CFRManager:
    def __init__(self, host:str=os.environ.get('DB_HOST'), port:str=os.environ.get('DB_PORT'), user:str=os.environ.get('DB_USER'), password:str=os.environ.get('DB_PASSWORD'), database:str=os.environ.get('DB_NAME')):
        self.conn = None
        self.pool = None
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.connection_string = connection_string
        
        self.chat_memory = []  # In-memory list to store chat messages

    async def connect(self, connection_string=None):
        if connection_string:
            self.pool = await create_pool(
                dsn=connection_string, min_size=1, max_size=10
            )
        else:
            self.pool = await create_pool(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                database=self.database,
                min_size=1,
                max_size=10
            )
        return self.pool

    async def create_title_table(self):
        async with self.pool.acquire() as conn:
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS cfr_title (
                    id SERIAL PRIMARY KEY,
                    title_name TEXT NOT NULL
                );
            ''')

    async def create_chapter_table(self):
        async with self.pool.acquire() as conn:
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS cfr_chapter (
                    id SERIAL PRIMARY KEY,
                    chapter_name TEXT NOT NULL,
                    title_id INTEGER REFERENCES cfr_title(id)
                );
            ''')

    async def create_part_table(self):
        async with self.pool.acquire() as conn:
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS cfr_part (
                    id SERIAL PRIMARY KEY,
                    part_name TEXT NOT NULL,
                    chapter_id INTEGER REFERENCES cfr_chapter(id)
                );
            ''')

    async def create_authority_table(self):
        async with self.pool.acquire() as conn:
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS cfr_authority (
                    id SERIAL PRIMARY KEY,
                    authority_text TEXT NOT NULL,
                    part_id INTEGER REFERENCES cfr_part(id)
                );
            ''')

    async def create_source_table(self):
        async with self.pool.acquire() as conn:
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS cfr_source (
                    id SERIAL PRIMARY KEY,
                    source_text TEXT NOT NULL,
                    part_id INTEGER REFERENCES cfr_part(id)
                );
            ''')
    async def batch_insert(self, table_name: str, records: List[Dict[str, Any]]):
        if not records:
            return

        # Extract column names from the keys of the first record
        columns = records[0].keys()
        placeholders = ", ".join([f"${i+1}" for i in range(len(columns))])
        
        query = f"""
            INSERT INTO {table_name} ({', '.join(columns)})
            VALUES ({placeholders})
            ON CONFLICT DO NOTHING;
        """
        
        async with self.pool.acquire() as conn:
            # Create a list of values to be inserted
            values = [[record[col] for col in columns] for record in records]

            # Perform batch insertion
            await conn.executemany(query, values)


    def parse_title_12_xml(self, file_path: str) -> Dict[str, List[Dict]]:
        # Initialize data structures to hold parsed data
        titles = []
        chapters = []
        parts = []
        authorities = []
        sources = []
        
        # Parse the XML file
        tree = ElementTree()
        tree.parse(file_path)
        
        title_id = 1  # Initialize title_id
        chapter_id = 1  # Initialize chapter_id
        part_id = 1  # Initialize part_id
        
        for div1 in tree.findall(".//DIV1"):
            title_name = div1.find(".//HEAD").text if div1.find(".//HEAD") is not None else ""
            title_name = clean_string(title_name)
            titles.append({"id": title_id, "title_name": title_name})
            
            for div3 in div1.findall(".//DIV3"):
                chapter_name = div3.find(".//HEAD").text if div3.find(".//HEAD") is not None else ""
                chapter_name = clean_string(chapter_name)  # Clean the string here
                chapters.append({"id": chapter_id, "chapter_name": chapter_name, "title_id": title_id})
                
                
                for div5 in div3.findall(".//DIV5"):
                    part_name = div5.find(".//HEAD").text if div5.find(".//HEAD") is not None else ""
                    part_name = clean_string(part_name)
                    parts.append({"id": part_id, "part_name": part_name, "chapter_id": chapter_id})
                    
                    authority = div5.find(".//AUTH")
                    if authority is not None:
                        authority_text = "".join(authority.itertext())
                        authority_text = clean_string(authority_text)
                        authorities.append({"id": len(authorities) + 1, "authority_text": authority_text, "part_id": part_id})
                    
                    source = div5.find(".//SOURCE")
                    if source is not None:
                        source_text = "".join(source.itertext())
                        source_text = clean_string(source_text)
                        sources.append({"id": len(sources) + 1, "source_text": source_text, "part_id": part_id})
                    
                    part_id += 1  # Increment part_id for the next part
                chapter_id += 1  # Increment chapter_id for the next chapter
            title_id += 1  # Increment title_id for the next title
    
        return {
            "titles": titles,
            "chapters": chapters,
            "parts": parts,
            "authorities": authorities,
            "sources": sources
        }

            
    # Add the batch_insert method to the DatabaseManager class
    async def insert_parsed_data(self, parsed_data: Dict[str, List[Dict]]):
        # Insert titles
        await self.batch_insert("cfr_title", parsed_data['titles'])

        # Insert chapters
        await self.batch_insert("cfr_chapter", parsed_data['chapters'])

        # Insert parts
        await self.batch_insert("cfr_part", parsed_data['parts'])

        # Insert authorities
        await self.batch_insert("cfr_authority", parsed_data['authorities'])

        # Insert sources
        await self.batch_insert("cfr_source", parsed_data['sources'])


    def get_xml_document(self, url, filename):
        r = requests.get(url)
        # Check for a valid response
        if r.status_code == 200:
            # Write the content to a file
            with open(filename, "wb") as file:
                file.write(r.content)
        else:
            print(f"Failed to retrieve the file: {r.status_code}")


