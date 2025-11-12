import os


from dotenv import load_dotenv
load_dotenv()
import psycopg2



class DiscordDBManager:
    def __init__(self):
            


        self.channel_types = { 

            'category': '4',
            
            
        }

        self.member_permissions = '71453223935041'

        self.role_dict = {
            'LIFETIME MEMBER': 1002249878283493456,
            'youtube support': 1086118401660964914,
            'youtube level1': 1086118401660964916,
            'youtube level2': 1086118401660964917,
            'patreon level1': 896207245853999145,
            'patreon level2': 941029523699400705,
            'patreon level3': 938824920589283348,
            'patreon last': 1145204112481321040

        }
       
        self.fudstop_id = 888488311927242753 # replace with your guild ID
        self.test_id=1193717419328409600
        self.token = os.environ.get('DISCORD_AUTHORIZATION')
        print(self.token)
        self.headers = {
                "authority": "discord.com",
                "method": "POST",
                "path": "/api/v9/science",
                "scheme": "https",
                "Accept": "*/*",
                'Content-Type': 'application/json',
                "Accept-Encoding": "gzip, deflate, br",
                "Accept-Language": "en-US,en;q=0.9",
                "Authorization": self.token,
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",

        }

    def create_thread_table(self, connection):
        """
        Creates a PostgreSQL database table to store thread-related information.

        This function defines the structure of the "thread" table with columns
        for thread attributes such as thread_id, parent_channel, name, total_messages,
        message_count, last_message_id, and guild.

        The table is created if it doesn't already exist, and a unique constraint
        is added on the "thread_id" column. This ensures that if the same thread_id
        is inserted again, it will update the existing row with the new values.

        :return: None
        """
        try:

            if connection:
                cursor = connection.cursor()

                # Define the SQL CREATE TABLE statement for the thread table
                create_table_query = """
                    CREATE TABLE IF NOT EXISTS threads (
                        id SERIAL PRIMARY KEY,
                        thread_id BIGINT UNIQUE,
                        parent_channel BIGINT,
                        name TEXT,
                        total_messages INT,
                        message_count INT,
                        last_message_id BIGINT,
                        guild BIGINT
                    );
                """

                # Execute the CREATE TABLE statement
                cursor.execute(create_table_query)
                connection.commit()
                cursor.close()
                print("Thread table created successfully")
        except (Exception, psycopg2.Error) as error:
            print("Error creating the thread table:", error)
        finally:
            if connection:
                connection.close()

    def insert_thread(self, thread_id, parent_channel, name, total_messages, message_count, last_message_id, guild):
        try:
            connection = self.connect_to_database()
            if connection:
                cursor = connection.cursor()
                # Define the SQL INSERT statement
                insert_query = """
                    INSERT INTO threads (thread_id, parent_channel, name, total_messages, message_count, last_message_id, guild)
                    VALUES (%s, %s, %s, %s, %s, %s, %s);
                """
                # Execute the INSERT statement with the retrieved attributes
                cursor.execute(insert_query, (thread_id, parent_channel, name, total_messages, message_count, last_message_id, guild))
                connection.commit()
                cursor.close()
                print("Insertion successful")
        except (Exception, psycopg2.Error) as error:
            print("Error inserting data into the database:", error)
        finally:
            if connection:
                connection.close()
    