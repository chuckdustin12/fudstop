import os
from dotenv import load_dotenv
load_dotenv()

key = os.environ.get('YOUTUBE_DATA_API_KEY')
import google_auth_oauthlib.flow
import googleapiclient.discovery
import googleapiclient.errors
import asyncio
import aiohttp
import requests
scopes = ["https://www.googleapis.com/auth/youtube.readonly"]
api_service_name = "youtube"
api_version = "v3"


client_secrets_file = r"C:\Users\chuck\markets\fudstop\fudstop\apis\youtube\client_secret.json"  # Absolute path to the client_secret.json file

# YouTube API setup
scopes = ["https://www.googleapis.com/auth/youtube.readonly"]
client_secrets_file = r"C:\Users\chuck\markets\fudstop\fudstop\apis\youtube\client_secret.json"  # Path to your OAuth client_secret.json file
class YoutubeDataAPI:
    def __init__(self):
        self.api_key = key

    # Asynchronously fetch data from the YouTube Data API
    async def fetch(self, session, url, params):
        async with session.get(url, params=params) as response:
            return await response.json()

    # Get all uploaded videos with pagination (async version)
    async def get_all_uploaded_videos(self, session, uploads_playlist_id):
        all_videos = []
        url = "https://www.googleapis.com/youtube/v3/playlistItems"

        # Start with the first page of results
        params = {
            "part": "snippet",
            "playlistId": uploads_playlist_id,
            "maxResults": 50,  # Max number of results per request
            "key": self.api_key
        }

        while True:
            data = await self.fetch(session, url, params)

            # Add the videos from this page to the list
            all_videos.extend(data["items"])

            # Check if there is another page
            if "nextPageToken" in data:
                params["pageToken"] = data["nextPageToken"]  # Get the next page token
            else:
                break  # No more pages, exit the loop

        return all_videos

    # Asynchronously get video details (including tags, titles, descriptions)
    async def get_video_details(self, session, video_id):
        url = "https://www.googleapis.com/youtube/v3/videos"
        params = {
            "part": "snippet",
            "id": video_id,
            "key": self.api_key
        }
        video_details = await self.fetch(session, url, params)
        return video_details

    # Asynchronously get the channel's uploads playlist ID
    async def get_channel_uploads_playlist(self, session, channel_id):
        url = "https://www.googleapis.com/youtube/v3/channels"
        params = {
            "part": "contentDetails",
            "id": channel_id,  # Your YouTube channel ID
            "key": self.api_key
        }
        data = await self.fetch(session, url, params)

        # Extract the uploads playlist ID
        uploads_playlist_id = data["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]
        return uploads_playlist_id

    # Main function to fetch and print video details concurrently
    async def main(self, channel_id):
        async with aiohttp.ClientSession() as session:
            # Get the uploads playlist ID for the channel
            uploads_playlist_id = await self.get_channel_uploads_playlist(session, channel_id)

            # Fetch all uploaded videos (handling pagination)
            all_videos = await self.get_all_uploaded_videos(session, uploads_playlist_id)

            # List to store all video details
            video_info = []

            # Create tasks to fetch video details concurrently
            tasks = []
            for item in all_videos:
                video_id = item["snippet"]["resourceId"]["videoId"]
                tasks.append(self.get_video_details(session, video_id))

            # Run all tasks concurrently
            video_details_list = await asyncio.gather(*tasks)

            # Process the results
            for video_details in video_details_list:
                # Extracting title, description, and tags
                title = video_details["items"][0]["snippet"]["title"]
                description = video_details["items"][0]["snippet"]["description"]
                tags = video_details["items"][0]["snippet"].get("tags", [])

                # Append video information to the list
                video_info.append({
                    "Title": title,
                    "Description": description,
                    "Tags": tags
                })

            # Return all video information in JSON format
            return video_info