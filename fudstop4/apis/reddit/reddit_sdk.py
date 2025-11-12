import pandas as pd


import os
from dotenv import load_dotenv
load_dotenv()
import aiohttp
import requests
from .reddit_models import RedditPost

import praw
import time
import asyncio


from datetime import datetime, timedelta, timezone


class RedditSDK:
    def __init__(self,
                 client_id: str = os.environ.get('REDDIT_APP'),
                 client_secret: str = os.environ.get('REDDIT_SECRET'),
                 user_agent: str = "FUDSTOP Reddit App",
                 redirect_uri: str = "https://www.fudstop.io"):
        """
        Initialize the RedditSDK with PRAW.
        """
        self.latest_comment_id=None
        self.client_id = client_id
        self.client_secret = client_secret
        self.user_agent = user_agent
        self.redirect_uri = redirect_uri
        self.latest_post_id = None  # Initialize the latest_post_id attribute
        # Initialize the Reddit client
        self.reddit = praw.Reddit(
            client_id=self.client_id,
            client_secret=self.client_secret,
            user_agent=self.user_agent,
            redirect_uri=self.redirect_uri
        )

    def subreddit(self, name: str, limit: int = 10):
        """
        Fetch posts from the specified subreddit.
        :param name: Name of the subreddit (e.g., 'python')
        :param limit: Number of posts to fetch (default: 10)
        :return: A list of dictionaries containing post data
        """
        subreddit = self.reddit.subreddit(name)
        posts = []

        for post in subreddit.hot(limit=limit):  # Use .hot, .new, or .top
            posts.append({
                "title": post.title,
                "score": post.score,
                "url": post.url,
                "comments": post.num_comments,
                "author": str(post.author),
                "created": post.created_utc
            })

        return posts

    def search(self, subreddit_name: str, query: str, limit: int = 10):
        """
        Search for posts in a subreddit by query.
        :param subreddit_name: Name of the subreddit
        :param query: Search query string
        :param limit: Number of posts to fetch (default: 10)
        :return: A list of dictionaries containing search result data
        """
        subreddit = self.reddit.subreddit(subreddit_name)
        search_results = []

        for post in subreddit.search(query, limit=limit):
            search_results.append({
                "title": post.title,
                "score": post.score,
                "url": post.url,
                "comments": post.num_comments,
                "author": str(post.author),
                "created": post.created_utc
            })

        return search_results
    

    def fetch_posts(self, subreddit, limit=100, after=None, detailed=False):
        """
        Fetch posts from a subreddit using Pushshift API and optionally enrich with PRAW.
        
        :param subreddit: Name of the subreddit (e.g., 'options')
        :param limit: Number of posts to fetch in one request (max: 100)
        :param after: Fetch posts after this timestamp (e.g., '7d' or a UNIX timestamp)
        :param detailed: If True, enrich Pushshift data with PRAW metadata
        :return: List of posts
        """
        params = {
            "subreddit": subreddit,
            "size": limit,
            "sort": "desc",
            "sort_type": "created_utc",
        }
        if after:
            params["after"] = after

        response = requests.get(self.pushshift_url, params=params)
        data = response.json()["data"]

        posts = []
        for post in data:
            post_data = {
                "id": post.get("id"),
                "title": post.get("title"),
                "author": post.get("author"),
                "url": post.get("url"),
                "score": post.get("score"),
                "num_comments": post.get("num_comments"),
                "created_utc": post.get("created_utc"),
            }
            posts.append(post_data)

        # If detailed is True, enrich posts with PRAW
        if detailed:
            enriched_posts = []
            for post in posts:
                submission = self.reddit.submission(id=post["id"])
                enriched_posts.append({
                    "id": submission.id,
                    "title": submission.title,
                    "author": str(submission.author),
                    "url": submission.url,
                    "score": submission.score,
                    "num_comments": submission.num_comments,
                    "created_utc": submission.created_utc,
                    "selftext": submission.selftext,
                })
            return enriched_posts

        return posts
    

    def get_latest_post(self, name:str):
        """
        Get the latest post from the subreddit.
        :return: A dictionary containing the latest post's details.
        """
        subreddit = self.reddit.subreddit(name)
        for post in subreddit.new(limit=1):
            return {
                "id": post.id,
                "title": post.title,
                "author": str(post.author),
                "url": post.url,
                "created": post.created_utc,
            }
    def get_new_posts(self, name):
        """
        Get new posts that were published after the latest tracked post.
        :param name: Name of the subreddit to monitor.
        :return: A list of RedditPost objects.
        """
        subreddit = self.reddit.subreddit(name)
        new_posts = []

        for post in subreddit.new(limit=10):  # Limit to the last 10 posts
            if post.id == self.latest_post_id:  # Skip posts already tracked
                continue
            new_posts.append(RedditPost(post))  # Append the RedditPost object

        if new_posts:
            return new_posts
        else:
            print("No new posts found.")
            return []
    def get_new_comments(self, post_id):
        """
        Get new comments on a specific post.
        :param post_id: The ID of the Reddit post to monitor.
        :return: A list of new comments.
        """
        post = self.reddit.submission(id=post_id)
        post.comments.replace_more(limit=0)  # Expand all "More comments"
        new_comments = []

        for comment in post.comments.list():  # Iterate over all comments
            if self.latest_comment_id and comment.id == self.latest_comment_id:
                break  # Stop processing comments once we reach the last tracked comment
            new_comments.append({
                "id": comment.id,
                "body": comment.body,
                "author": str(comment.author),
                "created_utc": comment.created_utc,
                "score": comment.score,
                "parent_id": comment.parent_id,
                "link_id": comment.link_id,
            })

        # If there are new comments, update latest_comment_id
        if new_comments:
            self.latest_comment_id = new_comments[0]["id"]  # Set to the most recent comment ID
            return new_comments
    def monitor(self, name:str, interval:int=60):
        """
        Monitor the subreddit for new posts and print them every interval (default: 5 minutes).
        """
        # Output the latest post on startup
        print("Fetching the latest post on startup...")
        latest_post = self.get_latest_post(name)
        if latest_post:
            self.latest_post_id = latest_post["id"]
            print(f"Latest Post: {latest_post['title']} (ID: {latest_post['id']})")
            print(f"URL: {latest_post['url']}")
            print("-" * 50)

        # Start monitoring for new posts
        while True:
            print("Checking for new posts...")
            new_posts = self.get_new_posts(name=name)

            if new_posts:
                for post in reversed(new_posts):  # Print in chronological order
                    print(post)

                # Update the latest_post_id to the most recent post's ID
                self.latest_post_id = new_posts[0].id
            else:
                print("No new posts found.")

            # Wait for the specified interval
            time.sleep(interval)