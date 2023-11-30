import requests


headers = { 

    'Authorization': 'Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJ1c2VyX2lkIjoiUHVzaHNoaWZ0LVN1cHBvcnQiLCJleHBpcmVzIjoxNjg1MTA4Nzg4LjE5NzY3OTh9.hATtBHzQh5hiFBSFg3gQsFK2xrwIlPynYrL7l6pPCMw'
}


data = requests.get("https://api.pushshift.io/reddit/search/comment?q=drs&subreddit=superstonk&sort=created_utc&order=desc&agg_size=25&shard_size=1.5&limit=10&track_total_hits=false", headers=headers).json()

print(data)
