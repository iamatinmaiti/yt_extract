from project.tasks import create_trending_snapshot


# Sample data from the user
sample_video = {
    "title": "Who did it best? ❤️ #Shorts #Dance #Trending",
    "channel": "Brat TV",
    "views": "12M views",
    "upload_time": "7 months ago",
    "video_url": "/shorts/2sokLXhqRFQ",
    "channel_url": "/@brat",
    "thumbnail_url": "https://i.ytimg.com/vi/2sokLXhqRFQ/hq720.jpg?sqp=-oaymwFBCNAFEJQDSFryq4qpAzMIARUAAIhCGADYAQHiAQoIGBACGAY4AUAB8AEB-AG2CIACgA-KAgwIABABGFUgYihlMA8=&rs=AOn4CLAQQc0pC19cJM_jxzBzFp_sGeiMxw",
    "duration": ""
}

if __name__ == "__main__":
    create_trending_snapshot()