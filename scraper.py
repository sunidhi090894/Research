import os
import pandas as pd
from googleapiclient.discovery import build
from youtube_transcript_api import YouTubeTranscriptApi
import time

# =============================
# CONFIGURATION
# =============================
YOUTUBE_API_KEY = "AIzaSyANTHOonjY6lks5i7qJBv0hycZkv7uqIo8"  # set in terminal: export YOUTUBE_API_KEY="your_key"
YOUTUBE_API_SERVICE_NAME = "youtube"
YOUTUBE_API_VERSION = "v3"

youtube = build(YOUTUBE_API_SERVICE_NAME, YOUTUBE_API_VERSION, developerKey=YOUTUBE_API_KEY)

# =============================
# FETCH VIDEOS WITH PAGINATION
# =============================
def search_videos(query, max_videos=10000):
    videos = []
    next_page_token = None

    while len(videos) < max_videos:
        request = youtube.search().list(
            q=query,
            part="snippet",
            type="video",
            maxResults=50,  # YouTube API max
            pageToken=next_page_token
        )
        response = request.execute()

        for item in response.get("items", []):
            videos.append(item["id"]["videoId"])
            if len(videos) >= max_videos:
                break

        next_page_token = response.get("nextPageToken")
        if not next_page_token:
            break  # No more results

        time.sleep(0.1)  # Avoid hitting quota too fast

    return videos


# =============================
# GET VIDEO DETAILS
# =============================
def get_video_details(video_ids):
    all_data = []

    for i in range(0, len(video_ids), 50):  # API allows max 50 IDs at once
        batch_ids = video_ids[i:i+50]
        request = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=",".join(batch_ids)
        )
        response = request.execute()

        for item in response.get("items", []):
            vid_id = item["id"]
            snippet = item["snippet"]
            stats = item.get("statistics", {})
            details = item["contentDetails"]

            # Get captions/transcript
            try:
                transcript_list = YouTubeTranscriptApi.get_transcript(vid_id)
                transcript_text = " ".join([t["text"] for t in transcript_list])
            except:
                transcript_text = None

            all_data.append({
                "video_id": vid_id,
                "title": snippet.get("title"),
                "description": snippet.get("description"),
                "channel_title": snippet.get("channelTitle"),
                "publish_time": snippet.get("publishedAt"),
                "view_count": stats.get("viewCount"),
                "like_count": stats.get("likeCount"),
                "duration": details.get("duration"),
                "video_url": f"https://www.youtube.com/watch?v={vid_id}",
                "transcript": transcript_text
            })

        time.sleep(0.1)  # Avoid hitting quota too fast

    return all_data


# =============================
# MAIN SCRAPER FUNCTION
# =============================
def scrape_kids_videos(query="kids cartoons", max_videos=10000):
    print(f"Searching for {max_videos} videos...")
    video_ids = search_videos(query, max_videos)
    print(f"Found {len(video_ids)} videos. Fetching details...")

    dataset = get_video_details(video_ids)

    df = pd.DataFrame(dataset)
    df.to_csv("kids_youtube_dataset.csv", index=False)
    print("Dataset saved as kids_youtube_dataset.csv")


if __name__ == "__main__":
    scrape_kids_videos(query="kids cartoons", max_videos=10000)
