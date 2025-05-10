from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import pandas as pd
import time
from datetime import datetime, timedelta
import hashlib

# ✅ 사용자 설정
API_KEYS = [
    "AIzaSyBn1v3HjJWhqh_YHyF6sJkV41cOyLabemI",
    "AIzaSyAg6WNmdx7MYigHbrA1TMf6cO89tuuqSX8",
    "AIzaSyBujNpcCzdVU4QBstIIXNR2yc85WxYWdHQ",
    "AIzaSyClz3cRUGIoe7ql0-KAP2b-tTIcONJkOzo",
    "AIzaSyD80A71_82-_WTDVYruqPBTG0fr034ajBk"
]

CHANNEL_IDS = [
    "UCugbqfMO94F9guLEb6Olb2A",  # 한겨레
    "UCF4Wxdo3inmxP-Y59wXDsFw",  # MBC
    "UCv3Y5Qz3z4Y2I4r0U7p9L2g",  # 경향
    "UCnHyx6H7fhKfUoAAaVGYvxQ",  # 동아
    "UCaVj4kzU4A5n4D9x3Gx3Gxw",  # 조선
    "UCH3mJ-nHxjjny2FJbJaqiDA"   # 중앙
]
KEYWORDS = ["대선", "윤석열", "이재명"]

START_DATE = "2022-03-01"
END_DATE = "2022-03-10"

# ✅ 도우미 함수
def get_youtube_service(api_key):
    return build("youtube", "v3", developerKey=api_key)

def get_video_hash(video_id):
    return hashlib.md5(video_id.encode()).hexdigest()

def generate_date_ranges(start, end):
    current = start
    while current < end:
        yield current, current + timedelta(days=1)
        current += timedelta(days=1)

# ✅ 영상 검색 함수 (제목+설명 기준 필터링)
def search_videos(youtube, channel_id, query, start_date, end_date, seen_videos):
    videos = []
    next_page_token = None
    while True:
        request = youtube.search().list(
            part="snippet",
            channelId=channel_id,
            q=query,
            type="video",
            publishedAfter=start_date.isoformat() + "Z",
            publishedBefore=end_date.isoformat() + "Z",
            maxResults=50,
            pageToken=next_page_token
        )
        response = request.execute()
        for item in response.get("items", []):
            snippet = item["snippet"]
            title = snippet.get("title", "")
            description = snippet.get("description", "")
            if query not in (title + description):
                continue
            video_id = item["id"]["videoId"]
            if video_id in seen_videos:
                continue
            seen_videos.add(video_id)
            videos.append(video_id)
        next_page_token = response.get("nextPageToken")
        if not next_page_token:
            break
    return videos

# ✅ 영상 정보 + 댓글 수집

def get_video_info(youtube, video_id):
    response = youtube.videos().list(part="snippet,statistics", id=video_id).execute()
    item = response["items"][0]
    return {
        "video_id": video_id,
        "video_title": item["snippet"]["title"],
        "channel": item["snippet"]["channelTitle"],
        "video_published": item["snippet"]["publishedAt"],
        "video_views": item["statistics"].get("viewCount", 0),
        "video_likes": item["statistics"].get("likeCount", 0),
        "video_comment_count": item["statistics"].get("commentCount", 0),
        "video_category_id": item["snippet"].get("categoryId", "")
    }

def get_all_comments(youtube, video_id):
    comments = []
    next_page_token = None
    while True:
        response = youtube.commentThreads().list(
            part="snippet",
            videoId=video_id,
            maxResults=100,
            textFormat="plainText",
            pageToken=next_page_token
        ).execute()
        for item in response.get("items", []):
            top_comment = item["snippet"]["topLevelComment"]["snippet"]
            comments.append({
                "comment_author_id": top_comment.get("authorChannelId", {}).get("value", "N/A"),
                "comment_text": top_comment.get("textDisplay", ""),
                "comment_published": top_comment.get("publishedAt", ""),
                "comment_likes": top_comment.get("likeCount", 0)
            })
        next_page_token = response.get("nextPageToken")
        if not next_page_token:
            break
    return comments

# ✅ 크롤링 실행

def run_full_crawler():
    start_date = datetime.fromisoformat(START_DATE)
    end_date = datetime.fromisoformat(END_DATE)
    seen_videos = set()
    results = []

    for single_date, next_date in generate_date_ranges(start_date, end_date):
        print(f"📅 {single_date.date()} 수집 중...")
        for api_key in API_KEYS:
            try:
                youtube = get_youtube_service(api_key)
                for channel_id in CHANNEL_IDS:
                    for keyword in KEYWORDS:
                        video_ids = search_videos(youtube, channel_id, keyword, single_date, next_date, seen_videos)
                        for vid in video_ids:
                            info = get_video_info(youtube, vid)
                            comments = get_all_comments(youtube, vid)
                            for c in comments:
                                results.append({**info, **c})
                break  # API 키 성공 시 반복 종료
            except HttpError as e:
                if e.resp.status == 403 and "quota" in str(e).lower():
                    print("⚠️ quotaExceeded → 다음 키로 전환")
                    continue
                else:
                    print("❌ 기타 오류 발생:", e)
                    break
        time.sleep(1)

    df = pd.DataFrame(results)
    df.to_csv("youtube_comments_full.csv", index=False, encoding="utf-8-sig")
    print(f"✅ 저장 완료: {len(df)}개 댓글 수집됨")

run_full_crawler()