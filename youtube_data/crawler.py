from googleapiclient.discovery import build
import pandas as pd
import time
import hashlib
from datetime import datetime, timedelta
from googleapiclient.errors import HttpError

API_KEYS = [
    "AIzaSyBn1v3HjJWhqh_YHyF6sJkV41cOyLabemI",
    "AIzaSyAg6WNmdx7MYigHbrA1TMf6cO89tuuqSX8",
    "AIzaSyBujNpcCzdVU4QBstIIXNR2yc85WxYWdHQ",
    "AIzaSyClz3cRUGIoe7ql0-KAP2b-tTIcONJkOzo",
    "AIzaSyD80A71_82-_WTDVYruqPBTG0fr034ajBk"
]

def get_video_hash(video_id):
    return hashlib.md5(video_id.encode()).hexdigest()

def generate_date_ranges(start_date, end_date):
    current = start_date
    while current < end_date:
        next_day = current + timedelta(days=1)
        yield current, next_day
        current = next_day

def get_youtube_service(api_key):
    return build("youtube", "v3", developerKey=api_key)

def search_videos(youtube, channel_id, query, published_after, published_before, video_hash_set):
    videos = []
    request = youtube.search().list(
        part="snippet",
        channelId=channel_id,
        q=query,
        type="video",
        publishedAfter=published_after.isoformat("T") + "Z",
        publishedBefore=published_before.isoformat("T") + "Z",
        maxResults=5
    )
    response = request.execute()
    for item in response["items"]:
        video_id = item["id"]["videoId"]
        vid_hash = get_video_hash(video_id)
        if vid_hash in video_hash_set:
            continue
        video_hash_set.add(vid_hash)
        videos.append({
            "video_id": video_id,
            "title": item["snippet"]["title"],
            "channel_title": item["snippet"]["channelTitle"]
        })
    return videos

def get_video_info(youtube, video_id):
    request = youtube.videos().list(part="snippet,statistics", id=video_id)
    response = request.execute()
    item = response["items"][0]
    return {
        "title": item["snippet"]["title"],
        "channel_title": item["snippet"]["channelTitle"],
        "view_count": item["statistics"].get("viewCount", 0),
        "like_count": item["statistics"].get("likeCount", 0)
    }

def get_top_comments(youtube, video_id, top_p=3):
    request = youtube.commentThreads().list(
        part="snippet",
        videoId=video_id,
        maxResults=100,
        textFormat="plainText"
    )
    response = request.execute()
    all_comments = []
    for item in response.get("items", []):
        top_comment = item["snippet"]["topLevelComment"]["snippet"]
        comment_data = {
            "text": top_comment["textDisplay"],
            "like_count": int(top_comment.get("likeCount", 0))
        }
        all_comments.append(comment_data)
    top_comments = sorted(all_comments, key=lambda c: c["like_count"], reverse=True)[:top_p]
    return top_comments

def run_crawler(start_date_str, end_date_str, output_file="youtube_comments_all.csv"):
    keywords = ["윤석열", "이재명", "안철수", "심상정", "대선", "후보자", "국민의힘", "더불어민주당", "지지율", "TV토론", "공약"]
    channel_ids = [
        "UCugbqfMO94F9guLEb6Olb2A",
        "UCF4Wxdo3inmxP-Y59wXDsFw",
        "UCv3Y5Qz3z4Y2I4r0U7p9L2g",
        "UCnHyx6H7fhKfUoAAaVGYvxQ",
        "UCaVj4kzU4A5n4D9x3Gx3Gxw",
        "UCH3mJ-nHxjjny2FJbJaqiDA"
    ]

    start_date = datetime.fromisoformat(start_date_str)
    end_date = datetime.fromisoformat(end_date_str)
    all_results = []

    for day_start, day_end in generate_date_ranges(start_date, end_date):
        print(f"\n📅 {day_start.date()} 수집 시작")
        video_hash_set = set()
        success = False
        for api_key in API_KEYS:
            try:
                youtube = get_youtube_service(api_key)
                day_results = []
                for channel_id in channel_ids:
                    for keyword in keywords:
                        videos = search_videos(youtube, channel_id, keyword, day_start, day_end, video_hash_set)
                        for video in videos:
                            video_id = video["video_id"]
                            info = get_video_info(youtube, video_id)
                            top_comments = get_top_comments(youtube, video_id, top_p=3)
                            for comment in top_comments:
                                row = {
                                    "date": str(day_start.date()),
                                    "video_id": video_id,
                                    "video_title": info["title"],
                                    "channel": info["channel_title"],
                                    "views": info["view_count"],
                                    "video_likes": info["like_count"],
                                    "comment": comment["text"],
                                    "comment_likes": comment["like_count"]
                                }
                                day_results.append(row)
                if day_results:
                    all_results.extend(day_results)
                else:
                    print(f"⚠️ {day_start.date()}에 수집된 댓글 없음")
                success = True
                break
            except HttpError as e:
                if e.resp.status == 403 and "quota" in str(e).lower():
                    print(f"🔁 quotaExceeded: API 키 교체 시도")
                    continue
                else:
                    print(f"[에러] {day_start.date()} 처리 중 오류 발생: {e}")
                    break
        if not success:
            print(f"❌ 모든 키 실패: {day_start.date()}는 건너뜀")
        time.sleep(1)

    if all_results:
        df = pd.DataFrame(all_results)
        df.to_csv(output_file, index=False, encoding='utf-8-sig')
        print(f"\n✅ 전체 저장 완료: {output_file} ({len(df)}개 댓글)")
    else:
        print("\n⚠️ 수집된 결과가 없습니다.")

if __name__ == "__main__":
    run_crawler("2022-01-01", "2022-03-10")
