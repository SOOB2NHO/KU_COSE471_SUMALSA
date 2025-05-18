from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import pandas as pd
import time
from datetime import datetime, timedelta
from collections import deque
from concurrent.futures import ThreadPoolExecutor
import threading

# ——————————————————————————
# 설정
# ——————————————————————————
API_KEYS = [
    "AIzaSyBn1v3HjJWhqh_YHyF6sJkV41cOyLabemI", # 1
    "AIzaSyAg6WNmdx7MYigHbrA1TMf6cO89tuuqSX8", # 2
    "AIzaSyBujNpcCzdVU4QBstIIXNR2yc85WxYWdHQ", # 3
    "AIzaSyClz3cRUGIoe7ql0-KAP2b-tTIcONJkOzo", # 4
    "AIzaSyD80A71_82-_WTDVYruqPBTG0fr034ajBk", # 5
    "AIzaSyDFhQRYh_cZ6lAgjRgFQYl9BzaTE58Qh1Q", # 6
    "AIzaSyCmZxMmIIyBNfw6tR_5-xQUdlPMlVHU-_8", # 7
    "AIzaSyBh265b37BHH_0j62feHIcqXH7QYDaV-Ag", # 8
    "AIzaSyCWPFZYM197mcRNCFac5klW-WPhLjbQk74", # 9
    "AIzaSyDMRehGlKMt-Dy2czwz6DbAco18hL_qdR8", # 10
]

CHANNEL_IDS = [
    "UCugbqfMO94F9guLEb6Olb2A",  # 한겨레
    "UCF4Wxdo3inmxP-Y59wXDsFw",  # MBC
    "UCHXvjavEtkPFJCfGlm0wTXw",  # 경향
    "UCnHyx6H7fhKfUoAAaVGYvxQ",  # 동아
    "UCWlV3Lz_55UaX4JsMj-z__Q",  # 조선
    "UCH3mJ-nHxjjny2FJbJaqiDA"   # 중앙
]

KEYWORDS = ["대선", "윤석열", "이재명"]
START_DATE = "2022-03-01"
END_DATE   = "2022-03-09"
TOTAL_LIMIT = 300_000
PER_CHANNEL_LIMIT = TOTAL_LIMIT // len(CHANNEL_IDS)
POLITICS_CATEGORY = "25"  # News & Politics

# ——————————————————————————
# 전역 변수 초기화
# ——————————————————————————
_exhausted = set()
_services = {}
results = []
counts = {cid: 0 for cid in CHANNEL_IDS}
video_queues = {cid: deque() for cid in CHANNEL_IDS}

# ——————————————————————————
# 헬퍼 함수
# ——————————————————————————
def get_svc(key):
    if key not in _services:
        _services[key] = build("youtube", "v3", developerKey=key)
    return _services[key]

def rotate_call(fn, *args, **kwargs):
    for k in API_KEYS:
        if k in _exhausted: continue
        try:
            return fn(get_svc(k), *args, **kwargs)
        except HttpError as e:
            if e.resp.status == 403 and "quota" in str(e).lower():
                print(f"⚠️ {k} quotaExceeded → 제외")
                _exhausted.add(k)
                continue
            else:
                raise
    raise RuntimeError("모든 키 소진됨")

def search_videos(svc, channelId, q, start, end, pageToken=None, category=None):
    params = {
        "part": "id",
        "channelId": channelId,
        "type": "video",
        "publishedAfter": start + "Z",
        "publishedBefore": end + "Z",
        "maxResults": 50,
        "pageToken": pageToken,
        "fields": "nextPageToken,items(id/videoId)"
    }
    if q:
        params["q"] = q
    if category:
        params["videoCategoryId"] = category
    return svc.search().list(**params).execute()

def fetch_video_info(svc, videoId):
    return svc.videos().list(
        part="snippet,statistics",
        id=videoId,
        fields="items(snippet(title,channelTitle,publishedAt,categoryId,tags),"
               "statistics(viewCount,likeCount,commentCount))"
    ).execute()

def fetch_comments(svc, videoId, pageToken=None):
    return svc.commentThreads().list(
        part="snippet",
        videoId=videoId,
        maxResults=100,
        pageToken=pageToken,
        textFormat="plainText",
        fields="nextPageToken,items/snippet/topLevelComment/snippet("
               "authorChannelId/value,textDisplay,publishedAt,likeCount)"
    ).execute()

def fetch_channel_uploads(svc, channelId, pageToken=None):
    # 채널 업로드 플레이리스트 ID 조회
    ch_resp = svc.channels().list(
        part="contentDetails",
        id=channelId,
        fields="items/contentDetails/relatedPlaylists/uploads"
    ).execute()
    items = ch_resp.get("items")
    if not items:
        # 채널 정보가 없으면 빈 결과 반환
        return {"items": [], "nextPageToken": None}

    upload_pl = items[0]["contentDetails"]["relatedPlaylists"]["uploads"]
    # 업로드 플레이리스트에서 영상 ID 조회
    pl_resp = svc.playlistItems().list(
        part="contentDetails",
        playlistId=upload_pl,
        maxResults=50,
        pageToken=pageToken,
        fields="nextPageToken,items/contentDetails/videoId"
    ).execute()
    return pl_resp

# ——————————————————————————
# Phase 1: 영상 ID 큐 생성
# ——————————————————————————
start = datetime.fromisoformat(START_DATE)
end   = datetime.fromisoformat(END_DATE)
for d in range((end - start).days):
    day1 = (start + timedelta(days=d)).isoformat()
    day2 = (start + timedelta(days=d+1)).isoformat()

    for cid in CHANNEL_IDS:
        # 키워드별
        for kw in KEYWORDS:
            token = None
            while True:
                resp = rotate_call(search_videos, cid, kw, day1, day2, token)
                for it in resp.get("items", []):
                    video_queues[cid].append(it["id"]["videoId"])
                token = resp.get("nextPageToken")
                if not token:
                    break
        # 카테고리별
        token = None
        while True:
            resp = rotate_call(search_videos, cid, None, day1, day2, token, category=POLITICS_CATEGORY)
            for it in resp.get("items", []):
                video_queues[cid].append(it["id"]["videoId"])
            token = resp.get("nextPageToken")
            if not token:
                break
    
        if not video_queues[cid]:
            token2 = None
            while True:
                resp2 = rotate_call(fetch_channel_uploads, cid, token2)
                for it2 in resp2.get("items", []):
                    video_queues[cid].append(it2["contentDetails"]["videoId"])
                token2 = resp2.get("nextPageToken")
                if not token2:
                    break

# ——————————————————————————
# Phase 2: 라운드로빈 댓글 수집
# ——————————————————————————
done_all = False
while sum(counts.values()) < TOTAL_LIMIT and not done_all:
    done_all = True
    for cid in CHANNEL_IDS:
        if counts[cid] >= PER_CHANNEL_LIMIT or not video_queues[cid]:
            continue
        done_all = False
        vid = video_queues[cid].popleft()

        # 영상 정보
        info = rotate_call(fetch_video_info, vid)["items"][0]
        # 태그 정보 수집 X ; 댓글 수집 적게 되는 채널
        '''
        tags = info["snippet"].get("tags", [])
        if not any(kw in tag for tag in KEYWORDS for tag in tags):
            continue
        '''
        base = {
            "video_id": vid,
            "channel": info["snippet"]["channelTitle"],
            "video_title": info["snippet"]["title"],
            "video_published": info["snippet"]["publishedAt"],
            "video_views": info["statistics"].get("viewCount", 0),
            "video_likes": info["statistics"].get("likeCount", 0),
            "video_comment_count": info["statistics"].get("commentCount", 0),
            "video_category_id": info["snippet"]["categoryId"]
        }

        # 댓글 수집
        token = None
        while counts[cid] < PER_CHANNEL_LIMIT:
            crep = rotate_call(fetch_comments, vid, token)
            for th in crep.get("items", []):
                c = th["snippet"]["topLevelComment"]["snippet"]
                results.append({**base,
                    "comment_author_id": c["authorChannelId"]["value"],
                    "comment_text":      c["textDisplay"],
                    "comment_published": c["publishedAt"],
                    "comment_likes":     c["likeCount"]
                })
                counts[cid] += 1
                if counts[cid] >= PER_CHANNEL_LIMIT or sum(counts.values()) >= TOTAL_LIMIT:
                    break
            token = crep.get("nextPageToken")
            if not token or counts[cid] >= PER_CHANNEL_LIMIT:
                break

        time.sleep(0.1)
    # end for CHANNEL_IDS
# end while

# ——————————————————————————
# 결과 저장
# ——————————————————————————
pd.DataFrame(results).to_csv("youtube_comments_full.csv", index=False, encoding="utf-8-sig")
print("채널별 counts:", counts)
print("총 댓글:", len(results))