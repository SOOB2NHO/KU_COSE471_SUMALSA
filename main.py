# main.py - 네이버 API로 뉴스 URL 수집 후 댓글 수집 (Selenium 없이 API 방식)

import pandas as pd
import requests
import re
import time

# ✅ 네이버 API 인증 정보
CLIENT_ID = "DkqIEaI_ltBe70Xdm4W6"  # 본인의 API 키 입력
CLIENT_SECRET = "BAA_gdcR17"
HEADERS = {
    "X-Naver-Client-Id": CLIENT_ID,
    "X-Naver-Client-Secret": CLIENT_SECRET
}
NAVER_NEWS_API = "https://openapi.naver.com/v1/search/news.json"

# ✅ 기사 URL 수집 함수
def get_news_urls(query="21대 대선", max_articles=2000):
    collected_data = []
    for start in range(1, max_articles + 1, 100):
        display = min(100, max_articles - len(collected_data))
        params = {
            "query": query,
            "display": display,
            "start": start,
            "sort": "date"
        }
        res = requests.get(NAVER_NEWS_API, headers=HEADERS, params=params)
        data = res.json()
        for item in data.get("items", []):
            title = re.sub(r"<.*?>", "", item["title"])
            link = item["link"]
            pub_date = item["pubDate"]
            if "n.news.naver.com" in link:
                collected_data.append([title, link, pub_date])
        if len(data.get("items", [])) < 100:
            break
        time.sleep(0.5)
    return collected_data

# ✅ 수집된 기사 저장 및 댓글 수집 시작
news_output_csv = "filtered_naver_news.csv"
news_data = get_news_urls(query="21대 대선", max_articles=2000)
df_news = pd.DataFrame(news_data, columns=["제목", "기사URL", "작성일"])
df_news.to_csv(news_output_csv, index=False, encoding='utf-8-sig')
print(f"📰 기사 URL {len(df_news)}개 저장 완료: {news_output_csv}")

# ✅ 댓글 수집 대상 파일 경로
csv_path = news_output_csv
output_path = "naver_comments_result.csv"

# ✅ CSV 로드 및 URL 컬럼 선택
df = pd.read_csv(csv_path)
if 'Naverlink' in df.columns:
    urls = df['Naverlink']
elif '기사URL' in df.columns:
    urls = df['기사URL']
else:
    raise ValueError("CSV 파일에 'Naverlink' 또는 '기사URL' 컬럼이 없습니다.")

# ✅ 댓글 리스트 평탄화 함수
def flatten(l):
    return [item for sublist in l for item in (sublist if isinstance(sublist, list) else [sublist])]

# ✅ 전체 댓글 저장 리스트 초기화
total_comments = []

# ✅ 각 기사별 댓글 수집 반복
for idx, url in enumerate(urls):
    try:
        print(f"{idx+1}/{len(urls)}번째 기사 댓글 수집 중: {url}")

        oid = url.split("article/")[1].split("/")[0]
        aid = url.split("article/")[1].split("/")[1].split("?")[0]

        page = 1
        all_comments = []

        header = {
            "User-agent": "Mozilla/5.0",
            "referer": url
        }

        while True:
            callback_id = f"jQuery1123{int(time.time() * 1000)}"
            c_url = (
                f"https://apis.naver.com/commentBox/cbox/web_neo_list_jsonp.json"
                f"?ticket=news&templateId=default_society&pool=cbox5&_callback={callback_id}"
                f"&lang=ko&country=KR&objectId=news{oid}%2C{aid}&pageSize=20&indexSize=10"
                f"&listType=OBJECT&pageType=more&page={page}&sort=FAVORITE"
            )

            res = requests.get(c_url, headers=header)
            html = res.text

            if 'comment":' not in html:
                print("🚫 댓글 없음 또는 JSON 구조 이상")
                break

            try:
                total_comm = int(html.split('comment":')[1].split(",")[0])
            except Exception as e:
                print("🚫 댓글 수 파싱 오류:", e)
                break

            matches = re.findall(r'"contents":"(.*?)","userIdNo"', html)
            if not matches:
                break

            all_comments.extend(matches)

            if page * 20 >= total_comm:
                break
            page += 1
            time.sleep(0.1)

        total_comments.append({
            "url": url,
            "댓글 수": len(all_comments),
            "댓글": flatten(all_comments)
        })

    except Exception as e:
        print(f"⛔ 오류 발생: {e}")
        total_comments.append({
            "url": url,
            "댓글 수": 0,
            "댓글": []
        })

# ✅ 결과를 CSV로 저장
df_result = pd.DataFrame(total_comments)
df_result.to_csv(output_path, index=False, encoding='utf-8-sig')
print(f"✅ 저장 완료: {output_path}")