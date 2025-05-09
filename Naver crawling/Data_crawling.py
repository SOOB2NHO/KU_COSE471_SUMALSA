import pandas as pd
import requests
import re
from bs4 import BeautifulSoup
import time

# CSV 파일 경로
csv_path = '/Users/hosubin/Desktop/ubuntu_data/Data Science/filtered_naver_news.csv'
output_path = '/Users/hosubin/Desktop/ubuntu_data/Data Science/네이버뉴스_댓글_결과.csv'

# CSV 로드 및 컬럼 확인
df = pd.read_csv(csv_path)
if 'Naverlink' in df.columns:
    urls = df['Naverlink']
elif '기사URL' in df.columns:
    urls = df['기사URL']
else:
    raise ValueError("CSV 파일에 'Naverlink' 또는 '기사URL' 컬럼이 없습니다.")

def flatten(l):
    return [item for sublist in l for item in (sublist if isinstance(sublist, list) else [sublist])]

total_comments = []

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
            c_url = f"https://apis.naver.com/commentBox/cbox/web_neo_list_jsonp.json?ticket=news&templateId=default_society&pool=cbox5&_callback={callback_id}&lang=ko&country=KR&objectId=news{oid}%2C{aid}&pageSize=20&indexSize=10&listType=OBJECT&pageType=more&page={page}&sort=FAVORITE"

            res = requests.get(c_url, headers=header)
            html = res.text

            # 'comment":'가 아예 없으면 패스
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
            time.sleep(0.1)  # 과도한 요청 방지

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

# CSV로 저장
df_result = pd.DataFrame(total_comments)
df_result.to_csv(output_path, index=False, encoding='utf-8-sig')
print(f"✅ 저장 완료: {output_path}")