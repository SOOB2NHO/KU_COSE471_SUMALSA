import pandas as pd
import requests
import json
import html
import time

# 1) 파일 경로 설정
csv_path    = '/Users/hosubin/Desktop/ubuntu_data/Data Science/all_05_news_윤석열.xlsx'
output_path = '/Users/hosubin/Desktop/ubuntu_data/Data Science/data/네이버댓글_윤석열.csv'

# 2) URL 컬럼 로드
df = pd.read_excel(csv_path)
if 'Naverlink' in df.columns:
    urls = df['Naverlink'].dropna()
elif 'URL' in df.columns:
    urls = df['URL'].dropna()
else:
    raise ValueError("엑셀 파일에 'Naverlink' 또는 'URL' 컬럼이 없습니다.")

# 3) 댓글 크롤러 함수
def crawl_comments(url):
    oid = url.split("article/")[1].split("/")[0]
    aid = url.split("article/")[1].split("/")[1].split("?")[0]
    page = 1
    comments = []
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Referer": url
    }

    while True:
        callback = f"jQuery{int(time.time()*1000)}"
        api_url = (
            "https://apis.naver.com/commentBox/cbox/web_neo_list_jsonp.json"
            f"?ticket=news&templateId=default_society&pool=cbox5&_callback={callback}"
            "&lang=ko&country=KR"
            f"&objectId=news{oid}%2C{aid}"
            "&pageSize=20&indexSize=10&listType=OBJECT&pageType=more"
            f"&page={page}&sort=FAVORITE"
        )
        res = requests.get(api_url, headers=headers)
        text = res.text
        json_str = text[text.find('(')+1 : text.rfind(')')]
        data = json.loads(json_str)

        comment_list = data.get("result", {}).get("commentList", [])
        if not comment_list:
            break

        for c in comment_list:
            comments.append(html.unescape(c.get("contents", "")))

        total_comm = data.get("result", {}).get("totalCommentCount", len(comments))
        if page * 20 >= total_comm:
            break

        page += 1
        time.sleep(0.3)

    return comments

# 4) 전체 URL에서 댓글 수집
results = []
for idx, url in enumerate(urls, 1):
    print(f"{idx}/{len(urls)} ▶ {url} 댓글 수집 중...")
    try:
        cmts = crawl_comments(url)
    except Exception as e:
        print(f"⛔ 오류: {e}")
        cmts = []
    results.append({
        "url": url,
        "댓글 수": len(cmts),
        "댓글": cmts
    })

# 5) DataFrame 생성 및 CSV 저장
df_result   = pd.DataFrame(results)
df_exploded = df_result.explode('댓글').reset_index(drop=True)
df_exploded.to_csv(output_path, index=False, encoding='utf-8-sig')

print(f"✅ 저장 완료: {output_path}")