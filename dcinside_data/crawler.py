import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime, date
import time, random, re
from urllib.parse import urljoin
from concurrent.futures import ThreadPoolExecutor, as_completed

# 설정
gallery_id       = "stock_new2"
start_date       = datetime(2025, 5, 1).date()
base_list_url    = "https://gall.dcinside.com/board/lists/"
base_view_url    = "https://gall.dcinside.com"
RETRY_PAGE_LOAD  = 10     # 빈 페이지 재요청 횟수
RETRY_DELAY_SEC  = 0.5   # 재요청 대기

# 세션 & 헤더
session = requests.Session()
session.headers.update({
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36"
    )
})

# 날짜 파싱
today = date.today()
def parse_dc_date(txt: str) -> date:
    txt = txt.strip()
    if ":" in txt: return today
    if re.match(r"^\d{4}-\d{2}-\d{2}$", txt):
        return datetime.strptime(txt, "%Y-%m-%d").date()
    if re.match(r"^\d{2}\.\d{2}\.\d{2}$", txt):
        return datetime.strptime(txt, "%y.%m.%d").date()
    if re.match(r"^\d{1,2}\.\d{1,2}$", txt):
        m, d = map(int, txt.split("."))
        return date(today.year, m, d)
    raise ValueError(f"Unknown date: {txt}")

# 안전 정수
def safe_int(text: str) -> int:
    t = text.strip().replace(",", "")
    return int(t) if t.isdigit() else 0

# 1) 메타 수집
meta_items = []
page = 1

while True:
    # 페이지 로드 + 빈 리스트 재시도
    for attempt in range(1, RETRY_PAGE_LOAD + 1):
        resp = session.get(
            base_list_url,
            params={"id": gallery_id, "sort_type": "recent", "page": page}
        )
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "lxml")
        rows = soup.select("tbody.listwrap2 tr.ub-content, tbody.gall_list tr.ub-content")
        if rows:
            break
        print(f"[WARN] 페이지 {page} 빈 리스트, 재요청 {attempt}/{RETRY_PAGE_LOAD}")
        time.sleep(RETRY_DELAY_SEC)
    else:
        print(f"[ERROR] 페이지 {page} 로드 실패, 종료")
        break

    dates = []
    for r in rows:
        if 'ub-topic' in r.get("class", []): continue
        num = r.select_one("td.gall_num").get_text(strip=True)
        if not num.isdigit(): continue
        raw = r.select_one("td.gall_date").get_text(strip=True)
        try:
            dates.append(parse_dc_date(raw))
        except:
            continue

    if not dates:
        break

    oldest = min(dates)
    if oldest < start_date and all(d < start_date for d in dates):
        break

    for r in rows:
        if 'ub-topic' in r.get("class", []): continue
        num = r.select_one("td.gall_num").get_text(strip=True)
        if not num.isdigit(): continue

        raw = r.select_one("td.gall_date").get_text(strip=True)
        try:
            post_date = parse_dc_date(raw)
        except:
            continue
        if post_date < start_date:
            continue

        a = r.select_one("td.gall_tit a")
        href = urljoin(base_view_url, a["href"])
        meta_items.append({
            "date":      post_date,
            "title":     a.get("title", a.get_text(strip=True)).strip(),
            "hit":       safe_int(r.select_one("td.gall_count").get_text(strip=True)),
            "recommend": safe_int(r.select_one("td.gall_recommend").get_text(strip=True)),
            "url":       href
        })

    page += 1
    time.sleep(random.uniform(0.2, 0.5))

# 2) 본문 병렬 수집
def fetch_content(item):
    try:
        vresp = session.get(item["url"], timeout=10)
        vresp.raise_for_status()
        soup = BeautifulSoup(vresp.text, "lxml")
        box = soup.select_one("div.writing_view_box")
        item["content"] = box.get_text("\n", strip=True) if box else ""
    except:
        item["content"] = ""
    return item

results = []
with ThreadPoolExecutor(max_workers=10) as executor:
    futures = [executor.submit(fetch_content, dict(it)) for it in meta_items]
    for f in as_completed(futures):
        results.append(f.result())

# 3) 저장
df = pd.DataFrame(results)
df["date"] = df["date"].apply(lambda d: d.isoformat())
filename = f"{gallery_id}_{start_date}_to_{date.today()}.csv"
df.to_csv(filename, index=False, encoding="utf-8-sig")
print(f"✅ 최적화+리트라이 적용: {len(df)}개 글을 '{filename}'에 저장했습니다.")
