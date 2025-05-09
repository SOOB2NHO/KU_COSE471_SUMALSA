import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime, date
import time, random, re
from urllib.parse import urljoin
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- 설정 ---
gallery_id    = "stock_new2"
start_date    = datetime(2022, 1, 1).date()
end_date      = datetime(2022, 3, 10).date()
base_list_url = "https://gall.dcinside.com/board/lists/"
base_view_url = "https://gall.dcinside.com"

RETRY_PAGE_LOAD = 5    # 빈 페이지 재시도 횟수
RETRY_DELAY_SEC = 1.0  # 재시도 전 대기
MAX_EMPTY_PAGES = 3    # 연속 빈 페이지 허용 후 종료

# --- 세션 & 헤더 ---
session = requests.Session()
session.headers.update({
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36"
    )
})

# --- 날짜 파싱 유틸 ---
today = date.today()
def parse_dc_date(txt: str) -> date:
    txt = txt.strip()
    if ":" in txt:
        return today
    if "/" in txt:
        for fmt in ("%y/%m/%d", "%Y/%m/%d"):
            try:
                return datetime.strptime(txt, fmt).date()
            except ValueError:
                pass
    if re.match(r"^\d{4}-\d{2}-\d{2}$", txt):
        return datetime.strptime(txt, "%Y-%m-%d").date()
    if re.match(r"^\d{2}\.\d{2}\.\d{2}$", txt):
        return datetime.strptime(txt, "%y.%m.%d").date()
    if re.match(r"^\d{1,2}\.\d{1,2}$", txt):
        m, d = map(int, txt.split("."))
        return date(today.year, m, d)
    raise ValueError(f"Unknown date: {txt}")

def safe_int(text: str) -> int:
    t = text.strip().replace(",", "")
    return int(t) if t.isdigit() else 0

# --- 페이지 로드 & 날짜 범위 리턴 ---
def fetch_page_dates(page: int):
    for attempt in range(1, RETRY_PAGE_LOAD + 1):
        resp = session.get(
            base_list_url,
            params={"id": gallery_id, "sort_type": "recent", "page": page}
        )
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "lxml")
        rows = soup.select(
            "tbody.listwrap2 tr.ub-content, tbody.gall_list tr.ub-content"
        )
        if rows:
            dates = []
            for r in rows:
                if 'ub-topic' in r.get("class", []):
                    continue
                num = r.select_one("td.gall_num").get_text(strip=True)
                if not num.isdigit():
                    continue
                raw = r.select_one("td.gall_date").get_text(strip=True)
                try:
                    dates.append(parse_dc_date(raw))
                except:
                    pass
            if dates:
                return max(dates), min(dates), rows
        time.sleep(RETRY_DELAY_SEC)
    return None, None, []

# --- 이진탐색 경계 찾기 ---
def get_page_bounds(target: date, low: int = 1):
    lo, hi = low, 1
    # 지수 탐색
    while True:
        newest, oldest, _ = fetch_page_dates(hi)
        if newest is None:
            raise RuntimeError(f"페이지 {hi} 로드 실패")
        if oldest < target:
            break
        lo = hi
        hi *= 2
    # 이진 탐색
    while lo + 1 < hi:
        mid = (lo + hi) // 2
        _, oldest, _ = fetch_page_dates(mid)
        if oldest < target:
            hi = mid
        else:
            lo = mid
    return hi

# --- 경계 계산 ---
print("Calculating page bounds…")
end_page   = get_page_bounds(end_date)
start_page = get_page_bounds(start_date)
lo, hi = min(start_page, end_page), max(start_page, end_page)
print(f"Pages to scan dynamically from {lo} onwards until done")

# --- 동적 스캔 + 메타 수집 ---
meta_items = []
page = lo
empty_count = 0
while True:
    newest, oldest, rows = fetch_page_dates(page)
    print(f"[PAGE {page}] newest={newest}, oldest={oldest}, rows={len(rows)}")

    # 빈 페이지 연속 카운트
    if not rows:
        empty_count += 1
        print(f"[DEBUG] empty page {page}, count {empty_count}/{MAX_EMPTY_PAGES}")
        if empty_count >= MAX_EMPTY_PAGES:
            print("[DEBUG] too many empty pages, stopping")
            break
        page += 1
        time.sleep(0.1)
        continue
    empty_count = 0

    # 너무 최신: skip
    if oldest > end_date:
        page += 1
        continue
    # 너무 오래전: done
    if newest < start_date:
        break

    # 메타 수집
    for r in rows:
        raw = r.select_one("td.gall_date").get_text(strip=True)
        dt = parse_dc_date(raw)
        if not (start_date <= dt <= end_date):
            continue
        a = r.select_one("td.gall_tit a")
        meta_items.append({
            "date": dt,
            "title": a.get("title", a.get_text(strip=True)).strip(),
            "hit": safe_int(r.select_one("td.gall_count").get_text(strip=True)),
            "recommend": safe_int(r.select_one("td.gall_recommend").get_text(strip=True)),
            "url": urljoin(base_view_url, a["href"])
        })

    page += 1
    time.sleep(random.uniform(0.05, 0.15))

# --- 본문 병렬 수집 ---
print("Fetching content in parallel…")
results = []
with ThreadPoolExecutor(max_workers=10) as executor:
    futures = [executor.submit(lambda item: fetch_page_dates and {**item, "content": BeautifulSoup(
        session.get(item["url"], timeout=10).text, "lxml").select_one("div.writing_view_box").get_text("\n",strip=True)
    if BeautifulSoup(session.get(item["url"], timeout=10).text,"lxml").select_one("div.writing_view_box") else ""
    }, it) for it in meta_items]
    for f in as_completed(futures):
        results.append(f.result())

# --- 저장 ---
df = pd.DataFrame(results)
df["date"] = df["date"].apply(lambda d: d.isoformat())
filename = f"{gallery_id}_{start_date}_to_{end_date}.csv"
df.to_csv(filename, index=False, encoding="utf-8-sig")
print(f"✅ Completed: {len(df)} posts saved to '{filename}'")
