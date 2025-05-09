# 2. 정치 뉴스 및 SNS 여론 분석 기반 정당 성향 분류

**2025년 21대 대통령 선거**를 앞두고, 뉴스 기사 및 온라인 커뮤니티 데이터를 수집하여 정당 성향 분석에 활용하기 위한 데이터 수집 파이프라인

---

## 📊 수집 대상 및 기간

- **대상 플랫폼**:  
  1. **네이버 뉴스 (정치 기사 + 댓글)**  
  2. **디시인사이드 정치/사회 갤러리 (게시글)**
  3. **유튜브 6개 언론사 (한겨레, MBC, 경향, 동아일보, 조선일보, 중앙일보)**

- **수집 기간**:  

---

## 📘 수집 내용

### 1. 📰 네이버 뉴스 + 댓글 수집기 (API 기반)

- **검색 키워드**: `"21대 대선 / 20대 대선 OR 윤석열 OR 이재명 OR 안철수"`
- **수집 방식**:  
  - 네이버 뉴스 검색 API를 통해 뉴스 기사 URL 최대 1000개 수집  
  - 기사 필터링: `n.news.naver.com` 도메인만 수집  
  - 각 기사별 댓글을 좋아요 순으로 수집 (`sort=FAVORITE`)
- **수집 항목**:
  - 뉴스 제목
  - 뉴스 URL
  - 작성일
  - 댓글 수
  - 댓글 내용 리스트
- **결과 파일**:
  - `filtered_naver_news.csv` (기사 목록)
  - `naver_comments_result.csv` (기사별 댓글 데이터)

---

### 2. 🗣️ 디시인사이드 정치/사회 갤러리 게시글 수집기

- **대상 갤러리**:  
  - [정치 갤러리](https://gall.dcinside.com/board/lists/?id=politics)  
  - [사회 갤러리](https://gall.dcinside.com/board/lists/?id=society)
- **수집 방식**:  
  - Selenium 기반 자동 스크롤 및 게시글 목록 탐색
  - 게시글 상세 페이지 접근 후 정보 추출
- **수집 항목**:
  - 게시글 ID
  - 게시글 제목
  - 게시글 조회수
  - 게시글 좋아요 수
- **결과 파일**:
  - `dcinside_politics_20220101_20220310.csv`
  - `dcinside_society_20220101_20220310.csv`

---

## 🛠️ 기술 스택

- Python 3.8+
- `requests`, `pandas`, `re`, `time`
- `Selenium`, `BeautifulSoup4`
- 네이버 OpenAPI: `https://openapi.naver.com/v1/search/news.json`
- 네이버 댓글 API: `https://apis.naver.com/commentBox/cbox/web_neo_list_jsonp.json`
