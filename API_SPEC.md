# Council Bill Scraper Engine — API 명세서

**Base URL:** `http://localhost:8900`  
**Framework:** FastAPI  
**버전:** 0.1.0

---

## 목차

1. [GET /scrapeList](#1-get-scrapelist)
2. [GET /scrapeView](#2-get-scrapeview)
3. [GET /stop](#3-get-stop)

---

## 공통 응답 포맷

| 필드 | 타입 | 설명 |
|------|------|------|
| `status` | `string` | 처리 결과 (`"success"`) |
| `count` | `integer` | 반환된 데이터 건수 |
| `data` | `array` | 실제 데이터 목록 |

---

## 1. GET /scrapeList

의안 목록 페이지를 크롤링하여 테이블 데이터를 반환합니다.  
Playwright를 사용해 실제 브라우저로 렌더링 후 파싱합니다.

### Query Parameters

| 파라미터 | 타입 | 필수 | 기본값 | 설명 |
|----------|------|------|--------|------|
| `url` | `string` | ✅ | — | 크롤링할 목록 페이지 URL |
| `rasmbly_numpr` | `string` | ❌ | `"9"` | 대수(기수) 번호 (예: `"9"`, `"10"`) |
| `numpr_selector` | `string` | ❌ | `"#rasmbly_numpr"` | 대수 선택 드롭다운의 CSS 셀렉터 |
| `list_table` | `string` | ❌ | `"table.board_list"` | 목록 테이블의 CSS 셀렉터 |

### 요청 예시

```
GET /scrapeList?url=https://example.council.go.kr/bill/list&rasmbly_numpr=10
```

### 응답 예시

```json
{
  "status": "success",
  "count": 25,
  "data": [
    {
      "번호": "1",
      "의안명": "○○조례 일부개정조례안",
      "제출자": "○○위원회",
      "제출일": "2025-03-01",
      "처리결과": "가결",
      "href": "/bill/detail?id=12345",
      "onclick": null,
      "BI_SJ": "○○조례 일부개정조례안"
    }
  ]
}
```

### 동작 흐름

```
1. Chromium headless 브라우저 실행
2. url 접속 (networkidle 대기)
3. numpr_selector 드롭다운에서 rasmbly_numpr 선택
4. 검색 버튼(#btnSearch) 클릭 후 로딩 대기
5. list_table 셀렉터 기준으로 테이블 파싱
6. FIELD_MAP을 통한 컬럼명 한→영 변환
7. 브라우저 종료 후 결과 반환
```

---

## 2. GET /scrapeView

의안 상세 페이지를 크롤링하여 상세 정보를 반환하고, `download/` 폴더에 JSON으로 저장합니다.

### Query Parameters

| 파라미터 | 타입 | 필수 | 기본값 | 설명 |
|----------|------|------|--------|------|
| `url` | `string` | ✅ | — | 크롤링할 상세 페이지 URL |
| `detail_mode` | `string` | ❌ | `"href"` | 상세 진입 방식 (`"href"` / `"onclick"` / `"submit"`) |
| `detail_table` | `string` | ❌ | `"table.board_view"` | 상세 정보 테이블의 CSS 셀렉터 |

#### `detail_mode` 값 설명

| 값 | 설명 |
|----|------|
| `href` | `<a href="...">` 링크로 직접 이동 |
| `onclick` | `onclick` JS 함수 실행 후 이동 |
| `submit` | Form Hidden 값 세팅 후 submit (미구현) |

### 요청 예시

```
GET /scrapeView?url=https://example.council.go.kr/bill/detail?id=12345&detail_table=table.view_table
```

### 응답 예시

```json
{
  "BI_SJ": "○○조례 일부개정조례안",
  "BI_PROPOSER": "○○위원회",
  "BI_PROPOSE_DT": "2025-03-01",
  "BI_RESULT": "가결",
  "BI_FILE_NM": "개정조례안.hwp",
  "BI_FILE_URL": "https://example.council.go.kr/files/개정조례안.hwp",
  "위원회심사": {
    "심사결과": "원안가결",
    "심사일": "2025-03-10"
  }
}
```

### 파일 저장

- 저장 경로: `download/bill_{ID}.json`
- URL에서 숫자 ID를 추출하여 파일명으로 사용
- ID를 추출할 수 없는 경우 `download/bill_data.json`으로 저장

### 동작 흐름

```
1. Chromium headless 브라우저 실행
2. url 접속 (networkidle 대기)
3. detail_table 셀렉터 기준으로 복수 테이블 파싱
4. 테이블 앞 제목 태그로 섹션명 자동 인식
5. SECTION_FIELD_MAP 기준으로 섹션별 필드명 변환
6. 첨부파일 a 태그 감지 시 BI_FILE_NM / BI_FILE_URL로 추출
7. download/ 폴더에 JSON 저장
8. 브라우저 종료 후 결과 반환
```

---

## 3. GET /stop

서버 프로세스를 즉시 종료합니다.

### Query Parameters

없음

### 요청 예시

```
GET /stop
```

### 응답

응답 없음 (프로세스가 `os._exit(0)`으로 즉시 종료됨)

---

## 내부 설정 구조 (config)

`BillScraper` 클래스에 전달되는 config 딕셔너리 구조입니다.

| 키 | 설명 | 예시 |
|----|------|------|
| `list_url` | 목록 페이지 기준 URL (상대 URL 절대화에 사용) | `"https://..."` |
| `numpr_param_selector` | 대수 선택 드롭다운 셀렉터 | `"#rasmbly_numpr"` |
| `search_btn_selector` | 검색 버튼 셀렉터 | `"#btnSearch"` |
| `list_table_selector` | 목록 테이블 셀렉터 | `"table.board_list"` |
| `pager_selector` | 페이지네이션 셀렉터 | `".pagination"` |
| `detail_entry_mode` | 상세 진입 방식 | `"href"` |
| `detail_table_selector` | 상세 테이블 셀렉터 | `"table.board_view"` |

---

## 에러 케이스

| 상황 | 결과 |
|------|------|
| `url` 파라미터 누락 | FastAPI `422 Unprocessable Entity` |
| 페이지 접속 실패 (네트워크 오류) | Playwright 예외 발생, `500 Internal Server Error` |
| 테이블 파싱 결과 0건 | `count: 0`, `data: []` 반환 |
| `FIELD_MAP` 미로드 | 원본 한글 컬럼명 그대로 반환 |

---

## 서버 실행

```bash
# 가상환경 활성화 후
python main.py
# 또는
uvicorn main:app --host 0.0.0.0 --port 8900 --reload
```

Swagger UI: `http://localhost:8900/docs`  
ReDoc: `http://localhost:8900/redoc`
