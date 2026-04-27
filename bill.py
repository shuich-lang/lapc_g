import os
import re
import httpx
import json
from datetime import datetime
from typing import List, Dict, Any, Optional
from urllib.parse import urljoin, urlparse
from pydantic import BaseModel, Field, field_validator, ValidationInfo
from typing import Optional
from fastapi import FastAPI, BackgroundTasks
from fastapi.responses import JSONResponse
from playwright.async_api import async_playwright, Page
import time

try:
    from field_maps.field_map import FIELD_MAP, SECTION_FIELD_MAP
except ImportError:
    print("[!] field_maps/field_map.py 로드 실패, 기본값 사용")
    FIELD_MAP, SECTION_FIELD_MAP = {}, {}

app = FastAPI(title="Enterprise Council Scraper API")
app.state.stop_scraping = False
DOWNLOAD_DIR = "download"
FILE_DOWNLOAD_DIR = "attachment"
FIELD_LOGS_DIR = "field_logs"

# 차단할 리소스 타입
BLOCKED_RESOURCES = {"image", "stylesheet", "media", "font"}
# view_id 자동 탐지용 파라미터 패턴
VIEW_ID_AUTO_PARAMS = r"[?&](uid|idx|code|no|seq|id|bill_no|billNo|idx_no|nttId|uuid)=([^&]+)"
# 페이지 파라미터 패턴 (페이지네이션 URL 치환용)
PAGE_PARAM_PATTERN = r'([?&](?:page|pageIndex|p|page_no|pageno|cPage|pageNum|page_id|cp))=(\d+)'
# 검색 버튼 함정 단어 (상단 메뉴 탭 배제용)
TRAP_WORDS = ["엑셀", "초기화", "취소", "통합", "메뉴", "상세", "회기", "의안", "별검색", "다운", "연혁"]
# 중복 컬럼 패턴 상수
_P_BI_NO_SESN    = re.compile(r'^(.+?)\s*[\(\（]제\s*(\d+)\s*회[\)\）]')  # "2827 (제343회)"
_P_NUMPR_SESN    = re.compile(r'제\s*(\d+)\s*대.*?제\s*(\d+)\s*회')       # "제8대 제266회"
_P_NUMPR_ONLY    = re.compile(r'제\s*(\d+)\s*대')                        # "제8대"
_P_SESN_ONLY     = re.compile(r'제\s*(\d+)\s*회')                        # "제266회"
_P_DIGIT_ONLY    = re.compile(r'^\d+$')                                  # "8", "266"
_P_SLASH_NUMPR_SESN = re.compile(r'(\d+)\s*대\s*/\s*제\s*(\d+)\s*회')     # "9대 / 제315회 임시회"
_P_HYPHEN_NUMPR_SESN = re.compile(r'(\d+)\s*대\s*[-–—]\s*(\d+)\s*회')   # "9대-287회"
_P_DAE_HOE = re.compile(r'(\d+)\s*대\s*(\d+)\s*회')                     # "9대 268회"
# 날짜 형식
_DATE_PATTERN = re.compile(r'(\d{4})[./\-](\d{1,2})[./\-](\d{1,2})')

# LAST_DATA 비교용 핵심 필드 목록 / 하드코딩없이 우선순위 순으로 시도
_LAST_DATA_MATCH_KEYS: List[str] = ["URL","BI_SJ","BI_CN","BI_NO",]

# 감사 대상 키 집합 (복합 파서가 분리 생성하는 키 포함)
_AUDIT_KEYS: frozenset = frozenset(
    list(FIELD_MAP.values()) +
    [v for sec in SECTION_FIELD_MAP.values() for v in sec.values()]
) - {"RASMBLY_NUMPR_SESN"}  # 복합키는 파서가 분리하므로 제외

# ────────────────────────────────────────────────────────────
# [신규] last_data 모델
# ────────────────────────────────────────────────────────────
class LastData(BaseModel):
    model_config = {"extra": "allow"}
    URL:     Optional[str] = Field(None, description="이전 수집 상세 URL")
    BI_SJ:   Optional[str] = Field(None, description="이전 수집 의안 제목")
    BI_CN:   Optional[str] = Field(None, description="이전 수집 내부 관리번호")
    BI_NO:   Optional[str] = Field(None, description="이전 수집 의안번호")

# 상세 파라미터
class ScrapeParam(BaseModel):
    list_url: str = Field(..., description="의회 리스트 URL")
    view_url: Optional[str] = Field(None, description="상세 진입 URL")
    view_id_param: str = Field("uuid", description="상세 식별 파라미터명")
    rasmbly_numpr: str = Field("", description="대수 (공백=전체)")
    list_class: str = Field("table.board_list", description="리스트 테이블 셀렉터")
    view_class: Optional[str] = Field(None, description="상세 테이블 셀렉터")
    max_pages: str = Field("", description="수집 페이지 수 (공백/0=전체)")
    paging_selector: str = Field("div#pagingNav", description="페이징 영역 셀렉터")
    next_btn_selector: str = Field("a.num_right", description="다음 버튼 셀렉터")
    end_btn_selector: str = Field("a.num_last", description="마지막 페이지 버튼 셀렉터")
    search_form_selector: str = Field("form#search_form", description="검색 폼 셀렉터")
    numpr_select_selector: str = Field("select#th_sch", description="대수 선택 셀렉터")
    search_btn_selector: str = Field("button.btn.blue", description="검색 버튼 셀렉터")

# 메타 정보와 param을 포함하는 전체 요청 모델
class ScrapeRequest(BaseModel):
    req_id: str = Field(..., min_length=1, description="요청 식별자")
    type: str = Field(..., min_length=1, description="수집 타입")
    crw_id: str = Field(..., min_length=1, description="크롤러 식별자")
    file_dir: str = Field(..., description="파일 저장 디렉토리")
    
    # 중첩 구조 정의
    param: ScrapeParam = Field(..., description="크롤링 상세 설정")

    last_data: Optional[LastData] = Field(None, description="추가수집 기준점 (없으면 전체 수집)")

    @field_validator('req_id', 'type', 'crw_id', 'file_dir')
    @classmethod
    def not_empty(cls, v: str, info: ValidationInfo):
        if not v or not v.strip():
            raise ValueError(f"[{info.field_name}] 필수 파라미터가 비어있습니다.")
        return v

def error_response(msg: str):
    return JSONResponse(
        status_code=200, # 요청 자체는 성공으로 보되 로직상 실패 처리
        content={"ok": False, "message": msg}
    )

# --- 유틸리티 ---

def clean_text(text: Optional[str]) -> str:
    return re.sub(r'\s+', ' ', text.strip()) if text else ""

def extract_domain(url: str) -> str:
    """URL에서 도메인 코드 추출 (예: www.guroc.go.kr -> guroc)"""
    try:
        netloc = urlparse(url).netloc.split(':')[0].lower()
        if not netloc: return "unknown"
        
        prefixes = ("www.", "council.", "office.", "assembly.")
        for pfx in prefixes:
            if netloc.startswith(pfx):
                netloc = netloc[len(pfx):]
                break # 하나만 제거하고 중단
        
        # 첫 번째 세그먼트 반환 (예: onjin.go.kr -> onjin)
        return netloc.split('.')[0]
        
    except Exception:
        return "unknown"

def save_to_json(data: Any, domain: str, prefix: str) -> str:
    """JSON 파일 저장 후 경로 반환"""
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)
    filename = f"{domain}_{prefix}_{datetime.now():%Y%m%d%H%M%S}.json"
    filepath = os.path.join(DOWNLOAD_DIR, filename)
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)
    print(f"[+] 저장: {filepath}", flush=True)
    return filepath
def normalize_selector(selector: str) -> str:
    """클래스명(.name)이나 ID(#name)만 들어와도 Playwright가 인식할 수 있는 표준 셀렉터로 반환합니다."""
    if not selector: return ""
    s = selector.strip()
    
    # 이미 표준 셀렉터 형식이면 그대로 반환
    if any(s.startswith(p) for p in (".", "#", "[", "table", "div", "ul", "nav", "span", "a", "button")):
        return s
    
    # 태그 없이 이름만 들어온 경우 클래스로 간주 (유연성 확보)
    return f".{s}"

def get_mapped_key(label: str, section: Optional[str] = None) -> str:
    """라벨 텍스트를 FIELD_MAP 표준 키로 변환"""
    label = clean_text(label)
    normalized = label.replace(" ", "")
    if section:
        # 1차: 원본 섹션명으로 직접 탐색 (예: "소관위원회 심사경과" 그대로)
        for mk, mv in SECTION_FIELD_MAP.get(section, {}).items():
            if mk.replace(" ", "") == normalized:
                return mv
        # 2차: 축약 sec_key로 탐색 (예: "위원회", "본회의")
        sec_key = "위원회" if "위원회" in section else ("본회의" if "본회의" in section else section)
        if sec_key != section:
            for mk, mv in SECTION_FIELD_MAP.get(sec_key, {}).items():
                if mk.replace(" ", "") == normalized:
                    return mv
    # 3차: 섹션 무관 전체 FIELD_MAP
    for mk, mv in FIELD_MAP.items():
        if mk.replace(" ", "") == normalized:
            return mv
    return label

def _parse_bi_no(value: str) -> Dict[str, str]:
    """BI_NO + RASMBLY_SESN 분리"""
    v = value.strip()
    m = _P_BI_NO_SESN.match(v)
    if m:
        return {
            "BI_NO":        m.group(1).strip(),
            "RASMBLY_SESN": _to_int_str(m.group(2))
        }
    
    # 추가: "531 (9대-295회)" / "531 (9대/295회)" / "531 (9대 295회)"
    m = re.match(r'^(.+?)\s*[\(\（](\d+)\s*대\s*[-/\s]\s*(\d+)\s*회[\)\）]', v)
    if m:
        return {
            "BI_NO":         m.group(1).strip(),
            "RASMBLY_NUMPR": m.group(2),
            "RASMBLY_SESN":  m.group(3)
        }

    # 추가: "531 (제9대 제295회)"
    m = re.match(r'^(.+?)\s*[\(\（]제\s*(\d+)\s*대.*?제\s*(\d+)\s*회[\)\）]', v)
    if m:
        return {
            "BI_NO":         m.group(1).strip(),
            "RASMBLY_NUMPR": m.group(2),
            "RASMBLY_SESN":  m.group(3)
        }
    return {"BI_NO": v}


def _parse_numpr_sesn(value: str) -> Dict[str, str]:
    v = value.strip()

    m = _P_NUMPR_SESN.search(v)
    if m:
        return {"RASMBLY_NUMPR": _to_int_str(m.group(1)),"RASMBLY_SESN":  _to_int_str(m.group(2))}

    m = _P_SLASH_NUMPR_SESN.search(v)
    if m:
        return {"RASMBLY_NUMPR": _to_int_str(m.group(1)),"RASMBLY_SESN":  _to_int_str(m.group(2))}

    m = _P_HYPHEN_NUMPR_SESN.search(v)
    if m:
        return {"RASMBLY_NUMPR": _to_int_str(m.group(1)),"RASMBLY_SESN":  _to_int_str(m.group(2))}
    
    m = _P_DAE_HOE.search(v)
    if m:
        return {"RASMBLY_NUMPR": _to_int_str(m.group(1)), "RASMBLY_SESN": _to_int_str(m.group(2))}

    m = _P_NUMPR_ONLY.search(v)
    if m:
        return {"RASMBLY_NUMPR": _to_int_str(m.group(1))}

    m = _P_SESN_ONLY.search(v)
    if m:
        return {"RASMBLY_SESN": _to_int_str(m.group(1))}

    if _P_DIGIT_ONLY.match(v):
        return {"RASMBLY_NUMPR": _to_int_str(v)}

    return {}

def _to_int_str(value: str) -> str:
    """숫자만 추출해 문자열로 반환 (DB NUMBER 컬럼 대응)"""
    m = re.search(r'\d+', value)
    return m.group() if m else ""

def build_save_path(req: "ScrapeRequest", year: str, bi_cn: str, seq: int, ext: str) -> str:
    """저장 경로 생성 /{file_dir}/{type}/{crw_id}/{rasmbly_numpr}/{year}/CLIKC{bi_cn}_{seq}.{ext}"""
    root      = req.file_dir
    req_type  = req.type
    crw_id    = req.crw_id
    rasmbly   = req.param.rasmbly_numpr or "0"
    filename  = f"CLIKC{bi_cn}_{seq}{ext}"
    # path      = os.path.join(root, req_type, crw_id, rasmbly, year, filename) # 로컬용
    path = os.path.join("/", root, req_type, crw_id, rasmbly, year, filename) # 운영개발용
    os.makedirs(os.path.dirname(path), exist_ok=True)
    return path

def _normalize_date(value: str) -> str:
    """날짜 형식 정규화: 2024.01.01 → 20240101"""
    m = _DATE_PATTERN.search(value)
    if m:
        y, mo, d = m.group(1), m.group(2).zfill(2), m.group(3).zfill(2)
        return f"{y}{mo}{d}"
    return value

def _build_result(view_data: list, log: list, interrupted: bool, error: str = "") -> dict:
    """수집 결과 상태 자동 판별"""
    has_timeout = any("Timeout" in (e.get("error") or "") for e in log)
    has_error   = any(e.get("error") for e in log)
    data_count  = len(view_data)

    if error:
        status, code, message = "FAILED", "500", f"수집 실패: {error}"
    elif data_count == 0 and has_timeout:
        status, code, message = "TIMEOUT", "408", "타임아웃으로 수집 불가"
    elif data_count == 0:
        status, code, message = "EMPTY", "204", "수집 결과 없음"
    elif interrupted or has_timeout or has_error:
        status, code, message = "PARTIAL", "206", "일부 수집 완료 (오류/중단 포함)"
    else:
        status, code, message = "SUCCESS", "200", "수집 완료"

    return {
        "status":      status,
        "code":        code,
        "message":     message,
        "dataCount":   data_count,
        "interrupted": interrupted,
    }

# ── 표준키 → 파서 함수 매핑 테이블 ────────────────────────
# 새 파싱 규칙: 이 딕셔너리에만 추가
VALUE_PARSERS: Dict[str, callable] = {
    "BI_NO":              _parse_bi_no,
    "RASMBLY_NUMPR_SESN": _parse_numpr_sesn,
    "RASMBLY_NUMPR":      lambda v: {"RASMBLY_NUMPR": _to_int_str(v)},
    "RASMBLY_SESN":       lambda v: {"RASMBLY_SESN":  _to_int_str(v)},
}

def parse_value(mapped_key: str, raw_value: str) -> Dict[str, str]:
    """표준키와 원시값을 받아 DB 적재용 딕셔너리를 반환"""
    parser = VALUE_PARSERS.get(mapped_key)
    if parser:
        result = parser(raw_value)
        return result if result else {mapped_key: raw_value}
    
    # _DE로 끝나는 키는 날짜 정규화
    if mapped_key.endswith("_DE"):
        return {mapped_key: _normalize_date(raw_value)}
    
    return {mapped_key: raw_value}

# ────────────────────────────────────────────────────────────────────────────
# [신규] last_data 매칭 유틸리티
# ────────────────────────────────────────────────────────────────────────────

def _build_last_data_signature(last_data: LastData) -> Dict[str, str]:
    """
    last_data 에서 비교 가능한 필드만 추출해 딕셔너리로 반환.
    extra 필드(model_extra)도 포함하여 향후 확장에 대비.
    """
    sig: Dict[str, str] = {}
    # 선언된 필드
    for key in _LAST_DATA_MATCH_KEYS:
        val = getattr(last_data, key, None)
        if val and str(val).strip():
            sig[key] = str(val).strip()
    # extra 필드 (LastData에 선언되지 않은 필드도 비교 대상에 추가)
    for key, val in (last_data.model_extra or {}).items():
        if val and str(val).strip():
            sig[key] = str(val).strip()
    return sig


def is_last_data_match(item: Dict[str, Any], last_sig: Dict[str, str]) -> bool:
    """
    수집된 item 이 last_data 와 일치하는지 판단.

    매칭 전략 (하드코딩 없이 _LAST_DATA_MATCH_KEYS 순서 우선):
      1. view_id 가 last_sig 에 있고 item['view_id'] 와 같으면 → True
      2. URL 이 last_sig 에 있고 item['URL'] 과 같으면 → True
      3. BI_SJ 이 last_sig 에 있고 item['BI_SJ'] 과 같으면 → True
      4. BI_CN 이 last_sig 에 있고 item['BI_CN'] 과 같으면 → True
      5. BI_NO 가 last_sig 에 있고 item['BI_NO'] 과 같으면 → True
      6. extra 필드 중 item 에 동일 키/값이 존재하면 → True
      → 모두 불일치하면 False
    """
    if not last_sig:
        return False

    for key in _LAST_DATA_MATCH_KEYS:
        if key in last_sig and last_sig[key]:
            item_val = str(item.get(key, "")).strip()
            if item_val and item_val == last_sig[key]:
                print(f"[last_data] '{key}' 일치 → 추가수집 중단 기준점 도달: {last_sig[key]}", flush=True)
                return True

    # extra 필드 비교 (선언되지 않은 필드)
    # for key, sig_val in last_sig.items():
    #     if key in _LAST_DATA_MATCH_KEYS:
    #         continue  # 이미 위에서 처리
    #     item_val = str(item.get(key, "")).strip()
    #     if item_val and item_val == sig_val:
    #         print(f"[last_data] extra 필드 '{key}' 일치 → 추가수집 중단 기준점 도달: {sig_val}", flush=True)
    #         return True

    return False


# last_data 리스트 단계 조기 중단 판별
def is_list_item_past_last(list_item: Dict[str, Any], last_sig: Dict[str, str]) -> bool:
    """
    리스트 수집 결과의 단일 item 이 last_data 의 기준점 이후 데이터인지 판별.
    (view_id / link_href / URL 중 하나라도 일치하면 True)

    - list_item: extract_list_page() 반환 형식
                 {"view_id": "...", "link_href": "...", "BI_SJ": "...", ...}
    - last_sig : _build_last_data_signature() 반환값
    """
    if not last_sig:
        return False

    # view_id 비교
    vid = str(list_item.get("view_id", "")).strip()
    if vid and last_sig.get("view_id") and vid == last_sig["view_id"]:
        print(f"[last_data][리스트] view_id 일치 → 이후 항목 모두 건너뜀: {vid}", flush=True)
        return True

    # URL 비교: link_href 가 last_sig URL의 끝 부분을 포함하는지
    href = str(list_item.get("link_href", "")).strip()
    last_url = last_sig.get("URL", "")
    if href and last_url:
        # 절대 URL 과 상대 URL 모두 대응 (파라미터 포함 비교)
        if href == last_url or (href and href in last_url) or (last_url and last_url.endswith(href)):
            print(f"[last_data][리스트] URL 일치 → 이후 항목 모두 건너뜀: {href}", flush=True)
            return True

    return False

# 컬럼 수집 감사 로그 생성
def audit_fields(view_id: str, url: str, bi_cn: str, item: dict) -> dict:
    """
    수집 결과 item을 FIELD_MAP 전체 기대값과 비교해
    collected / empty / missing 세 버킷으로 분류한 감사 로그 반환.
    """
    collected, empty, missing = [], [], []
    for key in sorted(_AUDIT_KEYS):
        if key not in item:              missing.append(key)
        elif not str(item[key]).strip(): empty.append(key)
        else:                            collected.append(key)

    # 플랫 반환 — save_field_logs에서 {"field_log": entry}로 래핑
    return {
        "view_id":   view_id,
        "BI_CN":     bi_cn,
        "URL":       url,
        "collected": collected,
        "empty":     empty,
        "missing":   missing,
    }

def save_field_logs(field_logs: list, req: "ScrapeRequest") -> None:
    now  = datetime.now()
    path = os.path.join(FIELD_LOGS_DIR, req.type, req.crw_id, now.strftime("%Y"), now.strftime("%m"), f"{req.req_id}.json")
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump({FIELD_LOGS_DIR: field_logs}, f, ensure_ascii=False, indent=4)
    print(f"[+] field_logs 저장: {path} ({len(field_logs)}건)", flush=True)

# --- 브라우저 / 페이지네이션 헬퍼 ---

async def _setup_browser(pw):
    """헤드리스 브라우저 생성 + 불필요 리소스 차단"""
    browser = await pw.chromium.launch(headless=True)
    page = await browser.new_page()
    await page.route("**/*", lambda r: r.abort() if r.request.resource_type in BLOCKED_RESOURCES else r.continue_())
    return browser, page

async def _collect_pages(page, list_url, numpr, list_class, vid_param,
                         max_pages, paging_sel, next_btn_sel, end_btn_sel, extractor, stop_check, search_form_selector, numpr_select_selector, search_btn_selector, last_sig: Optional[Dict[str, str]] = None):
    """공통 리스트 수집 루프 (필터 + 페이지네이션)"""
    collect_errors = []
    last_data_reached = False  # ← [신규] 중단 플래그

    await page.goto(list_url, wait_until="domcontentloaded", timeout=5000)
    
    if numpr and numpr.strip():
        # 검색 후 list_class가 나타날 때까지 기다리도록 인자 추가
        filter_errors = await UniversalCrawler.apply_filter_and_search(page, numpr.strip(), list_class, search_form_selector, numpr_select_selector, search_btn_selector)
        collect_errors.extend(filter_errors)
    else:
        # 검색 안 할 때도 리스트는 기다려야 함
        await page.wait_for_selector(normalize_selector(list_class), timeout=2000)

    total = await UniversalCrawler.get_total_pages(page, end_btn_sel)
    safe_max = int(max_pages.strip()) if max_pages and max_pages.strip().isdigit() else 0
    target = total if safe_max == 0 else min(safe_max, total)
    data = []

    for cp in range(1, target + 1):
        if stop_check():
            print("[!] 중단 요청 감지", flush=True)
            break
        print(f"[*] 수집: {cp}/{target}p", flush=True)
        try:
            page_items = await extractor(page, list_class, vid_param)  # 페이지에서 데이터 추출
        
            if last_sig:
                filtered_items = []
                for li in page_items:
                    if is_list_item_past_last(li, last_sig):
                        # 기준점 도달 → 해당 항목 포함하지 않고 루프 종료
                        last_data_reached = True
                        break
                    filtered_items.append(li)
                data.extend(filtered_items)
                if last_data_reached:
                    print(f"[last_data] {cp}p 에서 기준점 도달 → 리스트 수집 중단", flush=True)
                    break
            else:
                data.extend(page_items)

        except Exception as e:
            msg = str(e)
            # ← 어떤 셀렉터에서 실패했는지 파싱
            sel_match = re.search(r'locator\("([^"]+)"\)', msg)
            sel_hint = f" [셀렉터: {sel_match.group(1)}]" if sel_match else ""
            print(f"[!] {cp}p 실패 (건너뜀): {msg}", flush=True)
            collect_errors.append({
                "step": f"리스트수집_{cp}p",
                "selector": sel_match.group(1) if sel_match else list_class,
                "error": msg[:300]
            })
        if cp < target and not last_data_reached:
            if not await UniversalCrawler.go_to_page(page, cp + 1, paging_sel, next_btn_sel):
                await _try_url_fallback(page, cp + 1)

    return data, collect_errors, last_data_reached  # ← [신규] last_data_reached 반환

async def _try_url_fallback(page, next_page):
    """페이지 이동 실패 시 URL 파라미터 직접 치환"""
    print(f"[!] {next_page}p 이동 실패, URL 강제 점프 시도", flush=True)
    url = page.url
    new_url = re.sub(PAGE_PARAM_PATTERN, rf'\g<1>={next_page}', url, flags=re.IGNORECASE)
    if new_url != url:
        try:
            await page.goto(new_url, wait_until="domcontentloaded", timeout=3000)
        except Exception as e:
            print(f"[!] URL fallback 실패: {e}", flush=True)

# --- 크롤링 엔진 ---

class UniversalCrawler:

    @staticmethod
    async def apply_filter_and_search(
        page: Page, 
        numpr: str, 
        list_class: str,
        form_sel: str,
        select_sel: str,
        btn_sel: str
    ):
        print(f"[*] 필터 적용 시작 (대수:{numpr})", flush=True)
        filter_errors = []
        try:
            # 1. 특정된 대수 셀렉터로 선택
            # 사용자가 준 select_sel (예: select#th_sch) 내에서 옵션 탐색
            target_select = await page.query_selector(select_sel)
            if target_select and numpr:
                options = await target_select.query_selector_all("option")
                for opt in options:
                    val = (await opt.get_attribute("value") or "").strip()
                    txt = (await opt.inner_text() or "").strip()
                    if val == numpr or val == f"0{numpr}" or f"{numpr}대" in txt:
                        await target_select.select_option(value=val)
                        await target_select.evaluate("node => node.dispatchEvent(new Event('change', {bubbles:true}))")
                        print(f"[+] 대수 선택 완료: {txt}", flush=True)
                        break

            # 2. 특정된 검색 버튼 클릭
            # btn_sel은 "button.btn.blue, #btnSearch" 처럼 쉼표로 여러 개를 받을 수 있음
            try:
                print(f"[*] 검색 버튼 대기 및 클릭: {btn_sel}", flush=True)
                # 버튼이 나타날 때까지 짧게 대기 후 클릭
                btn = await page.wait_for_selector(btn_sel, timeout=3000, state="visible")
                if btn:
                    # ★ 핵심 수정: navigation 여부를 모름 → 분기 처리
                    onclick_val = await btn.get_attribute("onclick") or ""
                    href_val = await btn.get_attribute("href") or ""
                    is_ajax = (
                        href_val in ("#", "", "javascript:void(0)") or
                        "return false" in onclick_val or
                        "loading" in onclick_val.lower() or
                        "ajax" in onclick_val.lower() or
                        "fetch" in onclick_val.lower()
                    )

                    if is_ajax:
                        # AJAX 버튼: navigation 없이 클릭 후 결과 영역 대기
                        print(f"[*] AJAX 버튼 감지 → navigation 없이 클릭", flush=True)
                        await btn.click()
                        # 결과가 list_class 영역에 주입될 때까지 대기
                        await page.wait_for_function(
                            f"""() => {{
                                const el = document.querySelector('{normalize_selector(list_class)} tbody > tr');
                                return el !== null;
                            }}""",
                            timeout=3000
                        )
                    else:
                        # 일반 페이지 이동 버튼
                        async with page.expect_navigation(timeout=3000):
                            await btn.click()

                print("[+] 검색 버튼 클릭 성공", flush=True)
            except Exception as e:
                msg = str(e)
                sel_match = re.search(r'locator\("([^"]+)"\)', msg)
                filter_errors.append({
                    "step": "필터_버튼클릭",
                    "selector": btn_sel,
                    "error": msg[:300]
                })
                print(f"[!] 버튼 클릭 실패, 폼({form_sel}) 직접 제출 시도...", flush=True)
                await page.evaluate(f"document.querySelector('{form_sel}')?.submit()")
                await page.wait_for_load_state("networkidle")

            # 3. 결과 로딩 확인
            if list_class:
                try:
                    await page.wait_for_selector(normalize_selector(list_class), timeout=2000)
                    print("[+] 리스트 로드 완료", flush=True)
                except Exception as e:
                    msg = str(e)
                    sel_match = re.search(r'locator\("([^"]+)"\)', msg)
                    filter_errors.append({
                        "step": "필터_리스트로드",
                        "selector": sel_match.group(1) if sel_match else list_class,
                        "error": msg[:300]
                    })
                    print(f"[!] 리스트 로드 실패: {msg}", flush=True)

        except Exception as e:
            msg = str(e)
            sel_match = re.search(r'locator\("([^"]+)"\)', msg)
            filter_errors.append({
                "step": "필터_전체오류",
                "selector": sel_match.group(1) if sel_match else "",
                "error": msg[:300]
            })
            print(f"[!] 필터 적용 중 오류: {e}", flush=True)

        return filter_errors

    @staticmethod
    def _extract_view_id(href: str, onclick: str, row_html: str, view_id_param: str) -> Optional[str]:
        """3중 ID 추출: href 파라미터 -> row HTML -> JS 함수 인자"""
        clean_href = href.replace("&amp;", "&") if href else ""
        # 0차: path 타입 명시 시 즉시 처리
        if not view_id_param or view_id_param.strip() == "":
            if clean_href and not clean_href.startswith(("javascript", "#")):
                m = re.search(r"/(\d+)(?:[/?#]|$)", clean_href)
                if m: return m.group(1)
        # 1차: href에서 지정 파라미터 추출
        if clean_href and not clean_href.startswith(("javascript", "#")):
            m = re.search(rf"[?&]{re.escape(view_id_param)}=([^&]+)", clean_href)
            if m: return m.group(1)
            m = re.search(VIEW_ID_AUTO_PARAMS, clean_href, re.IGNORECASE)
            if m: return m.group(2)
        # 2차: row HTML 내 파라미터
        m = re.search(rf"[?&]?{re.escape(view_id_param)}=([^&\"'>\s]+)", row_html)
        if m: return m.group(1)
        # 3차: JS 함수 인자값
        js = onclick or (href if href and href.startswith("javascript") else "")
        if js:
            m = re.search(r"\(['\"]?([^'\"),]+)['\"]?\)", js)
            if m: return m.group(1)
        # 4차: onclick HTML 속성 내 함수 인자
        m = re.search(r"onclick\s*=\s*[\"'][a-zA-Z0-9_]+\([\"']([^\"']+)[\"']\)", row_html)
        if m: return m.group(1)
        return None

    @staticmethod
    async def _get_row_link(row, tds) -> dict:
        """행에서 링크 정보(href, onclick, bi_sj) 추출"""
        info = {"href": "", "onclick": "", "bi_sj": ""}
        tr_onclick = await row.get_attribute("onclick") or ""
        a_tag = await row.query_selector("a")
        if a_tag:
            info["href"] = await a_tag.get_attribute("href") or ""
            info["onclick"] = await a_tag.get_attribute("onclick") or tr_onclick
            text = clean_text(await a_tag.inner_text())
            if text: info["bi_sj"] = text
        else:
            info["onclick"] = tr_onclick
            # 서울특별시의회 뷰 상세 링크는 onclick에 goDetail('의안번호', '대수', '회기', '의안종류', '의안번호') 형태로 들어있음
            m = re.search(r"goDetail\s*\(\s*'([^']+)'\s*,\s*'([^']+)'\s*,\s*'([^']+)'\s*,\s*'([^']+)'\s*,\s*'([^']+)'\s*\)", tr_onclick)
            if m:
                prop_type, gen_num, bill_no, bill_type, bill_num = m.groups()
                info["href"] = (
                    f"/info/billRead.do?menuId=006002003"
                    f"&propTypeCd={prop_type}"
                    f"&generationNum={gen_num}"
                    f"&billNo={bill_no}"
                    f"&billTypeCd={bill_type}"
                    f"&billNum={bill_num}"
                )
            for td in tds:
                title = await td.get_attribute("title")
                if title:
                    info["bi_sj"] = clean_text(title)
                    break
        return info

    @staticmethod
    async def extract_list_page(page: Page, list_class: str, view_id_param: str = "code") -> List[Dict[str, Any]]:
        """경량 목록 추출: ID/링크 중심 (상세 진입용)"""
        selector = normalize_selector(list_class)
        await page.wait_for_selector(selector, timeout=3000)

        items = []
        for row in await page.query_selector_all(f"{selector} tbody > tr, {selector} ul > li, {selector} .list_row"):
            tds = await row.query_selector_all("td") or await row.query_selector_all("div, span")
            if not tds: continue
            item = {"row_texts": [clean_text(await td.inner_text()) for td in tds]}
            link = await UniversalCrawler._get_row_link(row, tds)
            if link["bi_sj"]: item["BI_SJ"] = link["bi_sj"]
            item["link_href"] = link["href"]

            vid = UniversalCrawler._extract_view_id(link["href"], link["onclick"], await row.inner_html(), view_id_param)
            if vid: item["view_id"] = vid
            items.append(item)
        return items

    @staticmethod
    async def extract_view_detail(page: Page, view_class: str, base_url: str, req: "ScrapeRequest", bi_cn: str = "") -> Dict[str, Any]:
        selector = normalize_selector(view_class)
        await page.wait_for_selector(selector, timeout=3000)

        result = {}
        section, rs_counter = None, 0
        year = str(datetime.now().year)

        # ── [패턴 D 조기 감지] h4.check 있으면 h4+table 구조 우선 처리 후
        # rows 루프는 중첩 테이블 오염 방지를 위해 건너뜀
        _h4_tables_parsed = await UniversalCrawler._parse_h4_section_tables(
            page, selector, result
        )
        if _h4_tables_parsed:
            # h4+table 구조에서 처리된 경우 rows 루프 skip → 아래 보완 로직만 실행
            rows = []
        else:
            rows = await page.query_selector_all(f"{selector} tbody > tr, {selector} > li, {selector} .view_row")

        for row in rows:
            try:
                if rs_counter > 0: rs_counter -= 1
                if rs_counter == 0: section = None

                ths = await row.query_selector_all("th, dt, strong, .label, .title")
                tds = await row.query_selector_all("td, dd, .value, .cont")
                if not tds:
                    tds = await row.query_selector_all("span")
                pairs = []

                if not ths:
                    # th 없는 경우: td를 라벨/값으로 자동 분류
                    all_tds = await row.query_selector_all("td, dd")
                    if not all_tds:
                        continue

                    label_tds, value_tds = [], []
                    for td in all_tds:
                        cls       = (await td.get_attribute("class") or "").lower()
                        colspan   = await td.get_attribute("colspan")
                        colspan_n = int(colspan) if colspan and colspan.isdigit() else 1
                        bgcolor   = (await td.get_attribute("bgcolor") or "").lower()
                        style     = (await td.get_attribute("style") or "").lower()
                        inner_html = (await td.inner_html()).lower()
                        text      = clean_text(await td.inner_text())

                        is_label = any([
                            any(k in cls for k in ["subject", "tit", "label", "th", "head", "b_item", "header"]),
                            bool(bgcolor) and bgcolor not in ("#ffffff", "white"),
                            "background-color" in style and "#fff" not in style and "white" not in style,
                            colspan_n == 1 and len(text) <= 15 and not re.search(r'\d{4}[-./]\d{2}', text) and bool(text),
                        ])
                        is_value = any([
                            any(k in cls for k in ["con", "value", "cont", "data"]),
                            colspan_n >= 3,
                            "<a " in inner_html,
                            "<ul" in inner_html or "<table" in inner_html,
                        ])

                        if is_label and not is_value:
                            label_tds.append(td)
                        elif is_value and not is_label:
                            value_tds.append(td)
                        elif is_label and is_value:
                            (value_tds if colspan_n >= 3 else label_tds).append(td)
                        else:
                            idx = list(all_tds).index(td)
                            (label_tds if idx % 2 == 0 else value_tds).append(td)

                    if label_tds and value_tds:
                        for lbl_td, val_td in zip(label_tds, value_tds):
                            label_text = clean_text(await lbl_td.inner_text())
                            if label_text:
                                pairs.append((label_text, val_td))
                    elif len(all_tds) == 1:
                        text_content = clean_text(await all_tds[0].inner_text())
                        if text_content:
                            html = await all_tds[0].inner_html()
                            label = "본문내용_첨부파일" if any(k in html.lower() for k in ["down", "첨부", "file"]) else "본문내용"
                            pairs.append((label, all_tds[0]))
                    elif len(all_tds) >= 2:
                        label_text = clean_text(await all_tds[0].inner_text())
                        if label_text:
                            pairs.append((label_text, all_tds[-1]))
                else:
                    # th 있는 경우: subject 클래스 td 제외하고 페어링
                    tds_filtered = []
                    for td in tds:
                        cls = (await td.get_attribute("class") or "")
                        if "subject" not in cls:
                            tds_filtered.append(td)

                    ti, di = 0, 0
                    use_tds = tds_filtered if tds_filtered else tds
                    while ti < len(ths) and di < len(use_tds):
                        rs = await ths[ti].get_attribute("rowspan")
                        th_text = clean_text(await ths[ti].inner_text())

                        if rs and int(rs) > 1 and ti == 0:
                            section, rs_counter = th_text, int(rs)
                            ti += 1
                            if ti >= len(ths): break
                            th_text = clean_text(await ths[ti].inner_text())

                        pairs.append((th_text, use_tds[di]))
                        ti += 1; di += 1

                for label, td_el in pairs:
                    td_html = (await td_el.inner_html()).lower()
 
                    # 중첩 테이블 감지: td 안에 <table>이 있으면 섹션별 재귀 파싱 / 심사경과처럼 소관위원회/본회의 서브테이블이 td 하나에 묶인 구조 대응
                    if "<table" in td_html:
                        await UniversalCrawler._parse_nested_section_tables(
                            td_el, result, section
                        )
                        continue

                    val = clean_text(await td_el.inner_text())
                    is_file = any(x in label for x in ["첨부", "파일", "원문", "의안명", "원안", "수정안", "심사보고서", "공포문", "의안", "보고서"])
                    is_meeting = "회의록" in label

                    if is_file or is_meeting:
                        # ── 첨부파일 중복 수집 방지 ──────────────────────────
                        if is_file and result.get("BI_FILE_NM"):
                            continue

                        year = result.get("ITNC_DE", "")[:4] or str(datetime.now().year)
                        attachments = await UniversalCrawler._extract_attachments(
                            td_el, page, base_url, is_file, req=req, bi_cn=bi_cn, year=year
                        )
                        if is_file and attachments:
                            # 항상 문자열로 직렬화 (단건/다건 통일)
                            names = [a["original_name"] for a in attachments]
                            paths = [a["file_path"]     for a in attachments]
                            ids   = [a["file_id"]       for a in attachments]
                            urls  = [a["url"]           for a in attachments]

                            import json
                            result["BI_FILE_NM"]   = names[0] if len(names) == 1 else json.dumps(names,  ensure_ascii=False)
                            result["BI_FILE_PATH"] = paths[0] if len(paths) == 1 else json.dumps(paths,  ensure_ascii=False)
                            result["BI_FILE_ID"]   = ids[0]   if len(ids)   == 1 else json.dumps(ids,    ensure_ascii=False)
                            result["BI_FILE_URL"]  = urls[0]  if len(urls)  == 1 else json.dumps(urls,   ensure_ascii=False)
                    
                    mapped = get_mapped_key(label, section)

                    if not mapped or not mapped.strip():
                        continue

                    parsed = parse_value(mapped, val)
                    for k, v in parsed.items():
                        if k not in result:
                            result[k] = v
                    if "BI_NO" in parsed:
                        result["BI_NO"] = parsed["BI_NO"]

            except Exception as e:
                print(f"[-] row 파싱 에러: {e}", flush=True)
                continue

        if not result.get("BI_SJ"): # 평창군의회,부산시의회,서울특별시의회,종로구의회 등 한정 기능 (제목 하드 수집)
            for sel in ("table[summary='제목'] td", "div.ViewBoxHead", "th.vision2Tit", "h4.taC", "p.title", "h1.title", "h2.title", "div.view_top h2", ".view_title", ".board_title", "#title", "thead th[colspan]", "div.bbs_vtop h4", "th.text-left.pl-2", "tr.topline th.text-left", ".view_title th"):
                try:
                    el = await page.query_selector(sel)
                    if el:
                        text = clean_text(await el.inner_text())
                        if text:
                            result["BI_SJ"] = text
                            print(f"[+] BI_SJ 수집 실패시 발동 수집: {sel} → {text[:30]}", flush=True)
                            break
                except:
                    continue

        # req에서 받은 값으로 빈 필드 보완
        FALLBACK_FIELDS = {
            "RASMBLY_NUMPR": getattr(req.param, "rasmbly_numpr", None),
        }
        for field, fallback_val in FALLBACK_FIELDS.items():
            if not result.get(field) and fallback_val:
                result[field] = str(fallback_val)

        return result
    
    @staticmethod
    async def _parse_nested_section_tables(container, result: dict, outer_section: Optional[str]):
        """
        td 안에 중첩된 <table> 구조를 레이아웃에 따라 자동 분기.

        [패턴 A] 전라남도의회, 담양군의회 — 섹션헤더: tbody의 th만 있는 행, 데이터: th+td 같은 행
        [패턴 B] 부산광역시의회   — 섹션헤더: 첫 행에 th[rowspan] 2개 이상 (병렬 컬럼)
        [패턴 C] 제주시의회         — 섹션헤더: thead, 데이터: th행(라벨)/td행(값) 교대
        [패턴 D] 경상북도의회
        [패턴 E] 경산시의회 - 내 th 없이 td[rowspan]으로 섹션 구분하는 구조.

        하드코딩 없이 SECTION_FIELD_MAP + FIELD_MAP 위임.
        """
        sub_tables = await container.query_selector_all("table")
        if not sub_tables:
            return

        for tbl in sub_tables:
            # ── 섹션 헤더를 thead에서 먼저 추출 (패턴 C 대응) ─────────────────
            thead_section: Optional[str] = None
            thead_th = await tbl.query_selector("thead th")
            if thead_th:
                t = clean_text(await thead_th.inner_text())
                if t:
                    thead_section = t

            rows = await tbl.query_selector_all("tbody > tr, tr")
            if not rows:
                continue

            # ── 패턴 판별 ─────────────────────────────────────────────────────
            # 첫 행의 th 중 rowspan > 1 이면 패턴 B
            first_row_ths = await rows[0].query_selector_all("th")
            is_pattern_b  = False
            for th in first_row_ths:
                rs = await th.get_attribute("rowspan")
                if rs and int(rs) > 1:
                    is_pattern_b = True
                    break

            # 패턴 C: thead 섹션헤더가 있고, tbody 첫 행이 th만 있는 경우
            first_row_tds = await rows[0].query_selector_all("td")
            is_pattern_c  = bool(thead_section and first_row_ths and not first_row_tds)

            # 패턴 E: th 없이 td[rowspan]으로 섹션 구분 (중첩 테이블 내에서도 발동 가능)
            # 첫 행 all_tds로 _detect_td_rowspan_section 호출
            first_row_all_tds = await rows[0].query_selector_all("td, dd")
            is_pattern_e = bool(
                not first_row_ths
                and await UniversalCrawler._detect_td_rowspan_section(first_row_all_tds)
            )

            if is_pattern_b:
                await UniversalCrawler._parse_rowspan_sections(rows, result)
            elif is_pattern_c:
                await UniversalCrawler._parse_label_value_rows_c(
                    rows, result, thead_section
                )
            elif is_pattern_e:
                await UniversalCrawler._parse_td_rowspan_section_rows(rows, result)
            else:
                await UniversalCrawler._parse_colspan_sections(
                    rows, result, thead_section or outer_section
                )

        print(f"[+] 중첩테이블 파싱 완료 ({len(sub_tables)}개 테이블)", flush=True)

    @staticmethod
    async def _parse_colspan_sections(rows, result: dict, outer_section: Optional[str]):
        """ 패턴 A: th(colspan, td없음)가 섹션 헤더인 구조 파싱 전라남도의회처럼 섹션 헤더가 별도 tr에 있는 경우 """
        sub_section = outer_section
        for row in rows:
            ths = await row.query_selector_all("th")
            tds = await row.query_selector_all("td")

            # 섹션 헤더 행: th만 있고 td 없음
            if ths and not tds:
                header_text = clean_text(await ths[0].inner_text())
                if header_text:
                    sub_section = header_text
                    print(f"[+] 중첩테이블 섹션(A) 감지: {sub_section!r}", flush=True)
                continue

            if not ths or not tds:
                continue

            for th, td in zip(ths, tds):
                label  = clean_text(await th.inner_text())
                val    = clean_text(await td.inner_text())
                if not label:
                    continue
                mapped = get_mapped_key(label, sub_section)
                if mapped == label:
                    continue
                for k, v in parse_value(mapped, val).items():
                    if k not in result or not result[k]:
                        result[k] = v

    @staticmethod
    async def _parse_rowspan_sections(rows, result: dict):
        """ 패턴 B: th[rowspan]이 데이터 td와 같은 tr에 나란히 있는 구조 파싱. """
        sections = []
        cur_section, td_in_section = None, 0
        for cell in await rows[0].query_selector_all("th, td"):
            tag  = await cell.evaluate("el => el.tagName.toLowerCase()")
            text = clean_text(await cell.inner_text())
            rs   = await cell.get_attribute("rowspan")
            if tag == "th" and rs and int(rs) > 1:
                if cur_section is not None:
                    sections.append({"name": cur_section, "td_count": td_in_section})
                cur_section, td_in_section = text, 0
            elif tag == "td":
                td_in_section += 1
        if cur_section is not None:
            sections.append({"name": cur_section, "td_count": td_in_section})

        if not sections:
            return

        print(f"[+] 중첩테이블 섹션(B) 감지: {[s['name'] for s in sections]}", flush=True)

        for row in rows:
            td_texts = [clean_text(await td.inner_text())
                        for td in await row.query_selector_all("td")]
            cursor = 0
            for sec in sections:
                n       = sec["td_count"]
                sec_tds = td_texts[cursor:cursor + n]
                cursor += n
                for i in range(0, len(sec_tds) - 1, 2):
                    label, val = sec_tds[i], sec_tds[i + 1]
                    if not label: continue
                    mapped = get_mapped_key(label, sec["name"])
                    if mapped == label: continue
                    for k, v in parse_value(mapped, val).items():
                        if k not in result or not result[k]:
                            result[k] = v

    @staticmethod
    async def _parse_label_value_rows_c(rows, result: dict, sub_section: Optional[str]):
        """ 패턴 C: thead에 섹션헤더, tbody에 th행(라벨)/td행(값) 교대 구조 파싱. """
        print(f"[+] 중첩테이블 섹션(C): {sub_section!r}", flush=True)
        pending_labels: list = []

        for row in rows:
            ths = await row.query_selector_all("th")
            tds = await row.query_selector_all("td")

            if ths and not tds:
                # 라벨 행: th 텍스트 수집
                pending_labels = [clean_text(await th.inner_text()) for th in ths]
            elif tds and not ths:
                # 값 행: pending 라벨과 zip 매핑
                td_vals = [clean_text(await td.inner_text()) for td in tds]
                for label, val in zip(pending_labels, td_vals):
                    if not label:
                        continue
                    mapped = get_mapped_key(label, sub_section)
                    if mapped == label:
                        continue
                    for k, v in parse_value(mapped, val).items():
                        if k not in result or not result[k]:
                            result[k] = v
                pending_labels = []  # 소비 후 초기화
    
    @staticmethod
    async def _parse_h4_section_tables(page: Page, selector: str, result: dict):
        """ [패턴 D] view 영역 내 <h4 class="check">섹션명</h4> + <table> 반복 구조. """
        try:
            # selector 내에서 탐색 → 없으면 page 전체 fallback
            h4_els = await page.query_selector_all(f"{selector} h4.check")
            if not h4_els:
                h4_els = await page.query_selector_all("h4.check")
            if not h4_els:
                return False
 
            parsed_any = False
            for h4 in h4_els:
                h4_text = clean_text(await h4.inner_text())
                if not h4_text:
                    continue
 
                # h4 다음 형제 table 탐색 (JS nextElementSibling 순회)
                tbl = await h4.evaluate_handle("""el => {
                    let sib = el.nextElementSibling;
                    while (sib) {
                        if (sib.tagName === 'TABLE' || sib.querySelector('table')) return sib;
                        if (['H4','H3','H2'].includes(sib.tagName)) break;
                        sib = sib.nextElementSibling;
                    }
                    return null;
                }""")
 
                if not tbl or await tbl.evaluate("el => el === null"):
                    continue
 
                # thead th → 컬럼명 리스트
                thead_ths = await tbl.query_selector_all("thead th")
                if not thead_ths:
                    continue
                col_names = [clean_text(await th.inner_text()) for th in thead_ths]
 
                # tbody tr → 각 행의 td 값과 컬럼명 zip 매핑
                for tr in await tbl.query_selector_all("tbody tr"):
                    tds = await tr.query_selector_all("td")
                    td_vals = [clean_text(await td.inner_text()) for td in tds]
 
                    for col, val in zip(col_names, td_vals):
                        if not col or not val:
                            continue
                        mapped = get_mapped_key(col, h4_text)
                        if mapped == col:
                            continue
                        for k, v in parse_value(mapped, val).items():
                            if k not in result or not result[k]:
                                result[k] = v
                        parsed_any = True
 
            if parsed_any:
                print(f"[+] 패턴D(h4+table) 파싱 완료", flush=True)
            return parsed_any
 
        except Exception as e:
            print(f"[-] 패턴D 파싱 오류: {e}", flush=True)
            return False
        
    @staticmethod
    async def _parse_td_rowspan_section_rows(rows, result: dict):
        """
        [패턴 E] 중첩 테이블 내 th 없이 td[rowspan]으로 섹션 구분하는 구조.
        _detect_td_rowspan_section()으로 감지 후 모든 행 순회.
        발동 조건은 _detect_td_rowspan_section()이 보장하므로 여기선 파싱만 담당.
        """
        cur_section: Optional[str] = None
        rs_counter  = 0
 
        for row in rows:
            all_tds = await row.query_selector_all("td, dd")
            if not all_tds:
                continue
 
            if rs_counter > 0:
                rs_counter -= 1
 
            # 패턴 E 헤더 행 감지
            e_result = await UniversalCrawler._detect_td_rowspan_section(all_tds)
            if e_result:
                cur_section = e_result["section"]
                rs_counter  = e_result["rs_counter"]
                # 이 행의 라벨/값도 처리
                label, val_el = e_result["label"], e_result["val_el"]
                if label:
                    val = clean_text(await val_el.inner_text())
                    mapped = get_mapped_key(label, cur_section)
                    if mapped != label:
                        for k, v in parse_value(mapped, val).items():
                            if k not in result or not result[k]:
                                result[k] = v
                continue
 
            # 일반 데이터 행: all_tds = [라벨td, 값td]
            if len(all_tds) >= 2 and cur_section:
                label = clean_text(await all_tds[0].inner_text())
                val   = clean_text(await all_tds[-1].inner_text())
                if label:
                    mapped = get_mapped_key(label, cur_section)
                    if mapped != label:
                        for k, v in parse_value(mapped, val).items():
                            if k not in result or not result[k]:
                                result[k] = v

    @staticmethod
    async def _extract_attachments(td_el, page: Page, base_url: str, is_file: bool, req: "ScrapeRequest" = None, bi_cn: str = "", year: str = "") -> list:
        """첨부파일 BILL_ATTACHMENT 테이블용 정보 추출"""
        os.makedirs(FILE_DOWNLOAD_DIR, exist_ok=True)
        attachments = []
        seq = 0

        elements = await td_el.query_selector_all("a, span[onclick], [style*='cursor: pointer']")

        for el in elements:
            raw = clean_text(await el.inner_text())
            title_el = await el.query_selector("[title]")
            title = clean_text(await title_el.get_attribute("title")) if title_el else ""
            if not title:
                title = clean_text(await el.inner_text()) or ""

            skip_keywords = ["바로보기", "바로듣기", "미리보기", "뷰어", "첨부파일명, 미리보기 새창으로 이동", "바로보기"]
            if is_file and (any(k in raw for k in skip_keywords) or any(k in title for k in skip_keywords)):
                continue
            if not raw:
                continue

            href = await el.get_attribute("href") or ""
            onclick = await el.get_attribute("onclick") or ""
            is_js = href.startswith(("javascript", "#")) or (onclick and not href)
            url_val = onclick if is_js else (href or onclick)

            original_name = raw  # ★ 원본명 미리 보관
            save_name = raw
            file_path = ""

            if is_file:
                print(f"[*] 다운로드 시도: {raw}", flush=True)
                try:
                    await el.evaluate("node => { if(node.tagName === 'A') node.removeAttribute('target'); }")

                    async with page.expect_download(timeout=10000) as dl_info:
                        await el.click()

                    download = await dl_info.value

                    suggested_filename = download.suggested_filename or ""
                    _, ext = os.path.splitext(suggested_filename)
                    if not ext:
                        ext = ".bin"

                    # ★ 원본명: suggested_filename 우선, 없으면 화면 텍스트
                    original_name = title if title else raw

                    seq += 1
                    if req and bi_cn:
                        save_path = build_save_path(req, year, bi_cn, seq, ext)
                    else:
                        os.makedirs(FILE_DOWNLOAD_DIR, exist_ok=True)
                        save_path = os.path.join(FILE_DOWNLOAD_DIR, f"CLIKC{str(time.time_ns())[:16]}_{seq}{ext}")

                    await download.save_as(save_path)
                    print(f"[+] 다운로드 완료: {save_path}", flush=True)

                    save_name = os.path.basename(save_path).replace("\\", "/")   # ★ CLIKC123_1.hwp
                    file_path = save_path.replace("\\", "/")                      # ★ 전체 경로
                    url_val   = download.url

                except Exception as e:
                    print(f"[-] 다운로드 건너뜀 ({raw}): {str(e)[:100]}", flush=True)
                    seq += 1  # 실패해도 seq 증가 (순서 일관성)
                    if not url_val.startswith("http") and url_val:
                        url_val = urljoin(base_url, url_val)
                    if page.url != base_url:
                        try:
                            await page.goto(base_url, wait_until="domcontentloaded", timeout=3000)
                        except:
                            pass
            else:
                seq += 1

            attachments.append({
                "original_name": original_name,
                "save_name":     save_name,
                "file_path":     file_path,
                "file_id":       str(seq),
                "url":           url_val,
            })

        return attachments

    @staticmethod
    async def get_total_pages(page: Page, end_btn_selector: str = None) -> int:
        try:
            ex_selectors = [
                "a.last", "a.num_last", "a.btn-last", "a.direction.last",
                "a.btn.end", "a.btn.next", "a.next",
                "a[title*='마지막']", "a[onclick*='Retrieve']", 
                "a.l_font", "a:has-text('»')", "a:has-text('>>')"
            ]

            if end_btn_selector:
                # 리스트 중복 방지 및 사용자 셀렉터 우선
                unique_selectors = [end_btn_selector] + [s for s in ex_selectors if s != end_btn_selector]
            else:
                unique_selectors = ex_selectors
            
            btn_candidates = await page.query_selector_all(", ".join(unique_selectors))
            
            for btn in reversed(btn_candidates):
                href = await btn.get_attribute("href") or ""
                onclick = await btn.get_attribute("onclick") or ""
                text = await btn.inner_text() or ""
                
                combined = f"{href} {onclick} {text}"
                
                m = re.search(r'(?:fn[a-zA-Z_]*|pageIndex|pageNum|pageNo|page|go|move|schPageNo|cp)\s*[\(=]\s*[\'"]?(\d+)[\'"]?', combined, re.IGNORECASE)
                
                if m:
                    total = int(m.group(1))
                    if total > 1: return total

            # 2. 버튼으로 못 찾았을 경우, 현재 보이는 숫자 중 최대값 (Fallback)
            mx = 1
            paging_links = await page.query_selector_all(".paging a, .paging2 a, .pagination a, #pagingNav a, .paging strong")
            for b in paging_links:
                t = (await b.inner_text()).strip()
                if t.isdigit():
                    mx = max(mx, int(t))
            return mx

        except Exception as e:
            print(f"[-] 페이지 수 탐지 실패 (기본 1): {e}", flush=True)
            return 1

    @staticmethod
    async def go_to_page(page: Page, next_page: int, paging_sel: str, next_btn_sel: str) -> bool:
        """
        다음 페이지 이동 로직 (태그 제약 제거 버전)
        """
        # 1. 셀렉터 정규화 (.num_right -> .num_right 그대로 유지)
        p_sel = normalize_selector(paging_sel)
        n_sel = normalize_selector(next_btn_sel)

        # 전자정부 JS 우선 처리
        try:
            if await page.evaluate("typeof fn_egov_link_page === 'function'"):
                await page.evaluate(f"fn_egov_link_page({next_page});")
                await page.wait_for_load_state("domcontentloaded")
                return True
        except: pass

        try:
            # [수정] p_sel 영역 내부의 '텍스트가 해당 숫자인' 요소를 찾음 (a 태그 제약 제거)
            link = page.locator(p_sel).get_by_text(re.compile(f"^{next_page}$"), exact=True).first
            
            if await link.count() > 0:
                await link.click()
                try:
                    await page.wait_for_function(
                        "() => document.querySelectorAll('tbody#searchList tr').length > 0",
                        timeout=5000
                    )
                except:
                    await page.wait_for_timeout(1000)  # fallback
                return True

            # [수정] 다음 버튼 역시 a 태그 제약 없이 n_sel 그 자체를 클릭
            nxt = page.locator(n_sel).first
            if await nxt.count() > 0:
                print(f"[*] 다음 블록 클릭 (Selector: {n_sel})", flush=True)
                await nxt.click()
                await page.wait_for_load_state("domcontentloaded")
                await page.wait_for_timeout(500) 
                return True
        except Exception as e:
            print(f"[!] 페이지 이동 실패: {e}", flush=True)
        
        return False

# --- 공통 실행 엔진 (Service Layer) ---
async def execute_view_scraping(req: ScrapeRequest):
    app.state.stop_scraping = False
    p = req.param
    domain = extract_domain(p.list_url)
    list_data, view_data = [], []
    filepath = None
    error_logs = []
    field_logs: List[dict] = []

    # last_data 가 없으면 None → 기존 전체 수집 동작 그대로 유지
    last_sig: Optional[Dict[str, str]] = (
        _build_last_data_signature(req.last_data) if req.last_data else None
    )
    if last_sig:
        print(f"[last_data] 추가수집 모드 활성화. 기준점: {last_sig}", flush=True)
    
    async with async_playwright() as playwright:
        browser, page = await _setup_browser(playwright)
        try:
            # 1단계: 리스트 수집
            print(f"\n{'='*60}", flush=True)
            print(f"[*] [1단계] 리스트 수집 시작: {p.list_url}", flush=True)
            list_data, collect_errors, last_data_reached = await _collect_pages(
                page, p.list_url, p.rasmbly_numpr, p.list_class, p.view_id_param,
                p.max_pages, p.paging_selector, p.next_btn_selector, p.end_btn_selector,
                UniversalCrawler.extract_list_page, lambda: app.state.stop_scraping,
                p.search_form_selector, p.numpr_select_selector, p.search_btn_selector, last_sig=last_sig, 
            )
            error_logs.extend(collect_errors)

            # ← 리스트 수집 실패 감지
            if not list_data:
                error_logs.append({
                    "step": "1단계_리스트수집",
                    "url": p.list_url,
                    "selector": p.list_class,
                    "error": "리스트 수집 결과 0건"
                })

            # 2단계: 상세 뷰 수집 루프
            total = len(list_data)
            print(f"\n[*] [2단계] 상세 수집 시작 (총 {total}건)", flush=True)
            print(f"{'-'*60}", flush=True)

            detail_last_data_reached = False  # ← [신규] 상세 단계 중단 플래그

            for idx, item in enumerate(list_data):
                # [/stop 요청 감지]
                if app.state.stop_scraping:
                    print(f"\n[!] 중단 요청 감지: {idx}번째에서 상세 수집을 중단합니다.", flush=True)
                    break # 루프를 탈출하여 하단의 저장/전송 로직으로 이동

                vid = item.get("view_id")
                if not vid: continue

                print(f"[*] 상세 ({idx+1}/{total}) ID: {vid}", flush=True)

                href = item.get("link_href", "")
                is_real = href and not href.startswith(("#", "javascript"))
                
                target_url = urljoin(p.list_url, href) if is_real else f"{p.view_url}{'&' if '?' in p.view_url else '?'}{p.view_id_param}={vid}"

                try:
                    await page.goto(target_url, wait_until="domcontentloaded", timeout=5000)
                    
                    parsed = urlparse(target_url)
                    base = f"{parsed.scheme}://{parsed.netloc}"
                    
                    bi_cn  = str(str(time.time_ns())[:16])
                    detail = await UniversalCrawler.extract_view_detail(page, p.view_class, base, req=req, bi_cn=bi_cn)
                    collected_item = {"view_id": vid, "URL": target_url, "BI_CN": f"CLIKC{bi_cn}", **detail}

                    # ── 필드 감사 로그 ──────────────────────────────────────────
                    field_logs.append(audit_fields(vid, target_url, f"CLIKC{bi_cn}", collected_item))

                    # ── [신규] 상세 데이터 기준 last_data 비교 ────────────────
                    # 리스트 단계에서 걸러지지 않은 경우의 보조 안전망
                    if last_sig and not last_data_reached and is_last_data_match(collected_item, last_sig):
                        detail_last_data_reached = True
                        print(f"[last_data] 상세 단계에서 기준점 도달 → 수집 중단 후 전송", flush=True)
                        break
                    # ────────────────────────────────────────────────────────

                    view_data.append(collected_item)
                    
                except Exception as e:
                    print(f"    [!] ID: {vid} 수집 실패: {e}", flush=True)
                    view_data.append({"view_id": vid, "URL": target_url, "view_error": str(e)})

                    error_logs.append({  # ← 추가
                        "step": "2단계_상세수집",
                        "view_id": vid,
                        "url": target_url,
                        "error": str(e)
                    })

            is_interrupted = (
                app.state.stop_scraping
                or last_data_reached
                or detail_last_data_reached
            )
            # ────────────────────────────────────────────────────────────────

            result_block = _build_result(view_data, error_logs, is_interrupted)

            # ── [신규] last_data 모드일 때 result message 보완 ───────────────
            if (last_data_reached or detail_last_data_reached) and result_block["status"] in ("SUCCESS", "PARTIAL"):
                result_block["message"] = "추가수집 완료 (last_data 기준점 도달)"

            # ── field_logs 저장 (insert_api 전송 제외) ─────────────────────
            if field_logs:
                save_field_logs(field_logs, req)

            full_payload = {
                    "reqId":   req.req_id,
                    "type":    req.type,
                    "crwId":   req.crw_id,
                    "fileDir": req.file_dir,
                    "result":  result_block,
                    "data":    view_data,
                    "log":     error_logs  # ← 에러 로그 포함
                }
            
            view_data.reverse()
            # --- 루프 종료 후 공통 처리 (정상 종료 또는 중단 시 모두 실행) ---
            if view_data or error_logs:
                if view_data:
                    filepath = save_to_json(full_payload, domain, req.type)
                    print(f"[OK] 데이터 저장 완료 ({len(view_data)}건): {filepath}", flush=True)
                else:
                    filepath = save_to_json(full_payload, domain, req.type+"_error")
                    print(f"[!] 수집 데이터 없음, 에러 로그 {len(error_logs)}건 저장: {filepath}", flush=True)

                print(f"[*] [3단계] 데이터 전송 시도...", flush=True)
                await send_to_insert_api(
                    req_id=req.req_id,
                    type_val=req.type,
                    crw_id=req.crw_id,
                    file_dir=req.file_dir,
                    result=result_block,
                    data_list=view_data,
                    error_logs=error_logs  # ← 추가
                )
            else:
                print(f"[!] 수집된 데이터가 없어 전송을 생략합니다.", flush=True)

            return {
                "req_id": req.req_id, 
                "type": req.type, 
                "crw_id": req.crw_id, 
                "file_dir": req.file_dir,
                "ok": True, 
                "interrupted": is_interrupted,
                "last_data_reached": last_data_reached or detail_last_data_reached,  # ← [신규]
                "data_count": len(view_data), 
                "saved_file": filepath
            }

        except Exception as e:
            print(f"\n[!] 상세 수집 전체 에러: {e}", flush=True)
            return {"req_id": req.req_id, "type": req.type, "crw_id": req.crw_id, "file_dir": req.file_dir, "ok": False, "error_msg": str(e)}
        finally:
            await browser.close()

async def send_to_insert_api(req_id: str, type_val: str, crw_id: str, file_dir: str, result: dict, data_list: list, error_logs: list = None):
    target_url = "http://10.201.38.157:8080/insert_api.do"
    # target_url = "http://211.219.26.15:18120/insert_api.do"
    
    payload = {
        "reqId": req_id,
        "type": type_val,
        "crwId": crw_id,
        "fileDir": file_dir,
        "result": result,
        "data": data_list,
        "log": error_logs or []
    }

    print(f"\n[*] [3단계] 데이터 전송 시도 (JSON 방식)", flush=True)
    
    # 즉시 응답 후 백그라운드에서 전송
    await _do_send(target_url, payload)
    print(f"[OK] {target_url} -> 전송 접수완료 (백그라운드 처리 중)", flush=True)
    return True

async def _do_send(target_url: str, payload: dict):
    async with httpx.AsyncClient() as client:
        try:
            response = await client.post(target_url, json=payload, timeout=120.0)
            
            if response.status_code == 200:
                print(f"[OK] API 전송 성공", flush=True)
            else:
                print(f"[!] {target_url} 전송 완료", flush=True)
        except Exception as e:
            print(f"[!] 네트워크 오류: {str(e)}", flush=True)
        
async def handle_scraping_request(req: ScrapeRequest, background_tasks: BackgroundTasks):
    try:
        # 실제 무거운 작업은 백그라운드 태스크로 등록
        background_tasks.add_task(execute_view_scraping, req)

        # 즉시 응답 반환
        return {
            "req_id": req.req_id,
            "type": req.type,
            "crw_id": req.crw_id,
            "file_dir": req.file_dir,
            "ok": True,
            "message": "수집 요청 완료"
        }
    except Exception as e:
        return error_response(f"요청 처리 중 오류 발생: {str(e)}")
    
# 2026.04.09 - 이성진 코드 추가
# test용으로 1건만 수집하는 함수
async def execute_view_scraping_test(req: ScrapeRequest) -> dict:
    """테스트용: 1건만 수집하여 insert_api.do 동일 형식 payload 반환"""
    p = req.param
    view_data = []

    async with async_playwright() as playwright:
        browser, page = await _setup_browser(playwright)
        try:
            # 1단계: 리스트 1페이지만 수집
            print(f"[TEST] 리스트 수집: {p.list_url}", flush=True)
            await page.goto(p.list_url, wait_until="domcontentloaded", timeout=3000)

            if p.rasmbly_numpr and p.rasmbly_numpr.strip():
                await UniversalCrawler.apply_filter_and_search(
                    page, p.rasmbly_numpr.strip(), p.list_class,
                    p.search_form_selector, p.numpr_select_selector, p.search_btn_selector,
                )
            else:
                await page.wait_for_selector(normalize_selector(p.list_class), timeout=3000)

            list_data = await UniversalCrawler.extract_list_page(page, p.list_class, p.view_id_param)

            if not list_data:
                return {
                    "req_id": req.req_id,
                    "type": req.type,
                    "crw_id": req.crw_id,
                    "file_dir": req.file_dir,
                    "data": [],
                }

            # 2단계: 첫 1건만 상세 수집
            item = list_data[0]
            vid = item.get("view_id")

            if vid:
                href = item.get("link_href", "")
                is_real = href and not href.startswith(("#", "javascript"))
                target_url = urljoin(p.list_url, href) if is_real else f"{p.view_url}{'&' if '?' in p.view_url else '?'}{p.view_id_param}={vid}"

                print(f"[TEST] 상세 수집: {vid}", flush=True)

                try:
                    await page.goto(target_url, wait_until="domcontentloaded", timeout=3000)
                    parsed_url = urlparse(target_url)
                    base = f"{parsed_url.scheme}://{parsed_url.netloc}"
                    bi_cn  = str(str(time.time_ns())[:16])
                    detail = await UniversalCrawler.extract_view_detail(page, p.view_class, base, req=req, bi_cn=bi_cn)
                    print(f"[TEST] 상세 수집 완료: {vid} / BI_CN: CLIKC{bi_cn}", flush=True)
                    view_data.append({"view_id": vid, "view_url": target_url, "BI_CN": f"CLIKC{bi_cn}", **detail})
                except Exception as e:
                    print(f"[TEST] 상세 수집 실패: {e}", flush=True)
                    view_data.append({"view_id": vid, "view_url": target_url, "view_error": str(e)})

            view_data.reverse()
            return {
                "req_id": req.req_id,
                "type": req.type,
                "crw_id": req.crw_id,
                "file_dir": req.file_dir,
                "data": view_data,
            }

        except Exception as e:
            print(f"[TEST] 에러: {e}", flush=True)
            return {
                "req_id": req.req_id,
                "type": req.type,
                "crw_id": req.crw_id,
                "file_dir": req.file_dir,
                "data": [],
            }
        finally:
            await browser.close()

    
# 공통 테스트 처리 로직
async def handle_test_request(req: ScrapeRequest):
    return {
        "req_id": req.req_id,
        "type": req.type,
        "crw_id": req.crw_id,
        "file_dir": req.file_dir,
        "ok": True
    }