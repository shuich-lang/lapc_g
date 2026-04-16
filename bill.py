
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
# FILE_DOWNLOAD_DIR = "/clicker-apps/diquest/fileDown/tempDir"

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
_P_HYPHEN_NUMPR_SESN = re.compile(r'(\d+)\s*대\s*[-–—]\s*(\d+)\s*회')  # "9대-287회"
# 날짜 형식
_DATE_PATTERN = re.compile(r'(\d{4})[./\-](\d{1,2})[./\-](\d{1,2})')

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
        sec_key = "위원회" if "위원회" in section else ("본회의" if "본회의" in section else section)
        for mk, mv in SECTION_FIELD_MAP.get(sec_key, {}).items():
            if mk.replace(" ", "") == normalized:
                return mv
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
    
# --- 브라우저 / 페이지네이션 헬퍼 ---

async def _setup_browser(pw):
    """헤드리스 브라우저 생성 + 불필요 리소스 차단"""
    browser = await pw.chromium.launch(headless=True)
    page = await browser.new_page()
    await page.route("**/*", lambda r: r.abort() if r.request.resource_type in BLOCKED_RESOURCES else r.continue_())
    return browser, page

async def _collect_pages(page, list_url, numpr, list_class, vid_param,
                         max_pages, paging_sel, next_btn_sel, end_btn_sel, extractor, stop_check, search_form_selector, numpr_select_selector, search_btn_selector):
    """공통 리스트 수집 루프 (필터 + 페이지네이션)"""
    collect_errors = []  # ← 추가

    await page.goto(list_url, wait_until="domcontentloaded", timeout=30000)
    
    if numpr and numpr.strip():
        # 검색 후 list_class가 나타날 때까지 기다리도록 인자 추가
        filter_errors = await UniversalCrawler.apply_filter_and_search(page, numpr.strip(), list_class, search_form_selector, numpr_select_selector, search_btn_selector)
        collect_errors.extend(filter_errors)
    else:
        # 검색 안 할 때도 리스트는 기다려야 함
        await page.wait_for_selector(normalize_selector(list_class), timeout=10000)

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
            data.extend(await extractor(page, list_class, vid_param))
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
        if cp < target and not await UniversalCrawler.go_to_page(page, cp + 1, paging_sel, next_btn_sel):
            await _try_url_fallback(page, cp + 1)
    return data, collect_errors

async def _try_url_fallback(page, next_page):
    """페이지 이동 실패 시 URL 파라미터 직접 치환"""
    print(f"[!] {next_page}p 이동 실패, URL 강제 점프 시도", flush=True)
    url = page.url
    new_url = re.sub(PAGE_PARAM_PATTERN, rf'\g<1>={next_page}', url, flags=re.IGNORECASE)
    if new_url != url:
        try:
            await page.goto(new_url, wait_until="domcontentloaded", timeout=30000)
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
                btn = await page.wait_for_selector(btn_sel, timeout=5000, state="visible")
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
                                const el = document.querySelector('{normalize_selector(list_class)} tbody tr');
                                return el !== null;
                            }}""",
                            timeout=15000
                        )
                    else:
                        # 일반 페이지 이동 버튼
                        async with page.expect_navigation(timeout=10000):
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
                    await page.wait_for_selector(normalize_selector(list_class), timeout=10000)
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
        await page.wait_for_selector(selector, timeout=10000)

        items = []
        for row in await page.query_selector_all(f"{selector} tbody tr, {selector} ul > li, {selector} .list_row"):
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
        await page.wait_for_selector(selector, timeout=10000)

        result = {}
        section, rs_counter = None, 0
        year = str(datetime.now().year)

        rows = await page.query_selector_all(f"{selector} tbody tr, {selector} > li, {selector} .view_row")

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
                    val = clean_text(await td_el.inner_text())
                    is_file = any(x in label for x in ["첨부", "파일", "원문", "의안명", "원안", "수정안", "심사보고서", "공포문"])
                    is_meeting = "회의록" in label

                    if is_file or is_meeting:
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
                for sel in ("table[summary='제목'] td", "div.ViewBoxHead", "th.vision2Tit", "h4.taC", "p.title", "h1.title", "h2.title", "div.view_top h2", ".view_title", ".board_title", "#title", "thead th[colspan]", "div.bbs_vtop h4"):
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

            skip_keywords = ["바로보기", "바로듣기", "미리보기", "뷰어"]
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

                    async with page.expect_download(timeout=15000) as dl_info:
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
                            await page.goto(base_url, wait_until="domcontentloaded", timeout=5000)
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
                        timeout=10000
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
    
    async with async_playwright() as playwright:
        browser, page = await _setup_browser(playwright)
        try:
            # 1단계: 리스트 수집
            print(f"\n{'='*60}", flush=True)
            print(f"[*] [1단계] 리스트 수집 시작: {p.list_url}", flush=True)
            list_data, collect_errors = await _collect_pages(
                page, p.list_url, p.rasmbly_numpr, p.list_class, p.view_id_param,
                p.max_pages, p.paging_selector, p.next_btn_selector, p.end_btn_selector,
                UniversalCrawler.extract_list_page, lambda: app.state.stop_scraping,
                p.search_form_selector, p.numpr_select_selector, p.search_btn_selector,
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
                    await page.goto(target_url, wait_until="domcontentloaded", timeout=15000)
                    
                    parsed = urlparse(target_url)
                    base = f"{parsed.scheme}://{parsed.netloc}"
                    
                    bi_cn  = str(str(time.time_ns())[:16])
                    detail = await UniversalCrawler.extract_view_detail(page, p.view_class, base, req=req, bi_cn=bi_cn)
                    view_data.append({"view_id": vid, "URL": target_url, "BI_CN": f"CLIKC{bi_cn}", **detail})
                    
                except Exception as e:
                    print(f"    [!] ID: {vid} 수집 실패: {e}", flush=True)
                    view_data.append({"view_id": vid, "URL": target_url, "view_error": str(e)})

                    error_logs.append({  # ← 추가
                        "step": "2단계_상세수집",
                        "view_id": vid,
                        "url": target_url,
                        "error": str(e)
                    })

            result_block = _build_result(view_data, error_logs, app.state.stop_scraping)

            full_payload = {
                    "reqId":   req.req_id,
                    "type":    req.type,
                    "crwId":   req.crw_id,
                    "fileDir": req.file_dir,
                    "result":  result_block,
                    "data":    view_data,
                    "log":     error_logs  # ← 에러 로그 포함
                }
            
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
                "interrupted": app.state.stop_scraping,
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
            await page.goto(p.list_url, wait_until="domcontentloaded", timeout=30000)

            if p.rasmbly_numpr and p.rasmbly_numpr.strip():
                await UniversalCrawler.apply_filter_and_search(
                    page, p.rasmbly_numpr.strip(), p.list_class,
                    p.search_form_selector, p.numpr_select_selector, p.search_btn_selector,
                )
            else:
                await page.wait_for_selector(normalize_selector(p.list_class), timeout=10000)

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
                    await page.goto(target_url, wait_until="domcontentloaded", timeout=15000)
                    parsed_url = urlparse(target_url)
                    base = f"{parsed_url.scheme}://{parsed_url.netloc}"
                    bi_cn  = str(str(time.time_ns())[:16])
                    detail = await UniversalCrawler.extract_view_detail(page, p.view_class, base, req=req, bi_cn=bi_cn)
                    print(f"[TEST] 상세 수집 완료: {vid} / BI_CN: CLIKC{bi_cn}", flush=True)
                    view_data.append({"view_id": vid, "view_url": target_url, "BI_CN": f"CLIKC{bi_cn}", **detail})
                except Exception as e:
                    print(f"[TEST] 상세 수집 실패: {e}", flush=True)
                    view_data.append({"view_id": vid, "view_url": target_url, "view_error": str(e)})

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
    
# --- API 엔드포인트 ---
@app.get("/crawl/stop")
async def api_stop():
    """크롤링 루프 즉시 중단"""
    app.state.stop_scraping = True
    print("[!] 외부 중단 요청 수신", flush=True)
    return {"ok": True, "message": "Stop requested. Current process will halt and save progress."} 