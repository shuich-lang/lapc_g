from __future__ import annotations
import asyncio, os, re, sys, time, json
from datetime import datetime
from typing import Any, Dict, List, Optional
from urllib.parse import parse_qsl, urlencode, urljoin, urlparse, urlunparse, unquote
import certifi, httpx
from bs4 import BeautifulSoup
from fastapi import FastAPI, BackgroundTasks, HTTPException
from fastapi.responses import JSONResponse
from playwright.async_api import async_playwright, Page, TimeoutError as PlaywrightTimeoutError
from pydantic import BaseModel, Field, HttpUrl, field_validator, ValidationInfo
import json as _json

if sys.platform.startswith("win"):
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

app = FastAPI(title="Council Crawler API")
app.state.stop_scraping = False

DOWNLOAD_DIR, FILE_DOWNLOAD_DIR, FIELD_LOGS_DIR = "download", "attachment", "field_logs"
#INSERT_API_URL = "http://10.201.38.157:8080/insert_api.do"
INSERT_API_URL = "http://172.17.0.1:18123/insert_api.do"
#INSERT_API_URL = "http://211.219.26.15:18123/insert_api.do"	
USER_AGENT = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
              "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36")
BLOCKED_RESOURCES     = {"image", "stylesheet", "media", "font"}
VIEW_ID_AUTO_PARAMS   = r"[?&](uid|idx|code|no|seq|id|bill_no|billNo|idx_no|nttId|uuid)=([^&]+)"
PAGE_PARAM_PATTERN    = r'([?&](?:page|pageIndex|p|page_no|pageno|cPage|pageNum|page_id|cp))=(\d+)'
FILE_EXTENSIONS       = ("pdf", "hwp", "hwpx", "doc", "docx", "xls", "xlsx", "zip")
_MAX_CONSECUTIVE_FAIL = 5
_DATE_PATTERN         = re.compile(r'(\d{4})[./\-](\d{1,2})[./\-](\d{1,2})')
_FILE_SKIP_COLS   = {"BI_FILE_NM", "BI_FILE_URL", "BI_FILE_PATH", "BI_FILE_ID"}
_LAST_DATA_MATCH_KEYS = ["URL", "BI_SJ", "BI_CN", "BI_NO"]
_POLICY_FILE_COLS = {"ORG_FILE_NM", "DOWNPATH", "DOWNID", "DOWNURL"}
MAX_FILE_SIZE_BYTES   = 20 * 1024 * 1024   # 20MB
_PRISM_FILE_COLS  = {"ORIGNL_FILE_NM", "SYS_FILE_NM", "FILE_SEQ", "RPRT_TYPE"}

# ── BI_NO / RASMBLY_NUMPR_SESN 파서 ──────────────────────────────
_P_BI_NO_SESN        = re.compile(r'^(.+?)\s*[\(\（]제\s*(\d+)\s*회[\)\）]')
_P_NUMPR_SESN        = re.compile(r'제\s*(\d+)\s*대.*?제\s*(\d+)\s*회')
_P_NUMPR_ONLY        = re.compile(r'제\s*(\d+)\s*대')
_P_SESN_ONLY         = re.compile(r'제\s*(\d+)\s*회')
_P_DIGIT_ONLY        = re.compile(r'^\d+$')
_P_SLASH_NUMPR_SESN  = re.compile(r'(\d+)\s*대\s*/\s*제\s*(\d+)\s*회')
_P_HYPHEN_NUMPR_SESN = re.compile(r'(\d+)\s*대\s*[-–—]\s*(\d+)\s*회')
_P_DAE_HOE           = re.compile(r'(\d+)\s*대\s*(\d+)\s*회')

# ── 모델 ─────────────────────────────────────────────────────────
class RegexItem(BaseModel):
    col:        str             = Field(...)
    regex:      List[str]       = Field(default_factory=list)
    xpath:      Optional[List[str]] = None
    removeTags: str             = Field("Y")
    value:      Optional[str]   = None

class ScrapeParam(BaseModel):
    list_url:              str           = Field(...)
    view_url:              Optional[str] = None
    view_id_param:         str           = Field("uuid")
    rasmbly_numpr:         str           = Field("")
    list_class:            str           = Field("table.board_list")
    view_class:            Optional[str] = None
    max_pages:             str           = Field("")
    paging_selector:       str           = Field("div#pagingNav")
    next_btn_selector:     str           = Field("a.num_right")
    end_btn_selector:      str           = Field("a.num_last")
    search_form_selector:  str           = Field("form#search_form")
    numpr_select_selector: str           = Field("select#th_sch")
    search_btn_selector:   str           = Field("button.btn.blue")
    timeout:               str           = Field("20000")
    @field_validator("timeout", mode="before")
    @classmethod
    def default_timeout(cls, v):
        if v is None or str(v).strip() == "": return "20000"
        return str(v)

class PolicyParam(BaseModel):
    list_url:              str           = Field(...)
    view_url:              Optional[str] = None
    view_id_param:         str           = Field("uuid")
    list_class:            str           = Field("table.board_list")
    view_class:            Optional[str] = None
    max_pages:             str           = Field("")
    paging_selector:       str           = Field("div#pagingNav")
    next_btn_selector:     str           = Field("a.num_right")
    end_btn_selector:      str           = Field("a.num_last")
    timeout:               str           = Field("20000")
    @field_validator("timeout", mode="before")
    @classmethod
    def default_timeout(cls, v):
        if v is None or str(v).strip() == "": return "20000"
        return str(v)

class PrismParam(BaseModel):
    list_url:       str           = Field(...)
    list_api_url:   Optional[str] = Field(None)
    detail_api_url: str           = Field(...)
    max_pages:      str           = Field("")
    timeout:        str           = Field("20000")
    @field_validator("timeout", mode="before")
    @classmethod
    def default_timeout(cls, v):
        if v is None or str(v).strip() == "": return "20000"
        return str(v)

class LastData(BaseModel):
    model_config = {"extra": "allow"}
    URL:   Optional[str] = None
    BI_SJ: Optional[str] = None
    BI_CN: Optional[str] = None
    BI_NO: Optional[str] = None

class UnifiedRequest(BaseModel):
    model_config = {"extra": "allow"}
    req_id:    str             = Field(..., min_length=1)
    type:      str             = Field(..., min_length=1)
    crw_id:    str             = Field(..., min_length=1)
    file_dir:  str             = Field(...)
    param:     dict            = Field(...)
    item:      List[RegexItem] = Field(default_factory=list)
    last_data: Optional[dict]  = None
    @field_validator("req_id","type","crw_id","file_dir")
    @classmethod
    def not_empty(cls, v, info: ValidationInfo):
        if not v or not v.strip(): raise ValueError(f"[{info.field_name}] 필수 파라미터가 비어있습니다.")
        return v

class ScrapeRequest(BaseModel):
    req_id:    str             = Field(..., min_length=1)
    type:      str             = Field(..., min_length=1)
    crw_id:    str             = Field(..., min_length=1)
    file_dir:  str             = Field(...)
    param:     ScrapeParam     = Field(...)
    item:      List[RegexItem] = Field(default_factory=list)
    last_data: Optional[LastData] = None

class PolicyRequest(BaseModel):
    req_id:    str             = Field(..., min_length=1)
    type:      str             = Field(..., min_length=1)
    crw_id:    str             = Field(..., min_length=1)
    file_dir:  str             = Field(...)
    bbs_id:    str             = Field(..., min_length=1)
    param:     PolicyParam     = Field(...)
    item:      List[RegexItem] = Field(default_factory=list)
    last_data: Optional[LastData] = None

class PrismRequest(BaseModel):
    req_id:    str             = Field(..., min_length=1)
    type:      str             = Field(..., min_length=1)
    crw_id:    str             = Field(..., min_length=1)
    file_dir:  str             = Field(...)
    bbs_id:    str             = Field(..., min_length=1)
    param:     PrismParam      = Field(...)
    item:      List[RegexItem] = Field(default_factory=list)
    last_data: Optional[LastData] = None

# ── 공통 유틸 ─────────────────────────────────────────────────────
def error_response(msg):
    return JSONResponse(status_code=200, content={"ok": False, "message": msg})

def clean_text(text):
    return re.sub(r'\s+', ' ', text.strip()) if text else ""

def normalize_text(text):
    if not text: return ""
    return re.sub(r"\s+", " ", text.replace("&nbsp;"," ").replace("&#160;"," ").replace("\xa0"," ")).strip()

def extract_domain(url):
    try:
        netloc = urlparse(url).netloc.split(':')[0].lower()
        if not netloc: return "unknown"
        for pfx in ("www.","council.","office.","assembly."):
            if netloc.startswith(pfx): netloc = netloc[len(pfx):]; break
        return netloc.split('.')[0]
    except Exception: return "unknown"

def save_to_json(data, domain, prefix):
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)
    filepath = os.path.join(DOWNLOAD_DIR, f"{domain}_{prefix}_{datetime.now():%Y%m%d%H%M%S}.json")
    with open(filepath, "w", encoding="utf-8") as f: json.dump(data, f, ensure_ascii=False, indent=4)
    print(f"[+] 저장: {filepath}", flush=True)
    return filepath

def normalize_selector(selector):
    if not selector: return ""
    s = selector.strip()
    if any(s.startswith(p) for p in (".", "#", "[", "table","div","ul","nav","span","a","button","input")): return s
    return f".{s}"

def normalize_date_to_yyyymmdd(value):
    if not value: return None
    text = normalize_text(value)
    if not text: return None
    if re.fullmatch(r"\d{8}", text): return text
    for p in [r"(\d{4})\s*년\s*(\d{1,2})\s*월\s*(\d{1,2})\s*일?",
              r"(\d{4})-(\d{1,2})-(\d{1,2})", r"(\d{4})/(\d{1,2})/(\d{1,2})",
              r"(\d{4})\.\s*(\d{1,2})\.\s*(\d{1,2})\.?"]:
        m = re.search(p, text)
        if m: return f"{m.group(1)}{int(m.group(2)):02d}{int(m.group(3)):02d}"
    return text

def get_verify_options(ssl_mode):
    if ssl_mode == "Y": return certifi.where()
    if ssl_mode == "N": return False
    raise ValueError(f"Invalid SSL mode: {ssl_mode}")

def _build_result(data, log, interrupted, error=""):
    has_timeout = any("Timeout" in (e.get("error") or "") for e in log)
    has_error   = any(e.get("error") for e in log)
    n = len(data)
    if error:                                     s,c,m = "FAILED","500",f"수집 실패: {error}"
    elif n==0 and has_timeout:                    s,c,m = "TIMEOUT","408","타임아웃으로 수집 불가"
    elif n==0:                                    s,c,m = "EMPTY","204","수집 결과 없음"
    elif interrupted or has_timeout or has_error: s,c,m = "PARTIAL","206","일부 수집 완료"
    else:                                         s,c,m = "SUCCESS","200","수집 완료"
    return {"status":s,"code":c,"message":m,"dataCount":n,"interrupted":interrupted}

# ── BI_NO / RASMBLY_NUMPR_SESN 파서 ──────────────────────────────
def _to_int_str(v: str) -> str:
    m = re.search(r'\d+', v); return m.group() if m else ""

def _parse_bi_no(value: str) -> Dict[str, str]:
    v = value.strip()
    m = _P_BI_NO_SESN.match(v)
    if m: return {"BI_NO": m.group(1).strip(), "RASMBLY_SESN": _to_int_str(m.group(2))}
    m = re.match(r'^(.+?)\s*[\(\（](\d+)\s*대\s*[-/\s]\s*(\d+)\s*회[\)\）]', v)
    if m: return {"BI_NO": m.group(1).strip(), "RASMBLY_NUMPR": m.group(2), "RASMBLY_SESN": m.group(3)}
    m = re.match(r'^(.+?)\s*[\(\（]제\s*(\d+)\s*대.*?제\s*(\d+)\s*회[\)\）]', v)
    if m: return {"BI_NO": m.group(1).strip(), "RASMBLY_NUMPR": m.group(2), "RASMBLY_SESN": m.group(3)}
    return {"BI_NO": v}

def _parse_numpr_sesn(value: str) -> Dict[str, str]:
    v = value.strip()
    for pat, keys in [
        (_P_NUMPR_SESN,        lambda m: {"RASMBLY_NUMPR": _to_int_str(m.group(1)), "RASMBLY_SESN": _to_int_str(m.group(2))}),
        (_P_SLASH_NUMPR_SESN,  lambda m: {"RASMBLY_NUMPR": _to_int_str(m.group(1)), "RASMBLY_SESN": _to_int_str(m.group(2))}),
        (_P_HYPHEN_NUMPR_SESN, lambda m: {"RASMBLY_NUMPR": _to_int_str(m.group(1)), "RASMBLY_SESN": _to_int_str(m.group(2))}),
        (_P_DAE_HOE,           lambda m: {"RASMBLY_NUMPR": _to_int_str(m.group(1)), "RASMBLY_SESN": _to_int_str(m.group(2))}),
        (_P_NUMPR_ONLY,        lambda m: {"RASMBLY_NUMPR": _to_int_str(m.group(1))}),
        (_P_SESN_ONLY,         lambda m: {"RASMBLY_SESN":  _to_int_str(m.group(1))}),
    ]:
        m = pat.search(v)
        if m: return keys(m)
    if _P_DIGIT_ONLY.match(v): return {"RASMBLY_NUMPR": _to_int_str(v)}
    return {}

_VALUE_PARSERS: Dict[str, callable] = {
    "BI_NO":              _parse_bi_no,
    "RASMBLY_NUMPR_SESN": _parse_numpr_sesn,
}

# ── 정책용역보고서 파서 ───────────────────────────────────────────
def _prism_numeric_id(seq_id: str) -> str:
    return re.sub(r"[^0-9]", "", seq_id)

def _prism_rprt_type(file_name: str) -> str:
    return "B" if "보고서" in (file_name or "") else "A"

# ── item[] regex 파서 ─────────────────────────────────────────────
def apply_regex_raw(source, pattern):
    if not pattern: return None
    try: m = re.search(pattern, source, re.IGNORECASE | re.DOTALL)
    except re.error as exc: raise ValueError(f"잘못된 정규식: {pattern} / {exc}") from exc
    if not m: return None
    return m.group(1) if m.groups() else m.group(0)

def strip_html_tags(value):
    if not value: return None
    soup = BeautifulSoup(value, "lxml")
    text = soup.get_text("\n", strip=True)
    lines = [normalize_text(l) for l in text.splitlines() if normalize_text(l)]
    return "".join(lines) if lines else None

def parse_detail_by_items(detail_html, items, list_title=None):
    result = {}
    for item in items:
        key = normalize_text(item.col)
        if not key: continue
        if key in _FILE_SKIP_COLS: continue
        if item.value is not None and normalize_text(item.value):
            result[key] = normalize_text(item.value); continue
        if item.regex and len(item.regex)==1 and normalize_text(item.regex[0]).lower()=="list_title":
            result[key] = normalize_text(list_title) or ""; continue
        if not item.regex: continue
        raw_value = None
        for pattern in item.regex:
            raw_value = apply_regex_raw(detail_html, pattern)
            if raw_value is not None: break
        value = strip_html_tags(raw_value) if item.removeTags=="Y" else normalize_text(raw_value)
        if value and key.endswith("_DE"): value = normalize_date_to_yyyymmdd(value) or value
        parser = _VALUE_PARSERS.get(key)
        if parser and value:
            parsed = parser(value)
            result.update({k: v for k, v in parsed.items()})
        else:
            result[key] = value if value is not None else ""
    return result

def audit_fields(view_id, url, bi_cn, item_data, items):
    expected = {normalize_text(i.col) for i in items if normalize_text(i.col)}
    collected, empty, missing = [], [], []
    for key in sorted(expected):
        if key not in item_data:                    missing.append(key)
        elif not str(item_data[key] or "").strip(): empty.append(key)
        else:                                       collected.append(key)
    return {"view_id":view_id,"BI_CN":bi_cn,"URL":url,"collected":collected,"empty":empty,"missing":missing}

def save_field_logs(field_logs, req):
    now = datetime.now()
    path = os.path.join(FIELD_LOGS_DIR, req.type, req.crw_id, now.strftime("%Y"), now.strftime("%m"), f"{req.req_id}.json")
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path,"w",encoding="utf-8") as f: json.dump({"field_logs":field_logs}, f, ensure_ascii=False, indent=4)
    print(f"[+] field_logs 저장: {path} ({len(field_logs)}건)", flush=True)

# ── last_data ─────────────────────────────────────────────────────
def _build_last_data_signature(last_data):
    sig = {}
    for key in _LAST_DATA_MATCH_KEYS:
        val = getattr(last_data, key, None)
        if val and str(val).strip(): sig[key] = str(val).strip()
    for key, val in (last_data.model_extra or {}).items():
        if val and str(val).strip(): sig[key] = str(val).strip()
    return sig

def is_last_data_match(item_data, last_sig):
    if not last_sig: return False
    for key in _LAST_DATA_MATCH_KEYS:
        if key in last_sig and last_sig[key] and str(item_data.get(key,"")).strip()==last_sig[key]:
            print(f"[last_data] '{key}' 일치 → 중단: {last_sig[key]}", flush=True); return True
    return False

def is_list_item_past_last(list_item, last_sig):
    if not last_sig: return False
    vid = str(list_item.get("view_id","")).strip()
    if vid and last_sig.get("view_id") and vid==last_sig["view_id"]:
        print(f"[last_data][리스트] view_id 일치: {vid}", flush=True); return True
    href, last_url = str(list_item.get("link_href","")).strip(), last_sig.get("URL","")
    if href and last_url and (href==last_url or href in last_url or last_url.endswith(href)):
        print(f"[last_data][리스트] URL 일치: {href}", flush=True); return True
    return False

# ── 파일 저장 경로 ────────────────────────────────────────────────
def build_save_path(req, year, bi_cn, seq, ext):
    rasmbly = req.param.rasmbly_numpr or "0"
    path = os.path.join("/", req.file_dir, req.type, req.crw_id, rasmbly, year, f"CLIKC{bi_cn}_{seq}{ext}")
    os.makedirs(os.path.dirname(path), exist_ok=True)
    return path

def build_policy_save_path(req, year, outbbs_cn, seq, ext):
    bbs_id = req.bbs_id or "0"
    path = os.path.join("/", req.file_dir, req.type,
                        req.crw_id, bbs_id, year,
                        f"{outbbs_cn}_attach_{seq}{ext}")
    os.makedirs(os.path.dirname(path), exist_ok=True)
    return path

def build_prism_save_path(req: "PrismRequest", year: str, seq_id: str, seq: int, ext: str) -> str:
    numeric_id = _prism_numeric_id(seq_id)
    path = os.path.join("/", req.file_dir, req.type, year, f"{numeric_id}{ext}")
    os.makedirs(os.path.dirname(path), exist_ok=True)
    return path

# ── HTTP 유틸 ─────────────────────────────────────────────────────
async def fetch_html(url, ssl_mode):
    async with httpx.AsyncClient(headers={"User-Agent":USER_AGENT}, timeout=httpx.Timeout(20.0,connect=10.0),
                                  follow_redirects=True, verify=get_verify_options(ssl_mode)) as client:
        r = await client.get(url); r.raise_for_status(); return r.text

async def fetch_html_post(url, ssl_mode, form_data=None):
    async with httpx.AsyncClient(headers={"User-Agent":USER_AGENT}, timeout=httpx.Timeout(20.0,connect=10.0),
                                  follow_redirects=True, verify=get_verify_options(ssl_mode)) as client:
        r = await client.post(url, data=form_data or {}); r.raise_for_status(); return r.text

# ── bill 브라우저 헬퍼 ────────────────────────────────────────────
async def _setup_browser(pw):
    browser = await pw.chromium.launch(headless=True)
    page    = await browser.new_page()
    await page.route("**/*", lambda r: r.abort() if r.request.resource_type in BLOCKED_RESOURCES else r.continue_())
    return browser, page

async def _try_url_fallback(page, next_page):
    print(f"[!] {next_page}p URL 강제 점프 시도", flush=True)
    new_url = re.sub(PAGE_PARAM_PATTERN, rf'\g<1>={next_page}', page.url, flags=re.IGNORECASE)
    if new_url != page.url:
        try: await page.goto(new_url, wait_until="domcontentloaded", timeout=3000)
        except Exception as e: print(f"[!] URL fallback 실패: {e}", flush=True)

# ── bill 목록 수집 클래스 ─────────────────────────────────────────
class BillListCrawler:
    @staticmethod
    def _extract_view_id(href, onclick, row_html, view_id_param):
        clean = href.replace("&amp;","&") if href else ""
        if not view_id_param or view_id_param.strip()=="":
            if clean and not clean.startswith(("javascript","#")):
                m = re.search(r"/(\d+)(?:[/?#]|$)", clean)
                if m: return m.group(1)
        if clean and not clean.startswith(("javascript","#")):
            m = re.search(rf"[?&]{re.escape(view_id_param)}=([^&]+)", clean)
            if m: return m.group(1)
            m = re.search(VIEW_ID_AUTO_PARAMS, clean, re.IGNORECASE)
            if m: return m.group(2)
        m = re.search(rf"[?&]?{re.escape(view_id_param)}=([^&\"'>\s]+)", row_html)
        if m: return m.group(1)
        js = onclick or (href if href and href.startswith("javascript") else "")
        if js:
            if view_id_param:
                m = re.search(r"[a-zA-Z_]+\s*\(\s*'[^']*'\s*,\s*'(\d+)'", js)
                if m: return m.group(1)
            m = re.search(r"\(['\"]?([^'\"),]+)['\"]?\)", js)
            if m: return m.group(1)
        m = re.search(r"onclick\s*=\s*[\"'][a-zA-Z0-9_]+\([\"']([^\"']+)[\"']\)", row_html)
        if m: return m.group(1)
        return None

    @staticmethod
    async def _get_row_link(row, tds):
        info = {"href":"","onclick":"","bi_sj":""}
        tr_onclick = await row.get_attribute("onclick") or ""
        a_tag = await row.query_selector("a")
        if a_tag:
            info["href"]    = await a_tag.get_attribute("href") or ""
            info["onclick"] = await a_tag.get_attribute("onclick") or tr_onclick
            text = clean_text(await a_tag.inner_text())
            if text: info["bi_sj"] = text
        else:
            info["onclick"] = tr_onclick
            m = re.search(r"goDetail\s*\(\s*'([^']+)'\s*,\s*'([^']+)'\s*,\s*'([^']+)'\s*,\s*'([^']+)'\s*,\s*'([^']+)'\s*\)", tr_onclick)
            if m:
                pt,gn,bn,bt,bnum = m.groups()
                info["href"] = f"/info/billRead.do?menuId=006002003&propTypeCd={pt}&generationNum={gn}&billNo={bn}&billTypeCd={bt}&billNum={bnum}"
            for td in tds:
                title = await td.get_attribute("title")
                if title: info["bi_sj"] = clean_text(title); break
        return info

    @staticmethod
    async def extract_list_page(page, list_class, view_id_param="code"):
        selector = normalize_selector(list_class)
        await page.wait_for_selector(selector, timeout=5000)
        items = []
        for row in await page.query_selector_all(f"{selector} tbody > tr, {selector} ul > li, {selector} .list_row"):
            tds = await row.query_selector_all("td") or await row.query_selector_all("div, span, p")
            if not tds: continue
            item = {"row_texts": [clean_text(await td.inner_text()) for td in tds]}
            link = await BillListCrawler._get_row_link(row, tds)
            if link["bi_sj"]: item["BI_SJ"] = link["bi_sj"]
            item["link_href"] = link["href"]
            vid = BillListCrawler._extract_view_id(link["href"], link["onclick"], await row.inner_html(), view_id_param)
            if vid: item["view_id"] = vid
            items.append(item)
        return items

    @staticmethod
    async def apply_filter_and_search(page, numpr, list_class, form_sel, select_sel, btn_sel):
        print(f"[*] 필터 적용 (대수:{numpr})", flush=True)
        errors = []
        try:
            target_select = await page.query_selector(select_sel)
            if target_select and numpr:
                for opt in await target_select.query_selector_all("option"):
                    val = (await opt.get_attribute("value") or "").strip()
                    txt = (await opt.inner_text() or "").strip()
                    if val==numpr or val==f"0{numpr}" or f"{numpr}대" in txt:
                        await target_select.select_option(value=val)
                        await target_select.evaluate("node => node.dispatchEvent(new Event('change', {bubbles:true}))")
                        print(f"[+] 대수 선택: {txt}", flush=True); break
            try:
                btn = await page.wait_for_selector(btn_sel, timeout=3000, state="visible")
                if btn:
                    tag_name    = await btn.evaluate("node => node.tagName.toLowerCase()")
                    input_type  = await btn.get_attribute("type") or ""
                    onclick_val = await btn.get_attribute("onclick") or ""
                    href_val    = await btn.get_attribute("href") or ""
                    if tag_name == "input" and input_type.lower() == "submit":
                        async with page.expect_navigation(timeout=5000): await btn.click()
                    else:
                        is_ajax = (href_val in ("#","","javascript:void(0)") or
                                   any(k in onclick_val.lower() for k in ["return false","loading","ajax","fetch"]))
                        if is_ajax:
                            await btn.click()
                            await page.wait_for_function(
                                f"() => {{ const el=document.querySelector('{normalize_selector(list_class)} tbody > tr'); return el!==null; }}",
                                timeout=3000)
                        else:
                            async with page.expect_navigation(timeout=3000): await btn.click()
                print("[+] 검색 버튼 클릭 성공", flush=True)
            except Exception as e:
                errors.append({"step":"필터_버튼클릭","selector":btn_sel,"error":str(e)[:300]})
                await page.evaluate(f"document.querySelector('{form_sel}')?.submit()")
                await page.wait_for_load_state("networkidle")
            if list_class:
                try: await page.wait_for_selector(normalize_selector(list_class), timeout=2000)
                except Exception as e: errors.append({"step":"필터_리스트로드","selector":list_class,"error":str(e)[:300]})
        except Exception as e: errors.append({"step":"필터_전체오류","selector":"","error":str(e)[:300]})
        return errors

    @staticmethod
    async def get_total_pages(page, end_btn_selector=None):
        _page_re = re.compile(
            r'(?:fn[a-zA-Z_]*|pageIndex|pageNum|pageNo|page|go|move|schPageNo|cp)\s*[\(=]\s*[\'"]?(\d+)[\'"]?'
            r'|[?&]page=(\d+)',
            re.IGNORECASE)

        async def _extract_num(el):
            src = " ".join(filter(None, [
                await el.get_attribute("href")    or "",
                await el.get_attribute("onclick") or "",
                await el.inner_text()             or "",
            ]))
            if not src.strip():
                try:
                    parent = await el.evaluate_handle("node => node.closest('a, button')")
                    p = parent.as_element()
                    if p:
                        src = " ".join(filter(None, [
                            await p.get_attribute("href")    or "",
                            await p.get_attribute("onclick") or "",
                        ]))
                except Exception:
                    pass
            m = _page_re.search(src)
            if m:
                val = int(next(g for g in m.groups() if g is not None))
                return val if val > 1 else None
            return None

        try:
            if end_btn_selector:
                try:
                    el = await page.query_selector(normalize_selector(end_btn_selector))
                    if el:
                        val = await _extract_num(el)
                        if val: return val
                except Exception:
                    pass
            mx = 1
            for el in await page.query_selector_all("a, button"):
                try:
                    val = await _extract_num(el)
                    if val: mx = max(mx, val)
                except Exception:
                    continue
            return mx
        except Exception as e:
            print(f"[-] 페이지 수 탐지 실패: {e}", flush=True)
            return 1

    @staticmethod
    async def go_to_page(page, next_page, paging_sel, next_btn_sel):
        p_sel = normalize_selector(paging_sel); n_sel = normalize_selector(next_btn_sel)
        try:
            if await page.evaluate("typeof fn_egov_link_page === 'function'"):
                await page.evaluate(f"fn_egov_link_page({next_page});")
                await page.wait_for_load_state("domcontentloaded"); return True
        except: pass
        try:
            link = page.locator(p_sel).get_by_text(re.compile(f"^{next_page}$"), exact=True).first
            if await link.count() > 0:
                await link.click()
                try: await page.wait_for_function("() => document.querySelectorAll('tbody#searchList tr').length > 0", timeout=5000)
                except: await page.wait_for_timeout(1000)
                return True
            nxt = page.locator(n_sel).first
            if await nxt.count() > 0:
                await nxt.click(); await page.wait_for_load_state("domcontentloaded")
                await page.wait_for_timeout(500); return True
        except Exception as e: print(f"[!] 페이지 이동 실패: {e}", flush=True)
        return False

async def _collect_pages(page, list_url, numpr="", list_class="", vid_param="",
                         max_pages="", paging_sel="", next_btn_sel="", end_btn_sel="",
                         stop_check=None, search_form_selector="", numpr_select_selector="",
                         search_btn_selector="", last_sig=None, timeout=30000):
    if stop_check is None: stop_check = lambda: False

    collect_errors, last_data_reached, consecutive_fail = [], False, 0
    await page.goto(list_url, wait_until="domcontentloaded", timeout=int(timeout))
    if numpr and numpr.strip():
        collect_errors.extend(await BillListCrawler.apply_filter_and_search(
            page, numpr.strip(), list_class, search_form_selector, numpr_select_selector, search_btn_selector))
    else:
        await page.wait_for_selector(normalize_selector(list_class), timeout=5000)
    total    = await BillListCrawler.get_total_pages(page, end_btn_sel)
    safe_max = int(max_pages.strip()) if max_pages and max_pages.strip().isdigit() else 0
    target   = total if safe_max==0 else min(safe_max, total)
    data     = []
    for cp in range(1, target+1):
        if stop_check(): print("[!] 중단 요청 감지", flush=True); break
        print(f"[*] 수집: {cp}/{target}p", flush=True)
        try:
            page_items = await BillListCrawler.extract_list_page(page, list_class, vid_param)
            consecutive_fail = 0
            if last_sig:
                filtered = []
                for li in page_items:
                    if is_list_item_past_last(li, last_sig): last_data_reached=True; break
                    filtered.append(li)
                data.extend(filtered)
                if last_data_reached: print(f"[last_data] {cp}p 기준점 도달", flush=True); break
            else: data.extend(page_items)
        except Exception as e:
            consecutive_fail += 1
            msg = str(e); sel_match = re.search(r'locator\("([^"]+)"\)', msg)
            print(f"[!] {cp}p 실패: {msg}", flush=True)
            collect_errors.append({"step":f"리스트수집_{cp}p","selector":sel_match.group(1) if sel_match else list_class,"error":msg[:300]})
            if consecutive_fail >= _MAX_CONSECUTIVE_FAIL:
                print(f"[!] 연속 {consecutive_fail}회 실패 → 중단", flush=True); break
        if cp < target and not last_data_reached:
            if not await BillListCrawler.go_to_page(page, cp+1, paging_sel, next_btn_sel):
                await _try_url_fallback(page, cp+1)
    return data, collect_errors, last_data_reached

# ── bill 상세 수집 ────────────────────────────────────────────────
async def _extract_bill_detail_html(page, view_class, target_url, timeout=30000):
    await page.goto(target_url, wait_until="domcontentloaded", timeout=int(timeout))
    if view_class:
        sel = normalize_selector(view_class)
        try:
            await page.wait_for_selector(sel, timeout=3000)
            el = await page.query_selector(sel)
            if el: return await el.inner_html()
        except Exception: pass
    return await page.content()

def _extract_all_matches(html: str, patterns: list) -> list:
    for pattern in patterns:
        try:
            matches = re.findall(pattern, html, re.IGNORECASE | re.DOTALL)
            if matches: return [m if isinstance(m, str) else m[0] for m in matches]
        except re.error: continue
    return []

_VIEWER_URL_PATTERNS = ["viewer","synap","htmlViewer","previewAjax","pdf.do","hwpViewer"]
def _is_viewer_url(url: str) -> bool:
    return any(p in url for p in _VIEWER_URL_PATTERNS)

async def _extract_bill_attachments(page, view_class, base_url, req, bi_cn, year, items=None):
    detail_url    = page.url
    if not items: return {}
    file_nm_item  = next((i for i in items if i.col in ("BI_FILE_NM", "ORG_FILE_NM")), None)
    file_url_item = next((i for i in items if i.col in ("BI_FILE_URL", "DOWNURL")),    None)
    if not file_nm_item: return {}

    attach_td = None
    for sel in ["th:has-text('첨부파일') + td", "th:has-text('첨부') + td",
                "th:has-text('의안') + td", "td.left ul#editable"]:
        try:
            el = await page.query_selector(sel)
            if el: attach_td = el; break
        except Exception: continue
    if not attach_td and view_class:
        try: attach_td = await page.query_selector(normalize_selector(view_class))
        except Exception: pass
    if not attach_td: attach_td = page

    if file_url_item and file_url_item.regex:
        detail_html = await page.content()
        if view_class:
            try:
                el = await page.query_selector(normalize_selector(view_class))
                if el: detail_html = await el.inner_html()
            except Exception: pass
        raw_urls  = _extract_all_matches(detail_html, file_url_item.regex)
        raw_names = _extract_all_matches(detail_html, file_nm_item.regex) if file_nm_item.regex else []
        if not raw_urls: return {}

        attachments, seq = [], 0
        for i, raw_url in enumerate(raw_urls):
            full_url  = urljoin(base_url, raw_url.strip().replace("&amp;","&"))
            hint_name = re.sub(r'[\s\u00a0&;]+', ' ', raw_names[i] if i < len(raw_names) else "").strip()
            seq += 1
            print(f"[*] 다운로드 시도: {hint_name or full_url}", flush=True)
            if _is_viewer_url(full_url):
                print(f"[*] 뷰어 URL 저장: {hint_name}", flush=True)
                attachments.append({"original_name": hint_name, "file_path": "", "file_id": str(seq), "url": full_url})
                continue
            if full_url.startswith("javascript:"): break
            try:
                async with httpx.AsyncClient(headers={"User-Agent": USER_AGENT},
                        timeout=httpx.Timeout(60.0,connect=10.0), follow_redirects=True, verify=certifi.where()) as client:
                    r = await client.get(full_url); r.raise_for_status()
                cd = r.headers.get("content-disposition", ""); cd_name = None
                if cd:
                    m = re.search(r'filename\*?=["\']?(?:UTF-8\'\')?([^"\';\r\n]+)', cd, re.IGNORECASE)
                    if m: cd_name = unquote(m.group(1).strip())
                resolved_name = normalize_text(hint_name or cd_name) or f"file_{seq}"
                _, ext = os.path.splitext(resolved_name)
                if not ext:
                    ct = r.headers.get("content-type", "")
                    ext = next((v for k,v in {
                        "application/pdf": ".pdf", "application/msword": ".doc",
                        "application/vnd.openxmlformats-officedocument.wordprocessingml": ".docx",
                        "application/haansofthwp": ".hwp", "application/x-hwp": ".hwp",
                        "application/haansofthwpx": ".hwpx", "application/zip": ".zip",
                    }.items() if k in ct.lower()), ".bin")
                    resolved_name += ext
                save_path = build_save_path(req, year, bi_cn, seq, ext)
                os.makedirs(os.path.dirname(save_path), exist_ok=True)
                with open(save_path, "wb") as f: f.write(r.content)
                print(f"[+] 다운로드 완료: {resolved_name} → {save_path}", flush=True)
                attachments.append({"original_name": resolved_name, "file_path": save_path.replace("\\","/"), "file_id": str(seq), "url": full_url})
            except Exception as e:
                print(f"[-] 다운로드 실패 ({full_url}): {e}", flush=True)
                attachments.append({"original_name": hint_name or full_url, "file_path": "", "file_id": str(seq), "url": full_url})

        if attachments:
            return _pack_bi_attachment_result(attachments)

    skip_kw   = ["바로보기","바로듣기","미리보기","뷰어","관련 회의록","회의록","회의록보기","발의 의원","내려받기","위원회 바로가기","본회의 바로가기","바로가기","다운로드"]
    viewer_oc  = ["previewAjax","preListen","preview","viewer"]
    viewer_href= ["synap","htmlViewer","viewer/pdf","viewer/hwp"]
    viewer_cls = ["abtn_preview","preview","view"]

    attachments, seq, seen_urls = [], 0, set()
    for el in await attach_td.query_selector_all("a, span[onclick], [style*='cursor: pointer']"):
        raw      = clean_text(await el.inner_text())
        title_el = await el.query_selector("[title]")
        title    = clean_text(await title_el.get_attribute("title")) if title_el else ""
        if not title: title = raw
        href     = await el.get_attribute("href") or ""
        onclick  = await el.get_attribute("onclick") or ""
        el_class = await el.get_attribute("class") or ""
        if (any(k in raw for k in skip_kw) or any(k in title for k in skip_kw) or
            any(k in onclick for k in viewer_oc) or any(k in href for k in viewer_href) or
            any(k in el_class for k in viewer_cls)): continue
        if not raw: continue
        is_js         = href.startswith(("javascript","#")) or (onclick and not href)
        url_val       = onclick if is_js else (href or onclick)
        normalized_url = urljoin(base_url, url_val.replace("&amp;","&")) if url_val else ""
        if normalized_url and normalized_url in seen_urls: continue
        if normalized_url: seen_urls.add(normalized_url)
        original_name = title if title else raw
        print(f"[*] 다운로드 시도: {raw}", flush=True)
        try:
            await el.evaluate("node => { if(node.tagName === 'A') node.removeAttribute('target'); }")
            async with page.expect_download(timeout=15000) as dl_info: await el.click()
            download = await dl_info.value
            _, ext = os.path.splitext(download.suggested_filename or "")
            if not ext: ext = ".bin"
            seq += 1
            save_path = build_save_path(req, year, bi_cn, seq, ext) if req and bi_cn else os.path.join(FILE_DOWNLOAD_DIR, f"CLIKC{str(time.time_ns())[:16]}_{seq}{ext}")
            await download.save_as(save_path)
            print(f"[+] 다운로드 완료: {save_path}", flush=True)
            attachments.append({"original_name": original_name, "file_path": save_path.replace("\\","/"), "file_id": str(seq), "url": download.url})
        except Exception as e:
            print(f"[-] 다운로드 건너뜀 ({raw}): {str(e)[:100]}", flush=True)
            seq += 1
            attachments.append({"original_name": raw, "file_path": "", "file_id": str(seq), "url": url_val})
            try:
                await page.goto(detail_url, wait_until="domcontentloaded", timeout=30000)
                await page.wait_for_timeout(300)
            except Exception: pass

    if not attachments: return {}
    return _pack_bi_attachment_result(attachments)

def _pack_bi_attachment_result(attachments: list) -> dict:
    names = [a["original_name"] for a in attachments]
    paths = [a["file_path"]     for a in attachments]
    ids   = [a["file_id"]       for a in attachments]
    urls  = [a["url"]           for a in attachments]
    def one_or_json(lst): return lst[0] if len(lst)==1 else json.dumps(lst, ensure_ascii=False)
    return {
        "BI_FILE_NM":   one_or_json(names),
        "BI_FILE_PATH": one_or_json(paths),
        "BI_FILE_ID":   one_or_json(ids),
        "BI_FILE_URL":  one_or_json(urls),
    }

# ── policy 전용 파싱 ──────────────────────────────────────────────
def parse_policy_detail(detail_html: str, items, list_title: str = None) -> dict:
    result = {}
    for item in items:
        key = normalize_text(item.col)
        if not key: continue
        if key in _POLICY_FILE_COLS: continue
        if item.value is not None and normalize_text(item.value):
            result[key] = normalize_text(item.value); continue
        if item.regex and len(item.regex)==1 and normalize_text(item.regex[0]).lower()=="list_title":
            result[key] = normalize_text(list_title) or ""; continue
        if not item.regex: continue
        raw_value = None
        for pattern in item.regex:
            raw_value = apply_regex_raw(detail_html, pattern)
            if raw_value is not None: break
        if key == "CDATE":
            result[key] = normalize_date_to_yyyymmdd(raw_value) or normalize_text(raw_value) or ""
        else:
            value = strip_html_tags(raw_value) if item.removeTags=="Y" else normalize_text(raw_value)
            result[key] = value if value is not None else ""
    return result

async def _extract_policy_attachments(page, view_class, base_url, req, outbbs_cn, year, items=None):
    detail_url    = page.url
    if not items: return {}
    file_nm_item  = next((i for i in items if i.col == "ORG_FILE_NM"), None)
    file_url_item = next((i for i in items if i.col == "DOWNURL"),     None)
    skip_kw   = ["바로보기","미리보기","뷰어","preview","바로가기"]
    viewer_oc = ["goPreview","preview","viewer"]
    viewer_cl = ["btn-board","preview","view"]
    attachments   = []

    if file_url_item and file_url_item.regex:
        detail_html = await page.content()
        if view_class:
            try:
                el = await page.query_selector(normalize_selector(view_class))
                if el: detail_html = await el.inner_html()
            except Exception: pass

        raw_urls  = _extract_all_matches(detail_html, file_url_item.regex)
        raw_names = _extract_all_matches(detail_html, file_nm_item.regex) if file_nm_item and file_nm_item.regex else []
        raw_urls  = raw_urls[:len(raw_names)] if raw_names else raw_urls[:1]

        seen, deduped_urls, deduped_names = set(), [], []
        for i, u in enumerate(raw_urls):
            if u not in seen:
                seen.add(u); deduped_urls.append(u)
                deduped_names.append(raw_names[i] if i < len(raw_names) else "")
        raw_urls, raw_names = deduped_urls, deduped_names

        for seq, raw_url in enumerate(raw_urls, start=1):
            raw_url_clean = raw_url.strip().replace("&amp;", "&")
            hint_name     = normalize_text(raw_names[seq-1] if seq-1 < len(raw_names) else "")
            print(f"[*] [POLICY] 다운로드 시도: {hint_name or raw_url_clean}", flush=True)
            save_path, resolved_name = "", hint_name

            is_js_call = not raw_url_clean.startswith(("http", "/", "./", "../")) and "(" in raw_url_clean
            full_url   = raw_url_clean if is_js_call else urljoin(base_url, raw_url_clean)

            try:
                if is_js_call:
                    args     = re.findall(r"[\'\"]?(\d+)[\'\"]?", raw_url_clean)
                    link_sel = None
                    for arg in reversed(args):
                        candidate = f"a[onclick*='{arg}']"
                        try:
                            if await page.locator(candidate).count() == 1:
                                link_sel = candidate; break
                        except Exception: continue
                    if not link_sel and args: link_sel = f"a[onclick*='{args[-1]}']"
                    if not link_sel: raise ValueError(f"JS 링크 셀렉터 없음: {raw_url_clean}")
                else:
                    full_url   = urljoin(base_url, raw_url_clean) if raw_url_clean.startswith(("http", "/")) else urljoin(base_url, "/" + raw_url_clean)
                    httpx_ok   = False
                    try:
                        async with httpx.AsyncClient(headers={"User-Agent": USER_AGENT},
                                timeout=httpx.Timeout(60.0, connect=10.0),
                                follow_redirects=True, verify=certifi.where()) as client:
                            r = await client.get(full_url); r.raise_for_status()
                            ct = r.headers.get("content-type", "")
                            if "text/html" in ct or len(r.content) < 100:
                                raise ValueError(f"유효하지 않은 응답: {ct}")
                        if len(r.content) > MAX_FILE_SIZE_BYTES:
                            print(f"[*] [POLICY] 크기 초과 → 스킵: {hint_name}", flush=True)
                            attachments.append({"original_name": resolved_name, "file_path": save_path,
                                                "file_id": str(seq), "url": full_url, "file_type": "A"})
                            continue
                        cd = r.headers.get("content-disposition", ""); cd_name = None
                        if cd:
                            m = re.search(r'filename\*?=["\']?(?:UTF-8\'\')?([^"\';\r\n]+)', cd, re.IGNORECASE)
                            if m: cd_name = unquote(m.group(1).strip())
                        resolved_name = normalize_text(hint_name or cd_name) or f"file_{seq}"
                        _, ext = os.path.splitext(resolved_name)
                        if not ext: ext = ".bin"
                        save_path = build_policy_save_path(req, year, outbbs_cn, seq, ext)
                        with open(save_path, "wb") as f: f.write(r.content)
                        save_path = save_path.replace("\\", "/")
                        print(f"[+] [POLICY] 저장: {resolved_name} → {save_path}", flush=True)
                        httpx_ok = True
                    except Exception as e:
                        print(f"[*] [POLICY] httpx 실패 → Playwright fallback: {str(e)[:80]}", flush=True)

                    if httpx_ok:
                        attachments.append({"original_name": resolved_name, "file_path": save_path,
                                            "file_id": str(seq), "url": full_url, "file_type": "A"})
                        continue

                    qs_pairs  = parse_qsl(urlparse(raw_url_clean).query)
                    path_part = urlparse(raw_url_clean).path
                    link_sel  = None
                    for k, v in reversed(qs_pairs):
                        candidate = f"a[href*='{k}={v}']"
                        try:
                            if await page.locator(candidate).count() >= 1:
                                link_sel = candidate; break
                        except Exception: continue
                    if not link_sel:
                        link_sel = f"a[href*='{path_part}']" if path_part and path_part != "/" else "a.attachment"

                link = page.locator(link_sel).first
                if await link.count() == 0: raise ValueError(f"링크 없음: {link_sel}")
                await link.evaluate("node => node.removeAttribute('target')")

                async def _do_download():
                    async with page.expect_download(timeout=30000) as dl_info: await link.click()
                    dl = await dl_info.value
                    _, ext = os.path.splitext(dl.suggested_filename or hint_name or "")
                    if not ext: ext = ".bin"
                    name = dl.suggested_filename or hint_name or f"file_{seq}"
                    path = build_policy_save_path(req, year, outbbs_cn, seq, ext)
                    await dl.save_as(path)
                    return name, path, dl.url

                try:
                    resolved_name, save_path, full_url = await asyncio.wait_for(_do_download(), timeout=10.0)
                    file_size = os.path.getsize(save_path)
                    if file_size > MAX_FILE_SIZE_BYTES:
                        os.remove(save_path); save_path = ""
                        print(f"[*] [POLICY] 크기 초과 ({file_size/1024/1024:.1f}MB) → 삭제: {resolved_name}", flush=True)
                    else:
                        save_path = save_path.replace("\\", "/")
                        print(f"[+] [POLICY] 저장: {resolved_name} → {save_path}", flush=True)
                except asyncio.TimeoutError:
                    print(f"[*] [POLICY] 타임아웃 → 스킵: {hint_name}", flush=True)
                    save_path = ""
                    try:
                        await page.goto(detail_url, wait_until="domcontentloaded", timeout=30000)
                        await page.wait_for_timeout(300)
                    except Exception: pass

            except Exception as e:
                print(f"[-] [POLICY] 실패 ({raw_url_clean}): {str(e)[:120]}", flush=True)
                try:
                    await page.goto(detail_url, wait_until="domcontentloaded", timeout=30000)
                    await page.wait_for_timeout(300)
                except Exception: pass

            attachments.append({"original_name": resolved_name, "file_path": save_path,
                                 "file_id": str(seq), "url": full_url, "file_type": "A"})
        if attachments:
            return _pack_attachment_result(attachments)

    root = page
    if view_class:
        try:
            el = await page.query_selector(normalize_selector(view_class))
            if el: root = el
        except Exception: pass

    seen_urls, seq = set(), 0
    for el in await root.query_selector_all("a"):
        href     = await el.get_attribute("href") or ""
        onclick  = await el.get_attribute("onclick") or ""
        el_class = await el.get_attribute("class") or ""
        raw      = clean_text(await el.inner_text())
        if not raw: continue
        if href in ("#", "") and not onclick: continue
        if any(k in raw     for k in skip_kw): continue
        if any(k in onclick for k in viewer_oc): continue
        if any(k in el_class for k in viewer_cl): continue
        if any(k in href    for k in ["viewer","synap","htmlViewer"]): continue
        normalized_url = urljoin(base_url, href.replace("&amp;","&")) if href else ""
        if normalized_url in seen_urls: continue
        if normalized_url: seen_urls.add(normalized_url)
        print(f"[*] [POLICY fallback] 다운로드 시도: {raw}", flush=True)
        try:
            await el.evaluate("node => node.removeAttribute('target')")
            async def _do_fallback():
                async with page.expect_download(timeout=10000) as dl_info: await el.click()
                dl = await dl_info.value
                _, ext = os.path.splitext(dl.suggested_filename or "")
                if not ext: ext = ".bin"
                name = dl.suggested_filename or raw
                path = build_policy_save_path(req, year, outbbs_cn, seq + 1, ext)
                await asyncio.wait_for(dl.save_as(path), timeout=10.0)
                return name, path, dl.url
            try:
                seq += 1
                resolved_name, save_path, dl_url = await asyncio.wait_for(_do_fallback(), timeout=10.0)
                file_size = os.path.getsize(save_path)
                if file_size > MAX_FILE_SIZE_BYTES:
                    os.remove(save_path); save_path = ""
                    print(f"[*] [POLICY fallback] 크기 초과 → 삭제: {resolved_name}", flush=True)
                else:
                    save_path = save_path.replace("\\", "/")
                    print(f"[+] [POLICY fallback] 저장: {save_path}", flush=True)
                attachments.append({"original_name": resolved_name, "file_path": save_path,
                                     "file_id": str(seq), "url": dl_url, "file_type": "A"})
            except asyncio.TimeoutError:
                print(f"[*] [POLICY fallback] 10초 초과 → 스킵: {raw}", flush=True)
                attachments.append({"original_name": raw, "file_path": "",
                                     "file_id": str(seq), "url": normalized_url})
                try:
                    await page.goto(detail_url, wait_until="domcontentloaded", timeout=30000)
                    await page.wait_for_timeout(300)
                except Exception: pass
        except Exception as e:
            print(f"[-] [POLICY fallback] 건너뜀 ({raw}): {str(e)[:100]}", flush=True)
            seq += 1
            attachments.append({"original_name": raw, "file_path": "", "file_id": str(seq), "url": normalized_url})
            try:
                await page.goto(detail_url, wait_until="domcontentloaded", timeout=30000)
                await page.wait_for_timeout(300)
            except Exception: pass

    return _pack_attachment_result(attachments) if attachments else {}

def _pack_attachment_result(attachments: list) -> dict:
    def one_or_json(lst): return lst[0] if len(lst)==1 else json.dumps(lst, ensure_ascii=False)
    return {
        "ORG_FILE_NM": one_or_json([a["original_name"]        for a in attachments]),
        "DOWNPATH":    one_or_json([a["file_path"]            for a in attachments]),
        "DOWNID":      one_or_json([a["file_id"]              for a in attachments]),
        "DOWNURL":     one_or_json([a["url"]                  for a in attachments]),
        "FILE_TYPE":   one_or_json([a.get("file_type", "A")   for a in attachments]),
    }

# ── PRISM 리스트/상세 수집 ────────────────────────────────────────
async def _fetch_prism_list_api(list_url, list_api_url, max_pages="", stop_check=None,
                                last_sig=None, timeout=20000):
    if stop_check is None: stop_check = lambda: False
    data, error_logs = [], []
    last_data_reached = False
    safe_max  = int(max_pages.strip()) if max_pages and max_pages.strip().isdigit() else 0
    base_url  = "{u.scheme}://{u.netloc}".format(u=urlparse(list_url))
    page_no   = 1
    while True:
        if stop_check(): print("[PRISM] 중단 요청", flush=True); break
        payload = {"currentPage": page_no, "pageSize": 10, "pageYn": "Y"}
        try:
            async with httpx.AsyncClient(
                    headers={"User-Agent": USER_AGENT, "Content-Type": "application/json",
                             "Referer": list_url, "Origin": base_url},
                    timeout=httpx.Timeout(float(timeout)/1000, connect=10.0),
                    follow_redirects=True, verify=False) as client:
                r = await client.post(list_api_url, json=payload); r.raise_for_status()
                raw = r.json()
        except Exception as e:
            error_logs.append({"step": f"리스트API_{page_no}p", "error": str(e)[:200]})
            print(f"[PRISM] 리스트 API 실패 ({page_no}p): {e}", flush=True); break

        if page_no == 1:
            print(f"[PRISM] 리스트 API keys: {list(raw.keys())}", flush=True)

        total_count = (raw.get("totalCount") or raw.get("listCnt")
                       or raw.get("resultData", {}).get("totalCount", 0))
        items = (raw.get("list") or raw.get("resultList")
                 or raw.get("resultData", {}).get("list")
                 or raw.get("resultData", {}).get("rschList") or [])
        if not items: print(f"[PRISM] {page_no}p 항목 없음 → 종료", flush=True); break

        total_pages = -(-int(total_count) // 10) if total_count else page_no
        target = total_pages if safe_max==0 else min(safe_max, total_pages)
        print(f"[PRISM] 리스트 API {page_no}/{target}p ({len(items)}건)", flush=True)

        for item in items:
            if stop_check(): break
            asmt_id = item.get("asmtId") or item.get("researchId") or item.get("taskId") or ""
            title   = item.get("asmtNm") or item.get("taskNm") or item.get("title") or ""
            if not asmt_id: continue
            detail_url = f"{base_url}/homepage/asmt/{asmt_id}"
            list_item  = {"view_id": asmt_id, "link_href": detail_url, "BI_SJ": title}
            print(f"  {title[:30]} → {asmt_id}", flush=True)
            if last_sig and is_list_item_past_last(list_item, last_sig):
                last_data_reached = True; break
            data.append(list_item)

        if last_data_reached or page_no >= target: break
        page_no += 1
    return data, error_logs, last_data_reached

async def _collect_prism_list(page, list_url, list_api_url=None, max_pages="",
                               stop_check=None, last_sig=None, timeout=20000):
    if stop_check is None: stop_check = lambda: False
    if list_api_url:
        return await _fetch_prism_list_api(list_url, list_api_url, max_pages, stop_check, last_sig, timeout)

    data, error_logs = [], []
    last_data_reached = False
    safe_max = int(max_pages.strip()) if max_pages and max_pages.strip().isdigit() else 0

    await page.goto(list_url, wait_until="domcontentloaded", timeout=timeout)
    await page.wait_for_timeout(5000)
    try:
        await page.wait_for_selector("table.tstyle_list tbody tr", timeout=timeout)
    except Exception:
        error_logs.append({"step": "리스트_로드", "url": list_url, "error": "테이블 로드 실패"})
        return data, error_logs, last_data_reached

    total_pages  = await _get_prism_total_pages(page)
    target       = total_pages if safe_max==0 else min(safe_max, total_pages)
    print(f"[PRISM] 총 {total_pages}p → 수집 대상 {target}p", flush=True)

    current_page = 1
    while current_page <= target:
        if stop_check(): print("[PRISM] 중단 요청", flush=True); break
        print(f"[PRISM] 리스트 {current_page}/{target}p 수집", flush=True)
        rows   = await page.query_selector_all("table.tstyle_list tbody tr")
        titles = []
        for row in rows:
            title_el = await row.query_selector("a.ellipsis")
            titles.append(clean_text(await title_el.inner_text()) if title_el else "")

        for idx in range(len(titles)):
            if stop_check(): break
            title = titles[idx]
            try:
                rows_fresh = await page.query_selector_all("table.tstyle_list tbody tr")
                if idx >= len(rows_fresh): raise ValueError(f"행 인덱스 초과: {idx}")
                title_a = await rows_fresh[idx].query_selector("a.ellipsis")
                if not title_a: continue
                current_url = page.url
                await title_a.click()
                await page.wait_for_function(
                    f"() => window.location.pathname !== '{urlparse(current_url).path}'", timeout=5000)
                new_url = page.url
                asmt_id = new_url.rstrip("/").split("/")[-1]
                if asmt_id == "list" or not asmt_id: raise ValueError(f"asmtId 추출 실패: {new_url}")
                list_item = {"view_id": asmt_id, "link_href": new_url, "BI_SJ": title}
                print(f"  [{idx+1}] {title[:30]} → {asmt_id}", flush=True)
                if last_sig and is_list_item_past_last(list_item, last_sig):
                    last_data_reached = True
                    await page.go_back(wait_until="domcontentloaded", timeout=timeout)
                    await page.wait_for_selector("table.tstyle_list tbody tr", timeout=5000)
                    break
                data.append(list_item)
            except Exception as e:
                print(f"  [{idx+1}] 클릭 실패 ({title[:20]}): {str(e)[:80]}", flush=True)
                error_logs.append({"step": f"리스트_클릭_{current_page}p_{idx+1}번", "title": title, "error": str(e)[:200]})
            finally:
                if "asmt/list" not in page.url:
                    try:
                        await page.go_back(wait_until="domcontentloaded", timeout=timeout)
                        await page.wait_for_selector("table.tstyle_list tbody tr", timeout=5000)
                        await page.wait_for_timeout(500)
                    except Exception:
                        await page.goto(list_url, wait_until="domcontentloaded", timeout=timeout)
                        await page.wait_for_timeout(5000)
                        if current_page > 1:
                            await _go_to_prism_page(page, current_page, timeout)

        if last_data_reached: break
        if current_page < target:
            if not await _go_to_prism_page(page, current_page+1, timeout):
                print(f"[PRISM] {current_page+1}p 이동 실패 → 중단", flush=True); break
        current_page += 1
    return data, error_logs, last_data_reached

def _parse_prism_detail(html: str, list_title: str = "") -> dict:
    if not html: return {"ASG_NM": list_title}
    soup = BeautifulSoup(html, "lxml")
    def get_td(th_text: str) -> str:
        th = soup.find("th", string=lambda t: t and th_text in t.strip())
        if not th: return ""
        td = th.find_next_sibling("td")
        return normalize_text(td.get_text(" ", strip=True)) if td else ""
    rsrc_term = get_td("연구기간")
    rsrc_from, rsrc_to = ("", "")
    if "~" in rsrc_term:
        parts = rsrc_term.split("~")
        rsrc_from = normalize_text(parts[0]); rsrc_to = normalize_text(parts[1]) if len(parts)>1 else ""
    m = re.search(r"\d{4}", get_td("발행년도"))
    return {
        "ASG_NM":        get_td("과제명") or list_title,
        "ASG_ORG_NM":    get_td("기관명"),
        "ASG_DPRT":      get_td("관리부서"),
        "ASG_PHONE":     get_td("전화번호"),
        "ASG_RSRC_FROM": rsrc_from,
        "ASG_RSRC_TO":   rsrc_to,
        "ASG_RSRC_FD":   re.sub(r"\s*&gt;\s*|\s*>\s*", " > ", get_td("연구분야")).strip(" >"),
        "RSRC_TITLE":    get_td("보고서명"),
        "RSRC_CNTN":     get_td("목차"),
        "RSRC_INFO":     get_td("초록"),
        "RSRC_SBJ":      get_td("주제어"),
        "RSRC_YEAR":     m.group() if m else "",
        "CNT_PRF_ORG":   get_td("수행기관"),
        "CNT_PRF_RSR":   get_td("수행연구원"),
        "CNT_DATE":      get_td("계약일자"),
        "CNT_MTHD":      get_td("계약방식"),
        "CNT_AMT":       get_td("계약금액"),
    }

async def _get_prism_total_pages(page) -> int:
    try:
        await page.wait_for_selector("div.page-links a.page-link", timeout=5000)
        links = await page.query_selector_all("div.page-links a.page-link")
        if links:
            txt = clean_text(await links[-1].inner_text())
            if txt.isdigit(): return int(txt)
    except Exception: pass
    return 1

async def _go_to_prism_page(page, page_no: int, timeout: int = 20000) -> bool:
    try:
        links = await page.query_selector_all("div.page-links a.page-link")
        for link in links:
            if clean_text(await link.inner_text()) == str(page_no):
                await link.click()
                await page.wait_for_selector("table.tstyle_list tbody tr", timeout=timeout)
                await page.wait_for_timeout(300); return True
        nxt = await page.query_selector("a.page-navi.next")
        if nxt:
            await nxt.click(); await page.wait_for_timeout(500)
            return await _go_to_prism_page(page, page_no, timeout)
    except Exception as e:
        print(f"[PRISM] 페이지 이동 실패 ({page_no}p): {e}", flush=True)
    return False

async def _extract_prism_attachments(page, view_class, base_url, req, seq_id, year, items=None):
    detail_url  = page.url
    attachments = []
    seq         = 0
    for li in await page.query_selector_all("div.file_list li"):
        name_el   = await li.query_selector("p.text")
        hint_name = clean_text(await name_el.inner_text()) if name_el else ""
        btn = await li.query_selector("a.btn4")
        if not btn: continue
        is_private = False
        try:
            notice = await li.evaluate(
                "node => node.closest('td')?.querySelector('p.fs15')?.textContent || ''")
            if "이후 공개 예정" in notice or "비공개" in notice: is_private = True
        except Exception: pass
        if is_private:
            print(f"[*] [PRISM] 비공개 스킵: {hint_name}", flush=True)
            seq += 1
            attachments.append({"orignl_file_nm": hint_name, "sys_file_nm": "",
                                 "file_seq": str(seq), "rprt_type": _prism_rprt_type(hint_name)})
            continue
        print(f"[*] [PRISM] 다운로드 시도: {hint_name}", flush=True)
        save_path = ""
        try:
            await btn.evaluate("node => node.removeAttribute('target')")
            async with page.expect_download(timeout=30000) as dl_info: await btn.click()
            dl            = await dl_info.value
            _, ext        = os.path.splitext(dl.suggested_filename or hint_name or "")
            if not ext: ext = ".bin"
            resolved_name = dl.suggested_filename or hint_name or f"file_{seq+1}"
            rprt_type     = _prism_rprt_type(resolved_name)
            seq          += 1
            save_path     = build_prism_save_path(req, year, seq_id, seq, ext)
            await dl.save_as(save_path)
            if os.path.getsize(save_path) > MAX_FILE_SIZE_BYTES:
                os.remove(save_path); save_path = ""
                print(f"[*] [PRISM] 크기 초과 → 삭제: {resolved_name}", flush=True)
            else:
                save_path = save_path.replace("\\", "/")
                print(f"[+] [PRISM] 저장(TYPE={rprt_type}): {resolved_name} → {save_path}", flush=True)
        except asyncio.TimeoutError:
            print(f"[*] [PRISM] 타임아웃 → 스킵: {hint_name}", flush=True)
            seq += 1; resolved_name = hint_name or f"file_{seq}"; rprt_type = _prism_rprt_type(resolved_name)
            try: await page.goto(detail_url, wait_until="domcontentloaded", timeout=30000); await page.wait_for_timeout(300)
            except Exception: pass
        except Exception as e:
            print(f"[-] [PRISM] 실패 ({hint_name}): {str(e)[:100]}", flush=True)
            seq += 1; resolved_name = hint_name or f"file_{seq}"; rprt_type = _prism_rprt_type(resolved_name)
            try: await page.goto(detail_url, wait_until="domcontentloaded", timeout=30000); await page.wait_for_timeout(300)
            except Exception: pass
        attachments.append({"orignl_file_nm": resolved_name, "sys_file_nm": save_path,
                             "file_seq": str(seq), "rprt_type": rprt_type})
    return _pack_prism_attachment_result(attachments) if attachments else {}

def _pack_prism_attachment_result(attachments: list) -> dict:
    def one_or_json(lst): return lst[0] if len(lst)==1 else json.dumps(lst, ensure_ascii=False)
    return {
        "ORIGNL_FILE_NM": one_or_json([a["orignl_file_nm"] for a in attachments]),
        "SYS_FILE_NM": one_or_json([a["sys_file_nm"] for a in attachments]),
        "FILE_SEQ":    one_or_json([a["file_seq"]    for a in attachments]),
        "RPRT_TYPE":   one_or_json([a["rprt_type"]   for a in attachments]),
    }

# ── 공통 전송 ─────────────────────────────────────────────────────
async def _do_send(target_url, payload):
    async with httpx.AsyncClient() as client:
        try:
            r = await client.post(target_url, json=payload, timeout=120.0)
            print(f"[OK] API 전송 {'성공' if r.status_code==200 else '완료'}", flush=True)
        except Exception as e: print(f"[!] 네트워크 오류: {e}", flush=True)

# ── bill 실행 엔진 ────────────────────────────────────────────────
async def execute_bill_scraping(req: ScrapeRequest):
    app.state.stop_scraping = False
    p = req.param
    list_data, view_data, error_logs, field_logs = [], [], [], []
    filepath = None
    last_sig = _build_last_data_signature(req.last_data) if req.last_data else None
    if last_sig: print(f"[last_data] 추가수집 모드: {last_sig}", flush=True)
    has_file_item = any(i.col in {"BI_FILE_NM","BI_FILE_URL"} for i in req.item)

    async with async_playwright() as playwright:
        print(f"\n{'='*60}\n[*] [1단계] 리스트 수집: {p.list_url}", flush=True)
        browser, page = await _setup_browser(playwright)
        try:
            list_data, collect_errors, last_data_reached = await _collect_pages(
                page, p.list_url, p.rasmbly_numpr, p.list_class, p.view_id_param,
                p.max_pages, p.paging_selector, p.next_btn_selector, p.end_btn_selector,
                lambda: app.state.stop_scraping,
                p.search_form_selector, p.numpr_select_selector, p.search_btn_selector,
                last_sig=last_sig, timeout=int(p.timeout))
        finally:
            await browser.close(); print("[*] 1단계 브라우저 종료", flush=True)
        error_logs.extend(collect_errors)
        if not list_data:
            error_logs.append({"step":"1단계_리스트수집","url":p.list_url,"selector":p.list_class,"error":"리스트 수집 결과 0건"})

        total = len(list_data)
        print(f"\n[*] [2단계] 상세 수집 (총 {total}건)\n{'-'*60}", flush=True)
        detail_last_data_reached = False
        browser, page = await _setup_browser(playwright)
        try:
            for idx, item in enumerate(list_data):
                if app.state.stop_scraping: print(f"\n[!] 중단 요청: {idx}번째", flush=True); break
                vid = item.get("view_id")
                if not vid: continue
                print(f"[*] 상세 ({idx+1}/{total}) ID: {vid}", flush=True)
                href     = item.get("link_href","")
                is_real  = href and not href.startswith(("#","javascript"))
                target_url = urljoin(p.list_url, href) if is_real else f"{p.view_url}{'&' if '?' in p.view_url else '?'}{p.view_id_param}={vid}"
                try:
                    detail_html = await _extract_bill_detail_html(page, p.view_class, target_url, timeout=int(p.timeout))
                    parsed_u = urlparse(target_url); base = f"{parsed_u.scheme}://{parsed_u.netloc}"
                    bi_cn       = str(time.time_ns())[:16]
                    list_title  = item.get("BI_SJ","")
                    detail      = parse_detail_by_items(detail_html, req.item, list_title=list_title)
                    if not detail.get("RASMBLY_NUMPR") and p.rasmbly_numpr:
                        detail["RASMBLY_NUMPR"] = str(p.rasmbly_numpr)
                    if has_file_item and not detail.get("BI_FILE_PATH"):
                        year = (detail.get("ITNC_DE") or "")[:4] or str(datetime.now().year)
                        try:
                            file_result = await _extract_bill_attachments(page, p.view_class, base, req, bi_cn, year, items=req.item)
                        except Exception as e:
                            print(f"    [!] 첨부파일 수집 실패: {str(e)[:100]}", flush=True); file_result={}
                            try: await page.goto(target_url, wait_until="domcontentloaded", timeout=int(p.timeout))
                            except Exception: pass
                        detail.update({
                            "BI_FILE_NM":   file_result.get("BI_FILE_NM",   ""),
                            "BI_FILE_PATH": file_result.get("BI_FILE_PATH", ""),
                            "BI_FILE_ID":   file_result.get("BI_FILE_ID",   ""),
                            "BI_FILE_URL":  file_result.get("BI_FILE_URL",  ""),
                        })
                    collected_item = {"view_id":vid,"URL":target_url,"BI_CN":f"CLIKC{bi_cn}",**detail}
                    field_logs.append(audit_fields(vid, target_url, f"CLIKC{bi_cn}", collected_item, req.item))
                    if last_sig and not last_data_reached and is_last_data_match(collected_item, last_sig):
                        detail_last_data_reached=True; print("[last_data] 상세 기준점 도달", flush=True); break
                    view_data.append(collected_item)
                except Exception as e:
                    print(f"    [!] ID: {vid} 실패: {e}", flush=True)
                    view_data.append({"view_id":vid,"URL":target_url,"view_error":str(e)})
                    error_logs.append({"step":"2단계_상세수집","view_id":vid,"url":target_url,"error":str(e)})
        except Exception as e:
            print(f"\n[!] 상세 수집 전체 에러: {e}", flush=True)
            return {"req_id":req.req_id,"type":req.type,"crw_id":req.crw_id,"file_dir":req.file_dir,"ok":False,"error_msg":str(e)}
        finally: await browser.close()

    is_interrupted = app.state.stop_scraping or last_data_reached or detail_last_data_reached
    result_block   = _build_result(view_data, error_logs, is_interrupted)
    if (last_data_reached or detail_last_data_reached) and result_block["status"] in ("SUCCESS","PARTIAL"):
        result_block["message"] = "추가수집 완료 (last_data 기준점 도달)"
    if field_logs: save_field_logs(field_logs, req)
    view_data.reverse()
    full_payload = {"reqId":req.req_id,"type":req.type,"crwId":req.crw_id,"fileDir":req.file_dir,
                    "result":result_block,"data":view_data,"log":error_logs}
    domain = extract_domain(p.list_url)
    if view_data:
        filepath = save_to_json(full_payload, domain, req.type)
        print(f"[OK] 저장 완료 ({len(view_data)}건): {filepath}", flush=True)
    else:
        filepath = save_to_json(full_payload, domain, req.type+"_error")
        print(f"[!] 에러 로그 {len(error_logs)}건 저장: {filepath}", flush=True)
    await _do_send(INSERT_API_URL, full_payload)
    return {"req_id":req.req_id,"type":req.type,"crw_id":req.crw_id,"file_dir":req.file_dir,
            "ok":True,"interrupted":is_interrupted,
            "last_data_reached":last_data_reached or detail_last_data_reached,
            "data_count":len(view_data),"saved_file":filepath}

async def execute_bill_scraping_test(req: ScrapeRequest):
    p = req.param; view_data=[]; has_file_item=any(i.col in {"BI_FILE_NM","BI_FILE_URL"} for i in req.item)
    async with async_playwright() as playwright:
        browser, page = await _setup_browser(playwright)
        try:
            print(f"[TEST] 리스트 수집: {p.list_url}", flush=True)
            await page.goto(p.list_url, wait_until="domcontentloaded", timeout=int(p.timeout))
            if p.rasmbly_numpr and p.rasmbly_numpr.strip():
                await BillListCrawler.apply_filter_and_search(page, p.rasmbly_numpr.strip(), p.list_class,
                    p.search_form_selector, p.numpr_select_selector, p.search_btn_selector)
            else:
                await page.wait_for_selector(normalize_selector(p.list_class), timeout=3000)
            list_data = await BillListCrawler.extract_list_page(page, p.list_class, p.view_id_param)
            if not list_data: return {"req_id":req.req_id,"type":req.type,"crw_id":req.crw_id,"file_dir":req.file_dir,"data":[]}
            item = list_data[0]; vid = item.get("view_id")
            if vid:
                href      = item.get("link_href","")
                is_real   = href and not href.startswith(("#","javascript"))
                target_url = urljoin(p.list_url, href) if is_real else f"{p.view_url}{'&' if '?' in p.view_url else '?'}{p.view_id_param}={vid}"
                print(f"[TEST] 상세 수집: {vid}", flush=True)
                try:
                    detail_html = await _extract_bill_detail_html(page, p.view_class, target_url, timeout=int(p.timeout))
                    bi_cn       = str(time.time_ns())[:16]
                    detail      = parse_detail_by_items(detail_html, req.item, list_title=item.get("BI_SJ",""))
                    if not detail.get("RASMBLY_NUMPR") and p.rasmbly_numpr: detail["RASMBLY_NUMPR"]=str(p.rasmbly_numpr)
                    parsed_u    = urlparse(target_url); base = f"{parsed_u.scheme}://{parsed_u.netloc}"
                    if has_file_item and not detail.get("BI_FILE_PATH"):
                        year = (detail.get("ITNC_DE") or "")[:4] or str(datetime.now().year)
                        try:
                            file_result = await _extract_bill_attachments(page, p.view_class, base, req, bi_cn, year, items=req.item)
                        except Exception as e:
                            print(f"    [!] 첨부파일 수집 실패: {str(e)[:100]}", flush=True); file_result={}
                            try: await page.goto(target_url, wait_until="domcontentloaded", timeout=int(p.timeout))
                            except Exception: pass
                        detail.update({
                            "BI_FILE_NM":   file_result.get("BI_FILE_NM",   ""),
                            "BI_FILE_PATH": file_result.get("BI_FILE_PATH", ""),
                            "BI_FILE_ID":   file_result.get("BI_FILE_ID",   ""),
                            "BI_FILE_URL":  file_result.get("BI_FILE_URL",  ""),
                        })
                    view_data.append({"view_id":vid,"view_url":target_url,"BI_CN":f"CLIKC{bi_cn}",**detail})
                except Exception as e: view_data.append({"view_id":vid,"view_url":target_url,"view_error":str(e)})
        except Exception as e: print(f"[TEST] 에러: {e}", flush=True)
        finally: await browser.close()
    view_data.reverse()
    return {"req_id":req.req_id,"type":req.type,"crw_id":req.crw_id,"file_dir":req.file_dir,"data":view_data}

# ── policy 실행 엔진 ──────────────────────────────────────────────
async def execute_policy_scraping(req: PolicyRequest):
    app.state.stop_scraping = False
    p        = req.param
    last_sig = _build_last_data_signature(req.last_data) if req.last_data else None
    list_data, view_data, error_logs, field_logs = [], [], [], []
    filepath      = None
    has_file_item = any(i.col in _POLICY_FILE_COLS for i in req.item)
    if last_sig: print(f"[POLICY] 추가수집 모드: {last_sig}", flush=True)

    async with async_playwright() as playwright:
        print(f"\n{'='*60}\n[POLICY] [1단계] 리스트: {p.list_url}", flush=True)
        browser, page = await _setup_browser(playwright)
        try:
            list_data, collect_errors, last_data_reached = await _collect_pages(
                page, p.list_url, numpr="", list_class=p.list_class, vid_param=p.view_id_param,
                max_pages=p.max_pages, paging_sel=p.paging_selector,
                next_btn_sel=p.next_btn_selector, end_btn_sel=p.end_btn_selector,
                stop_check=lambda: app.state.stop_scraping,
                search_form_selector="", numpr_select_selector="", search_btn_selector="",
                last_sig=last_sig, timeout=int(p.timeout))
        except Exception as e:
            print(f"[POLICY] 리스트 수집 에러: {e}", flush=True)
            list_data, collect_errors, last_data_reached = [], [], False
        finally:
            await browser.close(); print("[POLICY] 1단계 브라우저 종료", flush=True)

        error_logs.extend(collect_errors)
        if not list_data:
            error_logs.append({"step":"1단계_리스트수집","url":p.list_url,"selector":p.list_class,"error":"리스트 수집 결과 0건"})

        total = len(list_data)
        print(f"\n[POLICY] [2단계] 상세 수집 ({total}건)\n{'-'*60}", flush=True)
        detail_last_data_reached = False

        browser, page = await _setup_browser(playwright)
        try:
            for idx, list_item in enumerate(list_data):
                if app.state.stop_scraping: print(f"[POLICY] 중단 요청: {idx}번째", flush=True); break
                vid = list_item.get("view_id")
                if not vid: continue
                print(f"[POLICY] 상세 ({idx+1}/{total}) ID: {vid}", flush=True)
                href       = list_item.get("link_href","")
                is_real    = href and not href.startswith(("#","javascript"))
                target_url = (urljoin(p.list_url, href) if is_real
                              else f"{p.view_url}{'&' if '?' in (p.view_url or '') else '?'}{p.view_id_param}={vid}")
                try:
                    detail_html = await _extract_bill_detail_html(page, p.view_class, target_url, timeout=int(p.timeout))
                    parsed_u    = urlparse(target_url); base_url = f"{parsed_u.scheme}://{parsed_u.netloc}"
                    outbbs_cn   = f"CLIKC{str(time.time_ns())[:16]}"
                    list_title  = list_item.get("BI_SJ","")
                    detail      = parse_policy_detail(detail_html, req.item, list_title=list_title)
                    year        = (detail.get("CDATE") or "")[:4] or str(datetime.now().year)

                    if has_file_item:
                        try:
                            file_result = await _extract_policy_attachments(
                                page, p.view_class, base_url, req, outbbs_cn, year, items=req.item)
                        except Exception as e:
                            print(f"    [!] 첨부파일 수집 실패: {str(e)[:100]}", flush=True); file_result={}
                            try: await page.goto(target_url, wait_until="domcontentloaded", timeout=int(p.timeout))
                            except Exception: pass
                        detail.update({k: file_result.get(k,"") for k in ("ORG_FILE_NM","DOWNPATH","DOWNID","DOWNURL","FILE_TYPE")})

                    img_urls = [u for u in re.findall(r'<img[^>]+src="([^"]+)"', detail_html, re.IGNORECASE)
                                if not any(k in u for k in ["/resource/","/koglOpen/","btn_preview","pdf.png",
                                    "synap","opentype","/images/common/","/images/sub/","/images/board/","ico_"])]
                    if img_urls:
                        img_results = []
                        for img_seq, img_url in enumerate(img_urls, start=1):
                            full_img_url = urljoin(base_url, img_url)
                            img_param    = dict(parse_qsl(urlparse(full_img_url).query)).get("img","")
                            _, ext       = os.path.splitext(img_param or urlparse(full_img_url).path)
                            if not ext: ext = ".png"
                            img_path = build_policy_save_path(req, year, outbbs_cn, img_seq, ext).replace("_attach_","_img_")
                            try:
                                async with httpx.AsyncClient(headers={"User-Agent":USER_AGENT},
                                        timeout=httpx.Timeout(30.0), verify=certifi.where()) as client:
                                    r = await client.get(full_img_url); r.raise_for_status()
                                with open(img_path,"wb") as f: f.write(r.content)
                                img_results.append({"url":full_img_url,"path":img_path.replace("\\","/")})
                                print(f"[+] [POLICY] 이미지 저장: {img_path}", flush=True)
                            except Exception as e:
                                print(f"[-] [POLICY] 이미지 실패 ({full_img_url}): {e}", flush=True)
                        if img_results:
                            def to_list(v):
                                if not v: return []
                                try: parsed = json.loads(v); return parsed if isinstance(parsed, list) else [str(parsed)]
                                except: return [v] if v else []
                            nms   = to_list(detail.get("ORG_FILE_NM","") or "")
                            paths = to_list(detail.get("DOWNPATH","")    or "")
                            ids   = to_list(detail.get("DOWNID","")      or "")
                            urls  = to_list(detail.get("DOWNURL","")     or "")
                            types = to_list(detail.get("FILE_TYPE","")   or "")
                            for img in img_results:
                                nms.append(os.path.basename(img["path"])); paths.append(img["path"])
                                ids.append(str(len(ids)+1)); urls.append(img["url"]); types.append("I")
                            def one_or_json(lst): return lst[0] if len(lst)==1 else json.dumps(lst, ensure_ascii=False)
                            detail.update({"ORG_FILE_NM":one_or_json(nms),"DOWNPATH":one_or_json(paths),
                                           "DOWNID":one_or_json(ids),"DOWNURL":one_or_json(urls),
                                           "FILE_TYPE":one_or_json(types)})
                            detail.pop("IMAGEPATH", None)

                    collected_item = {"view_id":vid,"URL":target_url,"SEEDURL":p.list_url,"OUTBBS_CN":outbbs_cn,**detail}
                    field_logs.append(audit_fields(vid, target_url, outbbs_cn, collected_item, req.item))
                    if last_sig and not detail_last_data_reached and is_last_data_match(collected_item, last_sig):
                        detail_last_data_reached=True; print("[POLICY] 상세 기준점 도달", flush=True); break
                    view_data.append(collected_item)
                except Exception as e:
                    print(f"    [!] ID: {vid} 실패: {e}", flush=True)
                    view_data.append({"view_id":vid,"URL":target_url,"view_error":str(e)})
                    error_logs.append({"step":"2단계_상세수집","view_id":vid,"url":target_url,"error":str(e)})
        except Exception as e:
            print(f"\n[POLICY] 상세 수집 전체 에러: {e}", flush=True)
            return {"req_id":req.req_id,"type":req.type,"crw_id":req.crw_id,"file_dir":req.file_dir,"ok":False,"error_msg":str(e)}
        finally: await browser.close()

    is_interrupted = app.state.stop_scraping or last_data_reached or detail_last_data_reached
    result_block   = _build_result(view_data, error_logs, is_interrupted)
    if (last_data_reached or detail_last_data_reached) and result_block["status"] in ("SUCCESS","PARTIAL"):
        result_block["message"] = "추가수집 완료 (last_data 기준점 도달)"
    if field_logs: save_field_logs(field_logs, req)
    view_data.reverse()
    full_payload = {"reqId":req.req_id,"type":req.type,"crwId":req.crw_id,"fileDir":req.file_dir,
                    "result":result_block,"data":view_data,"log":error_logs}
    domain = extract_domain(p.list_url)
    if view_data:
        filepath = save_to_json(full_payload, domain, req.type)
        print(f"[POLICY] 저장 완료 ({len(view_data)}건): {filepath}", flush=True)
    else:
        filepath = save_to_json(full_payload, domain, req.type+"_error")
        print(f"[POLICY] 에러 로그 {len(error_logs)}건: {filepath}", flush=True)
    await _do_send(INSERT_API_URL, full_payload)
    return {"req_id":req.req_id,"type":req.type,"crw_id":req.crw_id,"file_dir":req.file_dir,
            "ok":True,"interrupted":is_interrupted,
            "last_data_reached":last_data_reached or detail_last_data_reached,
            "data_count":len(view_data),"saved_file":filepath}

async def execute_policy_scraping_test(req: PolicyRequest):
    p = req.param; view_data=[]; has_file_item=any(i.col in _POLICY_FILE_COLS for i in req.item)
    async with async_playwright() as playwright:
        browser, page = await _setup_browser(playwright)
        try:
            print(f"[POLICY TEST] 리스트: {p.list_url}", flush=True)
            await page.goto(p.list_url, wait_until="domcontentloaded", timeout=int(p.timeout))
            await page.wait_for_selector(normalize_selector(p.list_class), timeout=int(p.timeout))
            list_data = await BillListCrawler.extract_list_page(page, p.list_class, p.view_id_param)
            if not list_data:
                return {"req_id":req.req_id,"type":req.type,"crw_id":req.crw_id,"file_dir":req.file_dir,"data":[]}
            item = list_data[0]; vid = item.get("view_id")
            if not vid:
                return {"req_id":req.req_id,"type":req.type,"crw_id":req.crw_id,"file_dir":req.file_dir,"data":[]}
            href       = item.get("link_href","")
            is_real    = href and not href.startswith(("#","javascript"))
            target_url = (urljoin(p.list_url, href) if is_real
                          else f"{p.view_url}{'&' if '?' in (p.view_url or '') else '?'}{p.view_id_param}={vid}")
            print(f"[POLICY TEST] 상세: {vid}", flush=True)
            try:
                detail_html = await _extract_bill_detail_html(page, p.view_class, target_url, timeout=int(p.timeout))
                parsed_u    = urlparse(target_url); base_url = f"{parsed_u.scheme}://{parsed_u.netloc}"
                outbbs_cn   = f"CLIKC{str(time.time_ns())[:16]}"
                list_title  = item.get("BI_SJ","")
                detail      = parse_policy_detail(detail_html, req.item, list_title=list_title)
                year        = (detail.get("CDATE") or "")[:4] or str(datetime.now().year)
                if has_file_item:
                    try:
                        file_result = await _extract_policy_attachments(
                            page, p.view_class, base_url, req, outbbs_cn, year, items=req.item)
                    except Exception as e:
                        print(f"    [!] 첨부파일 실패: {str(e)[:100]}", flush=True); file_result={}
                        try: await page.goto(target_url, wait_until="domcontentloaded", timeout=int(p.timeout))
                        except Exception: pass
                    detail.update({k: file_result.get(k,"") for k in ("ORG_FILE_NM","DOWNPATH","DOWNID","DOWNURL","FILE_TYPE")})

                img_urls = [u for u in re.findall(r'<img[^>]+src="([^"]+)"', detail_html, re.IGNORECASE)
                            if not any(k in u for k in ["/resource/","/koglOpen/","btn_preview","pdf.png",
                                "synap","opentype","/images/common/","/images/sub/"])]
                if img_urls:
                    img_results = []
                    for img_seq, img_url in enumerate(img_urls, start=1):
                        full_img_url = urljoin(base_url, img_url)
                        img_param    = dict(parse_qsl(urlparse(full_img_url).query)).get("img","")
                        _, ext       = os.path.splitext(img_param or urlparse(full_img_url).path)
                        if not ext: ext = ".png"
                        img_path = build_policy_save_path(req, year, outbbs_cn, img_seq, ext).replace("_attach_","_img_")
                        try:
                            async with httpx.AsyncClient(headers={"User-Agent":USER_AGENT},
                                    timeout=httpx.Timeout(30.0), verify=certifi.where()) as client:
                                r = await client.get(full_img_url); r.raise_for_status()
                            with open(img_path,"wb") as f: f.write(r.content)
                            img_results.append({"url":full_img_url,"path":img_path.replace("\\","/")})
                        except Exception as e:
                            print(f"[-] [POLICY TEST] 이미지 실패 ({full_img_url}): {e}", flush=True)
                    if img_results:
                        def to_list(v):
                            if not v: return []
                            try: parsed = json.loads(v); return parsed if isinstance(parsed, list) else [str(parsed)]
                            except: return [v] if v else []
                        nms   = to_list(detail.get("ORG_FILE_NM","") or "")
                        paths = to_list(detail.get("DOWNPATH","")    or "")
                        ids   = to_list(detail.get("DOWNID","")      or "")
                        urls  = to_list(detail.get("DOWNURL","")     or "")
                        types = to_list(detail.get("FILE_TYPE","")   or "")
                        for img in img_results:
                            nms.append(os.path.basename(img["path"])); paths.append(img["path"])
                            ids.append(str(len(ids)+1)); urls.append(img["url"]); types.append("I")
                        def one_or_json(lst): return lst[0] if len(lst)==1 else json.dumps(lst, ensure_ascii=False)
                        detail.update({"ORG_FILE_NM":one_or_json(nms),"DOWNPATH":one_or_json(paths),
                                       "DOWNID":one_or_json(ids),"DOWNURL":one_or_json(urls),
                                       "FILE_TYPE":one_or_json(types)})
                        detail.pop("IMAGEPATH", None)
                view_data.append({"view_id":vid,"URL":target_url,"SEEDURL":p.list_url,"OUTBBS_CN":outbbs_cn,**detail})
            except Exception as e:
                view_data.append({"view_id":vid,"URL":target_url,"view_error":str(e)})
        except Exception as e: print(f"[POLICY TEST] 에러: {e}", flush=True)
        finally: await browser.close()
    view_data.reverse()
    return {"req_id":req.req_id,"type":req.type,"crw_id":req.crw_id,"file_dir":req.file_dir,"data":view_data}

# ── prism 실행 엔진 ───────────────────────────────────────────────
async def execute_prism_scraping(req: PrismRequest):
    app.state.stop_scraping = False
    p        = req.param
    last_sig = _build_last_data_signature(req.last_data) if req.last_data else None
    list_data, view_data, error_logs, field_logs = [], [], [], []
    if last_sig: print(f"[PRISM] 추가수집 모드: {last_sig}", flush=True)

    print(f"\n{'='*60}\n[PRISM] [1단계] 리스트: {p.list_url}", flush=True)
    if p.list_api_url:
        print(f"[PRISM] 리스트 API 방식: {p.list_api_url}", flush=True)
        list_data, collect_errors, last_data_reached = await _fetch_prism_list_api(
            p.list_url, p.list_api_url, p.max_pages, lambda: app.state.stop_scraping, last_sig, int(p.timeout))
    else:
        async with async_playwright() as pw:
            browser = await pw.chromium.launch(headless=True)
            page    = await browser.new_page()
            await page.set_extra_http_headers({"User-Agent": USER_AGENT})
            try:
                list_data, collect_errors, last_data_reached = await _collect_prism_list(
                    page, p.list_url, None, p.max_pages, lambda: app.state.stop_scraping, last_sig, int(p.timeout))
            except Exception as e:
                print(f"[PRISM] 리스트 수집 에러: {e}", flush=True)
                list_data, collect_errors, last_data_reached = [], [], False
            finally: await browser.close()

    print("[PRISM] 1단계 완료", flush=True)
    error_logs.extend(collect_errors)
    if not list_data:
        error_logs.append({"step":"1단계_리스트수집","url":p.list_url,"error":"리스트 수집 결과 0건"})

    total = len(list_data)
    print(f"\n[PRISM] [2단계] 상세 수집 ({total}건)\n{'-'*60}", flush=True)
    detail_last_data_reached = False

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)
        for idx, list_item in enumerate(list_data):
            if app.state.stop_scraping: print(f"[PRISM] 중단: {idx}번째", flush=True); break
            vid        = list_item.get("view_id")
            target_url = list_item.get("link_href","")
            list_title = list_item.get("BI_SJ","")
            if not vid: continue
            print(f"[PRISM] 상세 ({idx+1}/{total}) ID: {vid}", flush=True)
            try:
                seq_id = f"CLIKC{str(time.time_ns())[:16]}"
                year   = str(datetime.now().year)
                page   = await browser.new_page()
                await page.set_extra_http_headers({"User-Agent": USER_AGENT})
                try:
                    await page.goto(target_url, wait_until="domcontentloaded", timeout=int(p.timeout))
                    try: await page.wait_for_selector("div.file_list", timeout=int(p.timeout))
                    except Exception: await page.wait_for_timeout(5000)
                    el          = await page.query_selector("div.contents_area")
                    detail_html = await el.inner_html() if el else ""
                    detail      = _parse_prism_detail(detail_html, list_title=list_title)
                    year        = detail.get("RSRC_YEAR") or year
                    file_result = {}
                    if detail_html:
                        parsed_u = urlparse(target_url); base_url = f"{parsed_u.scheme}://{parsed_u.netloc}"
                        try:
                            file_result = await _extract_prism_attachments(
                                page, "div.contents_area", base_url, req, seq_id, year, items=req.item)
                        except Exception as e:
                            print(f"    [!] 첨부파일 실패: {str(e)[:100]}", flush=True)
                finally: await page.close()
                detail.update({k: file_result.get(k,"") for k in ("ORIGNL_FILE_NM","SYS_FILE_NM","FILE_SEQ","RPRT_TYPE")})
                collected_item = {"view_id":vid,"URL":target_url,"SEQ":seq_id,"BBS_ID":req.bbs_id,**detail}
                field_logs.append(audit_fields(vid, target_url, seq_id, collected_item, req.item))
                if last_sig and not detail_last_data_reached and is_last_data_match(collected_item, last_sig):
                    detail_last_data_reached=True; print("[PRISM] 상세 기준점 도달", flush=True); break
                view_data.append(collected_item)
            except Exception as e:
                print(f"    [!] ID: {vid} 실패: {e}", flush=True)
                view_data.append({"view_id":vid,"URL":target_url,"view_error":str(e)})
                error_logs.append({"step":"2단계_상세수집","view_id":vid,"url":target_url,"error":str(e)})
        await browser.close()

    is_interrupted = app.state.stop_scraping or last_data_reached or detail_last_data_reached
    result_block   = _build_result(view_data, error_logs, is_interrupted)
    if (last_data_reached or detail_last_data_reached) and result_block["status"] in ("SUCCESS","PARTIAL"):
        result_block["message"] = "추가수집 완료 (last_data 기준점 도달)"
    if field_logs: save_field_logs(field_logs, req)
    view_data.reverse()
    full_payload = {"reqId":req.req_id,"type":req.type,"crwId":req.crw_id,"fileDir":req.file_dir,
                    "result":result_block,"data":view_data,"log":error_logs}
    domain = extract_domain(p.list_url)
    if view_data:
        filepath = save_to_json(full_payload, domain, req.type)
        print(f"[PRISM] 저장 완료 ({len(view_data)}건): {filepath}", flush=True)
    else:
        filepath = save_to_json(full_payload, domain, req.type+"_error")
        print(f"[PRISM] 에러 로그 {len(error_logs)}건: {filepath}", flush=True)
    await _do_send(INSERT_API_URL, full_payload)
    return {"req_id":req.req_id,"type":req.type,"crw_id":req.crw_id,"file_dir":req.file_dir,
            "ok":True,"interrupted":is_interrupted,
            "last_data_reached":last_data_reached or detail_last_data_reached,
            "data_count":len(view_data),"saved_file":filepath}

async def execute_prism_scraping_test(req: PrismRequest):
    p = req.param; view_data=[]
 
    if p.list_api_url:
        list_data, _, _ = await _fetch_prism_list_api(p.list_url, p.list_api_url, "1", timeout=int(p.timeout))
    else:
        async with async_playwright() as pw:
            browser = await pw.chromium.launch(headless=True)
            page    = await browser.new_page()
            await page.set_extra_http_headers({"User-Agent": USER_AGENT})
            try:
                list_data, _, _ = await _collect_prism_list(page, p.list_url, None, "1", timeout=int(p.timeout))
            except Exception as e:
                print(f"[PRISM TEST] 에러: {e}", flush=True); list_data=[]
            finally: await browser.close()
 
    if not list_data:
        return {"req_id":req.req_id,"type":req.type,"crw_id":req.crw_id,"file_dir":req.file_dir,"data":[]}
 
    item       = list_data[0]
    vid        = item.get("view_id")
    target_url = item.get("link_href","")
    list_title = item.get("BI_SJ","")
    seq_id     = f"CLIKC{str(time.time_ns())[:16]}"
    year       = str(datetime.now().year)
 
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)
        page    = await browser.new_page()
        await page.set_extra_http_headers({"User-Agent": USER_AGENT})
        try:
            print(f"[PRISM TEST] 상세 수집: {vid}", flush=True)
            await page.goto(target_url, wait_until="domcontentloaded", timeout=int(p.timeout))
            try: await page.wait_for_selector("div.file_list", timeout=int(p.timeout))
            except Exception: await page.wait_for_timeout(5000)
 
            el          = await page.query_selector("div.contents_area")
            detail_html = await el.inner_html() if el else ""
            detail      = _parse_prism_detail(detail_html, list_title=list_title)
            year        = detail.get("RSRC_YEAR") or year
            file_result = {}
 
            if detail_html:
                parsed_u = urlparse(target_url); base_url = f"{parsed_u.scheme}://{parsed_u.netloc}"
                try:
                    file_result = await _extract_prism_attachments(
                        page, "div.contents_area", base_url, req, seq_id, year, items=req.item)
                except Exception as e:
                    print(f"    [!] 첨부파일 실패: {str(e)[:100]}", flush=True)
 
            detail.update({k: file_result.get(k,"") for k in ("ORIGNL_FILE_NM","SYS_FILE_NM","FILE_SEQ","RPRT_TYPE")})
            view_data.append({"view_id":vid,"URL":target_url,"SEQ":seq_id,"BBS_ID":req.bbs_id,**detail})
 
        except Exception as e:
            print(f"[PRISM TEST] 상세 에러: {e}", flush=True)
            view_data.append({"view_id":vid,"URL":target_url,"view_error":str(e)})
        finally:
            await browser.close()
 
    view_data.reverse()
    return {"req_id":req.req_id,"type":req.type,"crw_id":req.crw_id,"file_dir":req.file_dir,"data":view_data}

# ── FastAPI 라우터 ────────────────────────────────────────────────
def _route_request(raw: UnifiedRequest):
    extra     = raw.model_extra or {}
    last_data = LastData(**raw.last_data) if raw.last_data else None
    if "prism" in raw.type:
        return PrismRequest(req_id=raw.req_id, type=raw.type, crw_id=raw.crw_id,
                            file_dir=raw.file_dir, bbs_id=extra.get("bbs_id","0"),
                            param=PrismParam(**raw.param), item=raw.item, last_data=last_data)
    if "policy" in raw.type:
        return PolicyRequest(req_id=raw.req_id, type=raw.type, crw_id=raw.crw_id,
                            file_dir=raw.file_dir, bbs_id=extra.get("bbs_id","0"),
                            param=PolicyParam(**raw.param), item=raw.item, last_data=last_data)
    if "bill" in raw.type:
        return ScrapeRequest(req_id=raw.req_id, type=raw.type, crw_id=raw.crw_id,
                            file_dir=raw.file_dir, param=ScrapeParam(**raw.param),
                            item=raw.item, last_data=last_data)
    raise ValueError(f"지원하지 않는 type: '{raw.type}' (bill / policy / prism 중 하나여야 합니다)")

@app.post("/crawl", status_code=202)
async def crawl(raw: UnifiedRequest, background_tasks: BackgroundTasks):
    try:
        req = _route_request(raw)
        if   isinstance(req, PrismRequest):  background_tasks.add_task(execute_prism_scraping,  req)
        elif isinstance(req, PolicyRequest): background_tasks.add_task(execute_policy_scraping, req)
        else:                                background_tasks.add_task(execute_bill_scraping,   req)
        return {"req_id":raw.req_id,"type":raw.type,"crw_id":raw.crw_id,"file_dir":raw.file_dir,"ok":True,"message":"수집 요청 완료"}
    except Exception as e: return error_response(f"요청 처리 중 오류: {e}")

@app.post("/crawl/test")
async def crawl_test(raw: UnifiedRequest):
    try:
        req = _route_request(raw)
        if   isinstance(req, PrismRequest):  return await execute_prism_scraping_test(req)
        elif isinstance(req, PolicyRequest): return await execute_policy_scraping_test(req)
        return await execute_bill_scraping_test(req)
    except Exception as e: return error_response(f"테스트 요청 오류: {e}")

@app.get("/crawl/stop")
async def stop_crawl():
    app.state.stop_scraping = True
    print("[!] stop_scraping = True", flush=True)
    return {"ok": True, "message": "수집 중단 요청 완료"}

@app.get("/health")
async def health():
    return {"status": "ok", "time": datetime.now().isoformat()}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8900)