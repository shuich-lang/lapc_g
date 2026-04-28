from __future__ import annotations
import asyncio, os, re, sys, time, traceback, json
from datetime import datetime
from typing import Any, Dict, List, Optional
from urllib.parse import parse_qsl, urlencode, urljoin, urlparse, urlunparse, unquote
from uuid import uuid4
import certifi, httpx
from bs4 import BeautifulSoup
from fastapi import FastAPI, BackgroundTasks, HTTPException
from fastapi.responses import JSONResponse
from playwright.async_api import async_playwright, Page, TimeoutError as PlaywrightTimeoutError
from pydantic import BaseModel, Field, HttpUrl, field_validator, ValidationInfo

if sys.platform.startswith("win"):
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

app = FastAPI(title="Council Crawler API")
app.state.stop_scraping = False

DOWNLOAD_DIR, FILE_DOWNLOAD_DIR, FIELD_LOGS_DIR = "download", "attachment", "field_logs"
INSERT_API_URL = "http://10.201.38.157:8080/insert_api.do"
USER_AGENT = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
              "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36")
BLOCKED_RESOURCES     = {"image", "stylesheet", "media", "font"}
VIEW_ID_AUTO_PARAMS   = r"[?&](uid|idx|code|no|seq|id|bill_no|billNo|idx_no|nttId|uuid)=([^&]+)"
PAGE_PARAM_PATTERN    = r'([?&](?:page|pageIndex|p|page_no|pageno|cPage|pageNum|page_id|cp))=(\d+)'
FILE_EXTENSIONS       = ("pdf", "hwp", "hwpx", "doc", "docx", "xls", "xlsx", "zip")
_MAX_CONSECUTIVE_FAIL = 5
_DATE_PATTERN         = re.compile(r'(\d{4})[./\-](\d{1,2})[./\-](\d{1,2})')
_ATTACHMENT_COLS      = {"BI_FILE_NM", "BI_FILE_PATH", "BI_FILE_ID", "BI_FILE_URL"}
_LAST_DATA_MATCH_KEYS = ["URL", "BI_SJ", "BI_CN", "BI_NO"]

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

class MinutesParam(BaseModel):
    list_url:           HttpUrl       = Field(...)
    list_root_selector: str           = Field(...)
    item_selector:      str           = Field(...)
    target_selector:    str           = Field(...)
    ssl_mode:           str           = Field("Y")
    max_pages:          int           = Field(500)
    rasmbly_numpr:      Optional[str] = None
    skip_top_count:     int           = Field(0)

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

class MinutesRequest(BaseModel):
    req_id:    str                               = Field(...)
    crw_id:    Optional[str]                     = None
    type:      str                               = Field(...)
    last_data: Optional[Dict[str, Optional[str]]] = None
    file_dir:  str                               = Field("")
    param:     MinutesParam                      = Field(...)
    item:      List[RegexItem]                   = Field(default_factory=list)

class MinutesItem(BaseModel):
    rank:                  int
    list_title:            str
    detail_url:            Optional[str] = None
    access_method:         str
    open_type:             Optional[str] = None
    detail_access_success: bool
    fields:                Dict[str, Optional[str]] = Field(default_factory=dict)
    uid:                   Optional[str] = None
    mints_cn:              Optional[str] = None
    raw_href:              Optional[str] = None
    raw_onclick:           Optional[str] = None
    note:                  Optional[str] = None

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

def extract_year_from_date(date_str):
    if not date_str: return "0000"
    n = normalize_date_to_yyyymmdd(date_str)
    return n[:4] if n and len(n) >= 4 and n[:4].isdigit() else "0000"

def get_verify_options(ssl_mode):
    if ssl_mode == "Y": return certifi.where()
    if ssl_mode == "N": return False
    raise ValueError(f"Invalid SSL mode: {ssl_mode}")

def _build_result(data, log, interrupted, error=""):
    has_timeout = any("Timeout" in (e.get("error") or "") for e in log)
    has_error   = any(e.get("error") for e in log)
    n = len(data)
    if error:                                    s,c,m = "FAILED","500",f"수집 실패: {error}"
    elif n==0 and has_timeout:                   s,c,m = "TIMEOUT","408","타임아웃으로 수집 불가"
    elif n==0:                                   s,c,m = "EMPTY","204","수집 결과 없음"
    elif interrupted or has_timeout or has_error: s,c,m = "PARTIAL","206","일부 수집 완료"
    else:                                        s,c,m = "SUCCESS","200","수집 완료"
    return {"status":s,"code":c,"message":m,"dataCount":n,"interrupted":interrupted}

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
        result[key] = value if value is not None else ""
    return result

def audit_fields(view_id, url, bi_cn, item_data, items):
    expected = {normalize_text(i.col) for i in items if normalize_text(i.col)}
    collected, empty, missing = [], [], []
    for key in sorted(expected):
        if key not in item_data:                   missing.append(key)
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

def matches_last_data_minutes(item_fields, item_detail_url, item_mints_cn, last_data):
    if not last_data: return False
    source = dict(item_fields or {})
    if item_detail_url is not None: source["url"] = item_detail_url
    if item_mints_cn   is not None: source["mints_cn"] = item_mints_cn
    for key, expected in last_data.items():
        if normalize_text(str(expected) if expected else "") != normalize_text(str(source.get(key)) if source.get(key) else ""):
            return False
    print("[MATCH] last_data 완전 일치!", flush=True); return True

# ── 파일 저장 경로 ────────────────────────────────────────────────
def build_save_path(req, year, bi_cn, seq, ext):
    rasmbly = req.param.rasmbly_numpr or "0"
    path = os.path.join("/", req.file_dir, req.type, req.crw_id, rasmbly, year, f"CLIKC{bi_cn}_{seq}{ext}")
    os.makedirs(os.path.dirname(path), exist_ok=True)
    return path

def build_file_save_path(file_dir, crawl_type, crw_id, rasmbly_numpr, year, mints_cn, seq, original_filename):
    ext = ""
    if original_filename and "." in original_filename: ext = original_filename.rsplit(".",1)[-1].lower()
    if ext not in FILE_EXTENSIONS: ext = "bin"
    safe = re.sub(r'[\\/:*?"<>|\s]+', "_", normalize_text(rasmbly_numpr) if rasmbly_numpr else "unknown")
    return os.path.join(file_dir, crawl_type, crw_id or "unknown", safe, year, f"{mints_cn}_{seq}.{ext}")

# ── HTTP 유틸 ─────────────────────────────────────────────────────
async def fetch_html(url, ssl_mode):
    async with httpx.AsyncClient(headers={"User-Agent":USER_AGENT}, timeout=httpx.Timeout(20.0,connect=10.0),
                                  follow_redirects=True, verify=get_verify_options(ssl_mode)) as client:
        r = await client.get(url); r.raise_for_status(); return r.text

async def fetch_html_post(url, ssl_mode, form_data=None):
    async with httpx.AsyncClient(headers={"User-Agent":USER_AGENT}, timeout=httpx.Timeout(20.0,connect=10.0),
                                  follow_redirects=True, verify=get_verify_options(ssl_mode)) as client:
        r = await client.post(url, data=form_data or {}); r.raise_for_status(); return r.text

# ── minutes 목록 파싱 ─────────────────────────────────────────────
def safe_select_one(el, sel):
    try: return el.select_one(sel)
    except Exception: return None

def safe_select(el, sel):
    try: return el.select(sel)
    except Exception: return []

def is_javascript_href(href): return bool(href and href.strip().lower().startswith("javascript:"))
def is_http_like_href(href):
    if not href: return False
    return href.strip().lower().startswith(("http://","https://","/","../","./"))
def is_meaningful_detail_url(detail_url, list_url):
    if not detail_url: return False
    n = detail_url.strip()
    return bool(n) and not n.lower().startswith("javascript:") and n != list_url.strip()

def extract_uid(detail_url):
    if not detail_url: return None
    try:
        parsed = urlparse(detail_url)
        pairs  = parse_qsl(parsed.query, keep_blank_values=True)
        exclude = {"page","pageNo","pageNum","pageIndex","currentPage","search","keyword"}
        for pk in ["uid","key","id","seq"]:
            for k,v in pairs:
                if k==pk and normalize_text(v): return normalize_text(v)
        for k,v in pairs:
            if k not in exclude and normalize_text(v): return normalize_text(v)
        m = re.search(r"/(\d+)\.do(?:$|\?)", parsed.path or "")
        if m: return m.group(1)
        segs = [s for s in (parsed.path or "").split("/") if s]
        if segs:
            m = re.fullmatch(r"(\d+)", segs[-1])
            if m: return m.group(1)
        return None
    except Exception: return None

def extract_rasmbly_numpr(text):
    for p in [r"제\s*(\d+)\s*대", r"(\d+)\s*대"]:
        m = re.search(p, text)
        if m: return m.group(1)
    return None

def replace_query_param(url, name, value):
    parsed = urlparse(url)
    pairs  = parse_qsl(parsed.query, keep_blank_values=True)
    new, replaced = [], False
    for k,v in pairs:
        if k==name: new.append((k,value)); replaced=True
        else:       new.append((k,v))
    if not replaced: new.append((name,value))
    return urlunparse((parsed.scheme,parsed.netloc,parsed.path,parsed.params,urlencode(new),parsed.fragment))

def extract_list_candidates(html, list_root_selector, item_selector, target_selector, limit=5):
    soup = BeautifulSoup(html, "lxml")
    root = safe_select_one(soup, list_root_selector)
    if not root: return []
    results = []
    for item in safe_select(root, item_selector):
        target   = item if target_selector=="self" else safe_select_one(item, target_selector)
        if not target: continue
        title    = normalize_text(target.get_text(" ", strip=True))
        href     = normalize_text(target.get("href"))
        onclick  = normalize_text(target.get("onclick"))
        row_text = normalize_text(item.get_text(" ", strip=True))
        if not title: title = row_text
        if not title: continue
        results.append({"title":title,"href":href or None,"onclick":onclick or None,
                        "row_text":row_text,"rasmbly_numpr":extract_rasmbly_numpr(row_text)})
    return results[:limit] if limit else results

def extract_link_paging_info(html, list_url):
    soup = BeautifulSoup(html, "lxml")
    page_numbers, counter = {1}, {}
    cands = ["page","pageNo","pageNum","pageIndex","currentPage"]
    for a in soup.find_all("a"):
        href = normalize_text(a.get("href"))
        if not href or href.lower().startswith("javascript:"): continue
        for k,v in parse_qsl(urlparse(urljoin(list_url,href)).query, keep_blank_values=True):
            if k in cands and v.isdigit():
                page_numbers.add(int(v)); counter[k] = counter.get(k,0)+1
    if len(page_numbers)<=1: return None,[1]
    best = max(counter,key=counter.get) if counter else None
    return best, sorted(page_numbers)

def extract_form_request_info(html, list_url):
    soup = BeautifulSoup(html, "lxml")
    page_numbers = {1}
    for m in re.findall(r"fnActRetrieve\((\d+)\)", html):
        if m.isdigit(): page_numbers.add(int(m))
    form = safe_select_one(soup,"#frmDefault")
    if not form:
        for f in soup.find_all("form"):
            if f.find(attrs={"name":"pageCurNo"}): form=f; break
    if not form: return None,{},None,[1]
    action_url = urljoin(list_url, normalize_text(form.get("action")) or "")
    form_data = {normalize_text(inp.get("name")):normalize_text(inp.get("value"))
                 for inp in form.find_all(["input","select","textarea"]) if normalize_text(inp.get("name"))}
    page_field = "pageCurNo" if "pageCurNo" in form_data else next(
        (k for k in form_data if k.lower() in ("page","pageno","pageindex","currentpage","pagecurno")), None)
    return action_url,form_data,page_field,sorted(page_numbers)

async def build_list_pages(request, crawl_all):
    list_url   = str(request.param.list_url)
    first_html = await fetch_html(list_url, request.param.ssl_mode)
    if not crawl_all: return [(list_url, first_html)]
    pages, seen = [], set()
    link_param, _ = extract_link_paging_info(first_html, list_url)
    action_url,form_data,page_field,_ = extract_form_request_info(first_html, list_url)
    def has_items(html): return bool(extract_list_candidates(html,request.param.list_root_selector,request.param.item_selector,request.param.target_selector,limit=1))
    def make_sig(html):
        items = extract_list_candidates(html,request.param.list_root_selector,request.param.item_selector,request.param.target_selector,limit=None)
        return "||".join(f"{c.get('title','')}|{c.get('href','')}|{c.get('onclick','')}" for c in items[:10])
    cp,cur_url,cur_html = 1,list_url,first_html
    while cp <= request.param.max_pages:
        if not has_items(cur_html): break
        sig = make_sig(cur_html)
        if sig in seen: break
        seen.add(sig); pages.append((cur_url,cur_html))
        np = cp+1
        if link_param:
            try: cur_html = await fetch_html(replace_query_param(list_url,link_param,str(np)),request.param.ssl_mode)
            except Exception: break
            cp=np; cur_url=replace_query_param(list_url,link_param,str(np))
        elif action_url and page_field:
            try:
                fd=dict(form_data); fd[page_field]=str(np)
                cur_html = await fetch_html_post(action_url,request.param.ssl_mode,fd)
            except Exception: break
            cp=np; cur_url=action_url
        else: break
    return pages

# ── minutes 상세 접근 ─────────────────────────────────────────────
async def open_detail_page(list_url,list_root_selector,item_selector,target_selector,rank_index,href,onclick,ssl_mode):
    fallback_note = None
    if href and not is_javascript_href(href) and is_http_like_href(href):
        detail_url = urljoin(list_url,href)
        if is_meaningful_detail_url(detail_url,list_url):
            try:
                html = await fetch_html(detail_url,ssl_mode)
                return detail_url,"http-href","direct",html,None
            except Exception as exc: fallback_note=f"직접 접근 실패: {type(exc).__name__}"
        else: fallback_note="href가 목록 URL과 동일"
    else: fallback_note="javascript/onclick 기반"
    for raw in filter(None,[href,onclick]):
        for pat in [r"""['"](https?://[^'"]+)['"]""",r"""['"]((?:/|\.\./|\./)[^'"]+)['"]"""]:
            m = re.search(pat,raw)
            if m:
                resolved = urljoin(list_url,m.group(1))
                if is_meaningful_detail_url(resolved,list_url):
                    try:
                        html = await fetch_html(resolved,ssl_mode)
                        return resolved,"string-resolve","direct",html,fallback_note
                    except Exception: pass
    return await _resolve_by_playwright(list_url,list_root_selector,item_selector,target_selector,rank_index,ssl_mode,fallback_note)

async def _resolve_by_playwright(list_url,list_root_selector,item_selector,target_selector,rank_index,ssl_mode,fallback_note=None):
    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            ctx  = await browser.new_context(user_agent=USER_AGENT,ignore_https_errors=(ssl_mode=="N"))
            page = await ctx.new_page()
            await page.goto(list_url,wait_until="domcontentloaded",timeout=30000)
            root = page.locator(list_root_selector).first
            if await root.count()==0: await browser.close(); return None,"playwright-no-root",None,None,"root 없음"
            items = root.locator(item_selector)
            if await items.count()==0 or rank_index>=await items.count(): await browser.close(); return None,"playwright-no-item",None,None,"item 없음"
            target = items.nth(rank_index) if target_selector=="self" else items.nth(rank_index).locator(target_selector).first
            if await target.count()==0: await browser.close(); return None,"playwright-no-target",None,None,"target 없음"
            original_url = page.url
            try:
                async with page.expect_popup(timeout=5000) as pi: await target.click()
                popup = await pi.value
                try: await popup.wait_for_load_state("networkidle",timeout=10000)
                except PlaywrightTimeoutError: pass
                detail_url=popup.url; html=await popup.content()
                await popup.close(); await browser.close()
                return detail_url,"playwright-click","popup",html,fallback_note
            except PlaywrightTimeoutError: pass
            try: await page.wait_for_load_state("networkidle",timeout=5000)
            except PlaywrightTimeoutError: pass
            if page.url and page.url!=original_url:
                html=await page.content(); await browser.close()
                return page.url,"playwright-click","same_page",html,fallback_note
            for frame in page.frames[1:]:
                try:
                    html = await frame.content()
                    if html and len(html)>200: await browser.close(); return frame.url or page.url,"playwright-click","iframe",html,fallback_note
                except Exception: continue
            await browser.close(); return None,"playwright-click","unknown",None,"변화 감지 실패"
    except Exception as exc: return None,f"playwright-error:{type(exc).__name__}",None,None,str(exc)

# ── 파일 다운로드 ─────────────────────────────────────────────────
def extract_file_info_from_reserved_value(raw_file_value,base_url):
    raw = normalize_text(raw_file_value)
    if not raw: raise ValueError("ORGINL_FILE_URL 값이 비어있습니다.")
    if "<a" in raw.lower():
        soup=BeautifulSoup(raw,"lxml"); a=soup.find("a")
        if not a: raise ValueError("a 태그를 찾지 못했습니다.")
        href=normalize_text(a.get("href")); name=normalize_text(a.get_text(" ",strip=True))
        if href and href!="#" and not href.lower().startswith("javascript:"): return urljoin(base_url,href),(name or None)
        original=normalize_text(a.get("data-original_record_file"))
        return raw,(original or name or None)
    return urljoin(base_url,raw),None

async def download_attachment_file(file_url,file_name,file_dir,crawl_type,crw_id,rasmbly_numpr,year,mints_cn,seq,ssl_mode,detail_url=None):
    if not file_url.startswith(("http://","https://","/")) and "<a" in file_url.lower():
        return await _download_by_playwright(detail_url or "",file_url,file_name,file_dir,crawl_type,crw_id,rasmbly_numpr,year,mints_cn,seq,ssl_mode)
    async with httpx.AsyncClient(headers={"User-Agent":USER_AGENT},timeout=httpx.Timeout(60.0,connect=10.0),
                                  follow_redirects=True,verify=get_verify_options(ssl_mode)) as client:
        r=await client.get(file_url); r.raise_for_status()
        cd=r.headers.get("content-disposition",""); original_name=None
        if cd:
            m=re.search(r'filename\*?=["\']?(?:UTF-8\'\')?([^"\';\r\n]+)',cd,re.IGNORECASE)
            if m: original_name=unquote(m.group(1).strip())
        resolved_name=normalize_text(original_name) or normalize_text(file_name) or "unknown.bin"
        save_path=build_file_save_path(file_dir,crawl_type,crw_id,rasmbly_numpr,year,mints_cn,seq,resolved_name)
        os.makedirs(os.path.dirname(save_path),exist_ok=True)
        if not os.path.exists(save_path):
            with open(save_path,"wb") as f: f.write(r.content)
        return save_path,resolved_name,file_url

async def _download_by_playwright(detail_url,a_tag_html,file_name,file_dir,crawl_type,crw_id,rasmbly_numpr,year,mints_cn,seq,ssl_mode):
    soup=BeautifulSoup(a_tag_html,"lxml"); a_tag=soup.find("a")
    if not a_tag: raise ValueError("a 태그 없음")
    parts=["a"]
    classes=a_tag.get("class",[])
    if isinstance(classes,str): classes=classes.split()
    if classes: parts[0]+="."+ ".".join(c.strip() for c in classes if c.strip())
    for attr,val in a_tag.attrs.items():
        if attr in {"class","href"}: continue
        val=normalize_text(" ".join(val) if isinstance(val,list) else str(val))
        if val: parts.append(f'[{attr}*="{val[:50]}"]' if len(val)>60 else f'[{attr}="{val}"]')
    selector="".join(parts)
    async with async_playwright() as p:
        browser=await p.chromium.launch(headless=True)
        ctx=await browser.new_context(user_agent=USER_AGENT,ignore_https_errors=(ssl_mode=="N"),accept_downloads=True)
        page=await ctx.new_page()
        await page.goto(detail_url,wait_until="domcontentloaded",timeout=30000)
        await page.wait_for_timeout(500)
        target=page.locator(selector).first
        if await target.count()==0: await browser.close(); raise ValueError(f"다운로드 대상 없음: {selector}")
        async with page.expect_download(timeout=30000) as di: await target.click()
        dl=await di.value
        resolved_name=normalize_text(dl.suggested_filename or file_name or "unknown.bin")
        save_path=build_file_save_path(file_dir,crawl_type,crw_id,rasmbly_numpr,year,mints_cn,seq,resolved_name)
        os.makedirs(os.path.dirname(save_path),exist_ok=True)
        if not os.path.exists(save_path): await dl.save_as(save_path)
        await browser.close()
        return save_path,resolved_name,dl.url

# ── bill 브라우저 헬퍼 ────────────────────────────────────────────
async def _setup_browser(pw):
    browser=await pw.chromium.launch(headless=True)
    page=await browser.new_page()
    await page.route("**/*",lambda r: r.abort() if r.request.resource_type in BLOCKED_RESOURCES else r.continue_())
    return browser,page

async def _try_url_fallback(page,next_page):
    print(f"[!] {next_page}p URL 강제 점프 시도",flush=True)
    new_url=re.sub(PAGE_PARAM_PATTERN,rf'\g<1>={next_page}',page.url,flags=re.IGNORECASE)
    if new_url!=page.url:
        try: await page.goto(new_url,wait_until="domcontentloaded",timeout=3000)
        except Exception as e: print(f"[!] URL fallback 실패: {e}",flush=True)

# ── bill 목록 수집 클래스 ─────────────────────────────────────────
class BillListCrawler:
    @staticmethod
    def _extract_view_id(href,onclick,row_html,view_id_param):
        clean=href.replace("&amp;","&") if href else ""
        if not view_id_param or view_id_param.strip()=="":
            if clean and not clean.startswith(("javascript","#")):
                m=re.search(r"/(\d+)(?:[/?#]|$)",clean)
                if m: return m.group(1)
        if clean and not clean.startswith(("javascript","#")):
            m=re.search(rf"[?&]{re.escape(view_id_param)}=([^&]+)",clean)
            if m: return m.group(1)
            m=re.search(VIEW_ID_AUTO_PARAMS,clean,re.IGNORECASE)
            if m: return m.group(2)
        m=re.search(rf"[?&]?{re.escape(view_id_param)}=([^&\"'>\s]+)",row_html)
        if m: return m.group(1)
        js=onclick or (href if href and href.startswith("javascript") else "")
        if js:
            m=re.search(r"\(['\"]?([^'\"),]+)['\"]?\)",js)
            if m: return m.group(1)
        m=re.search(r"onclick\s*=\s*[\"'][a-zA-Z0-9_]+\([\"']([^\"']+)[\"']\)",row_html)
        if m: return m.group(1)
        return None

    @staticmethod
    async def _get_row_link(row,tds):
        info={"href":"","onclick":"","bi_sj":""}
        tr_onclick=await row.get_attribute("onclick") or ""
        a_tag=await row.query_selector("a")
        if a_tag:
            info["href"]=await a_tag.get_attribute("href") or ""
            info["onclick"]=await a_tag.get_attribute("onclick") or tr_onclick
            text=clean_text(await a_tag.inner_text())
            if text: info["bi_sj"]=text
        else:
            info["onclick"]=tr_onclick
            m=re.search(r"goDetail\s*\(\s*'([^']+)'\s*,\s*'([^']+)'\s*,\s*'([^']+)'\s*,\s*'([^']+)'\s*,\s*'([^']+)'\s*\)",tr_onclick)
            if m:
                pt,gn,bn,bt,bnum=m.groups()
                info["href"]=f"/info/billRead.do?menuId=006002003&propTypeCd={pt}&generationNum={gn}&billNo={bn}&billTypeCd={bt}&billNum={bnum}"
            for td in tds:
                title=await td.get_attribute("title")
                if title: info["bi_sj"]=clean_text(title); break
        return info

    @staticmethod
    async def extract_list_page(page,list_class,view_id_param="code"):
        selector=normalize_selector(list_class)
        await page.wait_for_selector(selector,timeout=3000)
        items=[]
        for row in await page.query_selector_all(f"{selector} tbody > tr, {selector} ul > li, {selector} .list_row"):
            tds=await row.query_selector_all("td") or await row.query_selector_all("div, span")
            if not tds: continue
            item={"row_texts":[clean_text(await td.inner_text()) for td in tds]}
            link=await BillListCrawler._get_row_link(row,tds)
            if link["bi_sj"]: item["BI_SJ"]=link["bi_sj"]
            item["link_href"]=link["href"]
            vid=BillListCrawler._extract_view_id(link["href"],link["onclick"],await row.inner_html(),view_id_param)
            if vid: item["view_id"]=vid
            items.append(item)
        return items

    @staticmethod
    async def apply_filter_and_search(page,numpr,list_class,form_sel,select_sel,btn_sel):
        print(f"[*] 필터 적용 (대수:{numpr})",flush=True)
        errors=[]
        try:
            target_select=await page.query_selector(select_sel)
            if target_select and numpr:
                for opt in await target_select.query_selector_all("option"):
                    val=(await opt.get_attribute("value") or "").strip()
                    txt=(await opt.inner_text() or "").strip()
                    if val==numpr or val==f"0{numpr}" or f"{numpr}대" in txt:
                        await target_select.select_option(value=val)
                        await target_select.evaluate("node => node.dispatchEvent(new Event('change', {bubbles:true}))")
                        print(f"[+] 대수 선택: {txt}",flush=True); break
            try:
                btn=await page.wait_for_selector(btn_sel,timeout=3000,state="visible")
                if btn:
                    onclick_val=await btn.get_attribute("onclick") or ""
                    href_val=await btn.get_attribute("href") or ""
                    is_ajax=(href_val in ("#","","javascript:void(0)") or any(k in onclick_val.lower() for k in ["return false","loading","ajax","fetch"]))
                    if is_ajax:
                        await btn.click()
                        await page.wait_for_function(f"() => {{ const el=document.querySelector('{normalize_selector(list_class)} tbody > tr'); return el!==null; }}",timeout=3000)
                    else:
                        async with page.expect_navigation(timeout=3000): await btn.click()
                print("[+] 검색 버튼 클릭 성공",flush=True)
            except Exception as e:
                errors.append({"step":"필터_버튼클릭","selector":btn_sel,"error":str(e)[:300]})
                await page.evaluate(f"document.querySelector('{form_sel}')?.submit()")
                await page.wait_for_load_state("networkidle")
            if list_class:
                try: await page.wait_for_selector(normalize_selector(list_class),timeout=2000)
                except Exception as e: errors.append({"step":"필터_리스트로드","selector":list_class,"error":str(e)[:300]})
        except Exception as e: errors.append({"step":"필터_전체오류","selector":"","error":str(e)[:300]})
        return errors

    @staticmethod
    async def get_total_pages(page,end_btn_selector=None):
        try:
            ex=["a.last","a.num_last","a.btn-last","a.direction.last","a.btn.end","a.btn.next","a.next",
                "a[title*='마지막']","a[onclick*='Retrieve']","a.l_font","a:has-text('»')","a:has-text('>>') "]
            sels=([end_btn_selector]+[s for s in ex if s!=end_btn_selector]) if end_btn_selector else ex
            for btn in reversed(await page.query_selector_all(", ".join(sels))):
                combined=f"{await btn.get_attribute('href') or ''} {await btn.get_attribute('onclick') or ''} {await btn.inner_text() or ''}"
                m=re.search(r'(?:fn[a-zA-Z_]*|pageIndex|pageNum|pageNo|page|go|move|schPageNo|cp)\s*[\(=]\s*[\'"]?(\d+)[\'"]?',combined,re.IGNORECASE)
                if m and int(m.group(1))>1: return int(m.group(1))
            mx=1
            for b in await page.query_selector_all(".paging a,.paging2 a,.pagination a,#pagingNav a,.paging strong"):
                t=(await b.inner_text()).strip()
                if t.isdigit(): mx=max(mx,int(t))
            return mx
        except Exception as e: print(f"[-] 페이지 수 탐지 실패: {e}",flush=True); return 1

    @staticmethod
    async def go_to_page(page,next_page,paging_sel,next_btn_sel):
        p_sel=normalize_selector(paging_sel); n_sel=normalize_selector(next_btn_sel)
        try:
            if await page.evaluate("typeof fn_egov_link_page === 'function'"):
                await page.evaluate(f"fn_egov_link_page({next_page});"); await page.wait_for_load_state("domcontentloaded"); return True
        except: pass
        try:
            link=page.locator(p_sel).get_by_text(re.compile(f"^{next_page}$"),exact=True).first
            if await link.count()>0:
                await link.click()
                try: await page.wait_for_function("() => document.querySelectorAll('tbody#searchList tr').length > 0",timeout=5000)
                except: await page.wait_for_timeout(1000)
                return True
            nxt=page.locator(n_sel).first
            if await nxt.count()>0:
                await nxt.click(); await page.wait_for_load_state("domcontentloaded"); await page.wait_for_timeout(500); return True
        except Exception as e: print(f"[!] 페이지 이동 실패: {e}",flush=True)
        return False

async def _collect_pages(page,list_url,numpr,list_class,vid_param,max_pages,paging_sel,next_btn_sel,end_btn_sel,stop_check,search_form_selector,numpr_select_selector,search_btn_selector,last_sig=None):
    collect_errors,last_data_reached,consecutive_fail=[],False,0
    await page.goto(list_url,wait_until="domcontentloaded",timeout=10000)
    if numpr and numpr.strip():
        collect_errors.extend(await BillListCrawler.apply_filter_and_search(page,numpr.strip(),list_class,search_form_selector,numpr_select_selector,search_btn_selector))
    else:
        await page.wait_for_selector(normalize_selector(list_class),timeout=2000)
    total=await BillListCrawler.get_total_pages(page,end_btn_sel)
    safe_max=int(max_pages.strip()) if max_pages and max_pages.strip().isdigit() else 0
    target=total if safe_max==0 else min(safe_max,total)
    data=[]
    for cp in range(1,target+1):
        if stop_check(): print("[!] 중단 요청 감지",flush=True); break
        print(f"[*] 수집: {cp}/{target}p",flush=True)
        try:
            page_items=await BillListCrawler.extract_list_page(page,list_class,vid_param)
            consecutive_fail=0
            if last_sig:
                filtered=[]
                for li in page_items:
                    if is_list_item_past_last(li,last_sig): last_data_reached=True; break
                    filtered.append(li)
                data.extend(filtered)
                if last_data_reached: print(f"[last_data] {cp}p 기준점 도달",flush=True); break
            else: data.extend(page_items)
        except Exception as e:
            consecutive_fail+=1
            msg=str(e); sel_match=re.search(r'locator\("([^"]+)"\)',msg)
            print(f"[!] {cp}p 실패: {msg}",flush=True)
            collect_errors.append({"step":f"리스트수집_{cp}p","selector":sel_match.group(1) if sel_match else list_class,"error":msg[:300]})
            if consecutive_fail>=_MAX_CONSECUTIVE_FAIL: print(f"[!] 연속 {consecutive_fail}회 실패 → 중단",flush=True); break
        if cp<target and not last_data_reached:
            if not await BillListCrawler.go_to_page(page,cp+1,paging_sel,next_btn_sel):
                await _try_url_fallback(page,cp+1)
    return data,collect_errors,last_data_reached

# ── bill 상세: Playwright → HTML → item[] regex ───────────────────
async def _extract_bill_detail_html(page,view_class,target_url):
    await page.goto(target_url,wait_until="domcontentloaded",timeout=10000)
    if view_class:
        sel=normalize_selector(view_class)
        try:
            await page.wait_for_selector(sel,timeout=3000)
            el=await page.query_selector(sel)
            if el: return await el.inner_html()
        except Exception: pass
    return await page.content()

async def _extract_bill_attachments(page, view_class, base_url, req, bi_cn, year, items=None):
    # ── item[] regex 기반 다운로드 ──────────────────────────────
    if items:
        file_url_item = next((i for i in items if i.col == "BI_FILE_URL"), None)
        file_nm_item  = next((i for i in items if i.col == "BI_FILE_NM"),  None)

        if file_url_item and file_url_item.regex:
            if view_class:
                try:
                    el = await page.query_selector(normalize_selector(view_class))
                    detail_html = await el.inner_html() if el else await page.content()
                except Exception:
                    detail_html = await page.content()
            else:
                detail_html = await page.content()
            
            raw_urls  = _extract_all_matches(detail_html, file_url_item.regex)
            raw_names = _extract_all_matches(detail_html, file_nm_item.regex) if (file_nm_item and file_nm_item.regex) else []

            if not raw_urls:
                return {}

            attachments, seq = [], 0
            for i, raw_url in enumerate(raw_urls):
                full_url  = urljoin(base_url, raw_url.strip().replace("&amp;", "&").replace("&nbsp;", " ").replace("\u00a0", " "))
                hint_name = re.sub(r'[\s\u00a0&;]+', ' ', (raw_names[i] if i < len(raw_names) else "")).strip()
                seq += 1
                print(f"[*] 다운로드 시도: {hint_name or full_url}", flush=True)
                try:
                    async with httpx.AsyncClient(
                        headers={"User-Agent": USER_AGENT},
                        timeout=httpx.Timeout(60.0, connect=10.0),
                        follow_redirects=True, verify=certifi.where(),
                    ) as client:
                        r = await client.get(full_url); r.raise_for_status()

                    # 파일명 확정: Content-Disposition → hint_name → URL
                    cd = r.headers.get("content-disposition", "")
                    resolved_name = None
                    if cd:
                        m = re.search(r'filename\*?=["\']?(?:UTF-8\'\')?([^"\';\r\n]+)', cd, re.IGNORECASE)
                        if m: resolved_name = unquote(m.group(1).strip())
                    resolved_name = normalize_text(resolved_name or hint_name) or f"file_{seq}"

                    # 확장자 보정
                    _, ext = os.path.splitext(resolved_name)
                    if not ext:
                        ct  = r.headers.get("content-type", "")
                        ext = next((v for k, v in {
                            "application/pdf": ".pdf",
                            "application/msword": ".doc",
                            "application/haansofthwp": ".hwp",
                            "application/x-hwp": ".hwp",
                            "application/zip": ".zip",
                        }.items() if k in ct.lower()), ".bin")
                        resolved_name += ext

                    save_path = build_save_path(req, year, bi_cn, seq, ext)
                    os.makedirs(os.path.dirname(save_path), exist_ok=True)
                    with open(save_path, "wb") as f: f.write(r.content)
                    print(f"[+] 다운로드 완료: {resolved_name} → {save_path}", flush=True)
                    attachments.append({
                        "original_name": resolved_name,
                        "file_path":     save_path.replace("\\", "/"),
                        "file_id":       str(seq),
                        "url":           full_url,
                    })
                except Exception as e:
                    print(f"[-] 다운로드 실패 ({full_url}): {e}", flush=True)
                    attachments.append({
                        "original_name": hint_name or full_url,
                        "file_path":     "",
                        "file_id":       str(seq),
                        "url":           full_url,
                    })

            if not attachments: return {}
            names = [a["original_name"] for a in attachments]
            paths = [a["file_path"]     for a in attachments]
            ids   = [a["file_id"]       for a in attachments]
            urls  = [a["url"]           for a in attachments]
            return {
                "BI_FILE_NM":   names[0] if len(names)==1 else json.dumps(names, ensure_ascii=False),
                "BI_FILE_PATH": paths[0] if len(paths)==1 else json.dumps(paths, ensure_ascii=False),
                "BI_FILE_ID":   ids[0]   if len(ids)==1   else json.dumps(ids,   ensure_ascii=False),
                "BI_FILE_URL":  urls[0]  if len(urls)==1  else json.dumps(urls,  ensure_ascii=False),
            }

    # ── Playwright 클릭 다운로드 (regex 없을 때 fallback) ────────
    if view_class:
        try: container = await page.query_selector(normalize_selector(view_class))
        except Exception: container = None
    else: container = None
    target = container or page

    attach_td = None
    for sel in ["th:has-text('첨부파일') + td", "th:has-text('첨부') + td"]:
        try:
            el = await page.query_selector(sel)
            if el: attach_td = el; break
        except Exception: continue
    if attach_td: target = attach_td

    attachments, seq, seen_urls = [], 0, set()
    skip_kw         = ["바로보기","바로듣기","미리보기","뷰어","관련 회의록","회의록","발의 의원"]
    viewer_oc       = ["previewAjax","preListen","preview","viewer"]
    viewer_href_pat = ["synap","htmlViewer"]
    viewer_cls      = ["abtn_preview","preview"]

    for el in await target.query_selector_all("a, span[onclick], [style*='cursor: pointer']"):
        raw      = clean_text(await el.inner_text())
        title    = clean_text(await el.get_attribute("title") or "")
        if not title:
            title_el = await el.query_selector("[title]")
            title = clean_text(await title_el.get_attribute("title")) if title_el else ""
        if not title: title = raw
        href     = await el.get_attribute("href") or ""
        onclick  = await el.get_attribute("onclick") or ""
        el_class = await el.get_attribute("class") or ""
        if (any(k in raw      for k in skip_kw) or any(k in title    for k in skip_kw) or
            any(k in onclick  for k in viewer_oc) or any(k in href    for k in viewer_href_pat) or
            any(k in el_class for k in viewer_cls)): continue
        if not raw: continue
        is_js   = href.startswith(("javascript","#")) or (onclick and not href)
        url_val = onclick if is_js else (href or onclick)
        if url_val and url_val in seen_urls: continue
        if url_val: seen_urls.add(url_val)
        print(f"[*] 다운로드 시도: {raw}", flush=True)
        try:
            await el.evaluate("node => { if(node.tagName === 'A') node.removeAttribute('target'); }")
            async with page.expect_download(timeout=10000) as dl_info: await el.click()
            download = await dl_info.value
            _, ext   = os.path.splitext(download.suggested_filename or "")
            if not ext: ext = ".bin"
            original_name = title if title else raw
            seq += 1
            save_path = build_save_path(req, year, bi_cn, seq, ext) if req and bi_cn else os.path.join(FILE_DOWNLOAD_DIR, f"CLIKC{str(time.time_ns())[:16]}_{seq}{ext}")
            await download.save_as(save_path)
            print(f"[+] 다운로드 완료: {save_path}", flush=True)
            attachments.append({"original_name": original_name, "file_path": save_path.replace("\\","/"), "file_id": str(seq), "url": download.url})
        except Exception as e:
            print(f"[-] 다운로드 건너뜀 ({raw}): {str(e)[:100]}", flush=True)
            seq += 1
            attachments.append({"original_name": raw, "file_path": "", "file_id": str(seq), "url": url_val})
            if page.url != base_url:
                try: await page.goto(base_url, wait_until="domcontentloaded", timeout=3000)
                except: pass

    if not attachments: return {}
    names = [a["original_name"] for a in attachments]
    paths = [a["file_path"]     for a in attachments]
    ids   = [a["file_id"]       for a in attachments]
    urls  = [a["url"]           for a in attachments]
    return {
        "BI_FILE_NM":   names[0] if len(names)==1 else json.dumps(names, ensure_ascii=False),
        "BI_FILE_PATH": paths[0] if len(paths)==1 else json.dumps(paths, ensure_ascii=False),
        "BI_FILE_ID":   ids[0]   if len(ids)==1   else json.dumps(ids,   ensure_ascii=False),
        "BI_FILE_URL":  urls[0]  if len(urls)==1  else json.dumps(urls,  ensure_ascii=False),
    }

def _extract_all_matches(html: str, patterns: list) -> list:
    for pattern in patterns:
        try:
            matches = re.findall(pattern, html, re.IGNORECASE | re.DOTALL)
            if matches:
                return [m if isinstance(m, str) else m[0] for m in matches]
        except re.error:
            continue
    return []

# ── bill 실행 엔진 ────────────────────────────────────────────────
async def execute_bill_scraping(req: ScrapeRequest):
    app.state.stop_scraping=False
    p=req.param
    list_data,view_data,error_logs,field_logs=[],[],[],[]
    filepath=None
    last_sig=_build_last_data_signature(req.last_data) if req.last_data else None
    if last_sig: print(f"[last_data] 추가수집 모드: {last_sig}",flush=True)
    has_file_item = any(i.col in {"BI_FILE_NM", "BI_FILE_URL"} for i in req.item)
    async with async_playwright() as playwright:
        print(f"\n{'='*60}\n[*] [1단계] 리스트 수집: {p.list_url}",flush=True)
        browser,page=await _setup_browser(playwright)
        try:
            list_data,collect_errors,last_data_reached=await _collect_pages(
                page,p.list_url,p.rasmbly_numpr,p.list_class,p.view_id_param,
                p.max_pages,p.paging_selector,p.next_btn_selector,p.end_btn_selector,
                lambda: app.state.stop_scraping,
                p.search_form_selector,p.numpr_select_selector,p.search_btn_selector,last_sig=last_sig)
        finally:
            await browser.close(); print(f"[*] 1단계 브라우저 종료",flush=True)
        error_logs.extend(collect_errors)
        if not list_data:
            error_logs.append({"step":"1단계_리스트수집","url":p.list_url,"selector":p.list_class,"error":"리스트 수집 결과 0건"})
        total=len(list_data)
        print(f"\n[*] [2단계] 상세 수집 (총 {total}건)\n{'-'*60}",flush=True)
        detail_last_data_reached=False
        browser,page=await _setup_browser(playwright)
        try:
            for idx,item in enumerate(list_data):
                if app.state.stop_scraping: print(f"\n[!] 중단 요청: {idx}번째",flush=True); break
                vid=item.get("view_id")
                if not vid: continue
                print(f"[*] 상세 ({idx+1}/{total}) ID: {vid}",flush=True)
                href=item.get("link_href","")
                is_real=href and not href.startswith(("#","javascript"))
                target_url=urljoin(p.list_url,href) if is_real else f"{p.view_url}{'&' if '?' in p.view_url else '?'}{p.view_id_param}={vid}"
                try:
                    detail_html=await _extract_bill_detail_html(page,p.view_class,target_url)
                    parsed_u=urlparse(target_url)
                    base=f"{parsed_u.scheme}://{parsed_u.netloc}"
                    bi_cn=str(time.time_ns())[:16]
                    list_title=item.get("BI_SJ","")
                    detail=parse_detail_by_items(detail_html,req.item,list_title=list_title)
                    if not detail.get("RASMBLY_NUMPR") and p.rasmbly_numpr:
                        detail["RASMBLY_NUMPR"]=str(p.rasmbly_numpr)
                    if has_file_item and not detail.get("BI_FILE_PATH"):
                        year=(detail.get("ITNC_DE") or "")[:4] or str(datetime.now().year)
                        detail.update(await _extract_bill_attachments(page,p.view_class,base,req,bi_cn,year,items=req.item))
                    collected_item={"view_id":vid,"URL":target_url,"BI_CN":f"CLIKC{bi_cn}",**detail}
                    field_logs.append(audit_fields(vid,target_url,f"CLIKC{bi_cn}",collected_item,req.item))
                    if last_sig and not last_data_reached and is_last_data_match(collected_item,last_sig):
                        detail_last_data_reached=True; print(f"[last_data] 상세 기준점 도달",flush=True); break
                    view_data.append(collected_item)
                except Exception as e:
                    print(f"    [!] ID: {vid} 실패: {e}",flush=True)
                    view_data.append({"view_id":vid,"URL":target_url,"view_error":str(e)})
                    error_logs.append({"step":"2단계_상세수집","view_id":vid,"url":target_url,"error":str(e)})
        except Exception as e:
            print(f"\n[!] 상세 수집 전체 에러: {e}",flush=True)
            return {"req_id":req.req_id,"type":req.type,"crw_id":req.crw_id,"file_dir":req.file_dir,"ok":False,"error_msg":str(e)}
        finally: await browser.close()
    is_interrupted=app.state.stop_scraping or last_data_reached or detail_last_data_reached
    result_block=_build_result(view_data,error_logs,is_interrupted)
    if (last_data_reached or detail_last_data_reached) and result_block["status"] in ("SUCCESS","PARTIAL"):
        result_block["message"]="추가수집 완료 (last_data 기준점 도달)"
    if field_logs: save_field_logs(field_logs,req)
    full_payload={"reqId":req.req_id,"type":req.type,"crwId":req.crw_id,"fileDir":req.file_dir,
                  "result":result_block,"data":view_data,"log":error_logs}
    view_data.reverse()
    if view_data or error_logs:
        domain=extract_domain(p.list_url)
        if view_data:
            filepath=save_to_json(full_payload,domain,req.type)
            print(f"[OK] 저장 완료 ({len(view_data)}건): {filepath}",flush=True)
        else:
            filepath=save_to_json(full_payload,domain,req.type+"_error")
            print(f"[!] 에러 로그 {len(error_logs)}건 저장: {filepath}",flush=True)
        await _do_send(INSERT_API_URL,full_payload)
    else: print(f"[!] 수집 데이터 없음",flush=True)
    return {"req_id":req.req_id,"type":req.type,"crw_id":req.crw_id,"file_dir":req.file_dir,
            "ok":True,"interrupted":is_interrupted,
            "last_data_reached":last_data_reached or detail_last_data_reached,
            "data_count":len(view_data),"saved_file":filepath}

async def execute_bill_scraping_test(req: ScrapeRequest):
    p=req.param; view_data=[]; has_file_item=any(i.col in {"BI_FILE_NM", "BI_FILE_URL"} for i in req.item)
    async with async_playwright() as playwright:
        browser,page=await _setup_browser(playwright)
        try:
            print(f"[TEST] 리스트 수집: {p.list_url}",flush=True)
            await page.goto(p.list_url,wait_until="domcontentloaded",timeout=3000)
            if p.rasmbly_numpr and p.rasmbly_numpr.strip():
                await BillListCrawler.apply_filter_and_search(page,p.rasmbly_numpr.strip(),p.list_class,p.search_form_selector,p.numpr_select_selector,p.search_btn_selector)
            else:
                await page.wait_for_selector(normalize_selector(p.list_class),timeout=3000)
            list_data=await BillListCrawler.extract_list_page(page,p.list_class,p.view_id_param)
            if not list_data: return {"req_id":req.req_id,"type":req.type,"crw_id":req.crw_id,"file_dir":req.file_dir,"data":[]}
            item=list_data[0]; vid=item.get("view_id")
            if vid:
                href=item.get("link_href","")
                is_real=href and not href.startswith(("#","javascript"))
                target_url=urljoin(p.list_url,href) if is_real else f"{p.view_url}{'&' if '?' in p.view_url else '?'}{p.view_id_param}={vid}"
                print(f"[TEST] 상세 수집: {vid}",flush=True)
                try:
                    detail_html=await _extract_bill_detail_html(page,p.view_class,target_url)
                    bi_cn=str(time.time_ns())[:16]
                    detail=parse_detail_by_items(detail_html,req.item,list_title=item.get("BI_SJ",""))
                    if not detail.get("RASMBLY_NUMPR") and p.rasmbly_numpr: detail["RASMBLY_NUMPR"]=str(p.rasmbly_numpr)
                    parsed_u=urlparse(target_url); base=f"{parsed_u.scheme}://{parsed_u.netloc}"
                    if has_file_item and not detail.get("BI_FILE_PATH"):
                        year=(detail.get("ITNC_DE") or "")[:4] or str(datetime.now().year)
                        detail.update(await _extract_bill_attachments(page,p.view_class,base,req,bi_cn,year,items=req.item))
                    view_data.append({"view_id":vid,"view_url":target_url,"BI_CN":f"CLIKC{bi_cn}",**detail})
                except Exception as e: view_data.append({"view_id":vid,"view_url":target_url,"view_error":str(e)})
        except Exception as e: print(f"[TEST] 에러: {e}",flush=True)
        finally: await browser.close()
    view_data.reverse()
    return {"req_id":req.req_id,"type":req.type,"crw_id":req.crw_id,"file_dir":req.file_dir,"data":view_data}

# ── minutes 실행 엔진 ─────────────────────────────────────────────
async def build_minutes_item(request,list_page_url,candidate,rank_index_in_page,final_rank):
    title=candidate["title"]; href=candidate["href"]; onclick=candidate["onclick"]
    rasmbly_numpr=extract_rasmbly_numpr(candidate.get("row_text",""))
    detail_url,access_method,open_type,detail_html,note=await open_detail_page(
        list_page_url,request.param.list_root_selector,request.param.item_selector,
        request.param.target_selector,rank_index_in_page,href,onclick,request.param.ssl_mode)
    uid=extract_uid(detail_url); mints_cn=("CLIKR"+str(time.time_ns()))[:21]
    if not rasmbly_numpr and detail_html: rasmbly_numpr=extract_rasmbly_numpr(detail_html)
    if not rasmbly_numpr: rasmbly_numpr=request.param.rasmbly_numpr
    if not detail_html:
        return MinutesItem(rank=final_rank,list_title=title,detail_url=detail_url,
                           access_method=access_method,open_type=open_type,detail_access_success=False,
                           uid=uid,mints_cn=mints_cn,raw_href=href,raw_onclick=onclick,note=note or "상세 접근 실패")
    parsed=parse_detail_by_items(detail_html,request.item,list_title=title)
    file_value=parsed.pop("ORGINL_FILE_URL",None)
    if file_value:
        try:
            full_url,file_name=extract_file_info_from_reserved_value(file_value,detail_url or list_page_url)
            year=extract_year_from_date(parsed.get("MTG_DE"))
            save_path,saved_name,file_url=await download_attachment_file(
                full_url,file_name,request.file_dir,request.type,request.crw_id or "unknown",
                rasmbly_numpr,year,mints_cn,1,request.param.ssl_mode,detail_url)
            parsed["ORGINL_FILE_URL"]=file_url; parsed["MINTS_FILE_PATH"]=save_path; parsed["ORGINL_FILE_NM"]=saved_name
        except Exception as exc:
            parsed["ORGINL_FILE_URL"]=parsed["MINTS_FILE_PATH"]=parsed["ORGINL_FILE_NM"]=None
            note=f"{note} / 파일 다운로드 실패: {type(exc).__name__}" if note else f"파일 다운로드 실패: {type(exc).__name__}"
    parsed["RASMBLY_NUMPR"]=rasmbly_numpr
    return MinutesItem(rank=final_rank,list_title=title,detail_url=detail_url,access_method=access_method,
                       open_type=open_type,detail_access_success=True,fields=parsed,uid=uid,mints_cn=mints_cn,
                       raw_href=href,raw_onclick=onclick,note=note)

async def execute_minutes_scraping(request: MinutesRequest):
    if not request.item: raise HTTPException(status_code=400,detail="item은 최소 1개 이상이어야 합니다.")
    is_additional=bool(request.last_data)
    try: list_pages=await build_list_pages(request,crawl_all=not is_additional)
    except Exception as exc: raise HTTPException(status_code=400,detail=f"목록 페이지 요청 실패: {exc}") from exc
    all_items: List[MinutesItem]=[]; seen_keys: set=set(); is_last_matched=False
    for page_idx,(page_url,page_html) in enumerate(list_pages,start=1):
        if is_last_matched: break
        print(f"[MINUTES] ===== {page_idx}p ===== {page_url}",flush=True)
        candidates=extract_list_candidates(page_html,request.param.list_root_selector,request.param.item_selector,request.param.target_selector,limit=None)
        if not candidates: continue
        if page_idx==1 and request.param.skip_top_count>0: candidates=candidates[request.param.skip_top_count:]
        if not candidates: continue
        for idx,candidate in enumerate(candidates,start=1):
            try: item=await build_minutes_item(request,page_url,candidate,idx-1,len(all_items)+1)
            except Exception as exc:
                item=MinutesItem(rank=len(all_items)+1,list_title=candidate["title"],detail_url=None,
                                 access_method="error",open_type=None,detail_access_success=False,
                                 raw_href=candidate.get("href"),raw_onclick=candidate.get("onclick"),
                                 note=f"상세 처리 실패: {type(exc).__name__}")
            if is_additional and matches_last_data_minutes(item.fields,item.detail_url,item.mints_cn,request.last_data):
                print(f"[MINUTES] last_data 일치 → 추가수집 종료",flush=True); is_last_matched=True; break
            dedupe_key=item.uid or item.detail_url or f"{item.list_title}|{item.raw_href}|{item.raw_onclick}"
            if dedupe_key in seen_keys: continue
            seen_keys.add(dedupe_key); item.rank=len(all_items)+1; all_items.append(item)
    data=[]
    for item in all_items:
        if item.fields:
            row=dict(item.fields); row["url"]=item.detail_url; row["mints_cn"]=item.mints_cn; data.append(row)
    data.reverse()
    payload={"reqId":request.req_id,"type":request.type,"crwId":request.crw_id,"fileDir":request.file_dir,
             "result":_build_result(data,[],is_last_matched),"data":data,"log":[]}
    domain=extract_domain(str(request.param.list_url))
    filepath=save_to_json(payload,domain,request.type)
    await _do_send(INSERT_API_URL,payload)
    return {"req_id":request.req_id,"type":request.type,"crw_id":request.crw_id,"ok":True,"data_count":len(data),"saved_file":filepath}

# ── 공통 전송 ─────────────────────────────────────────────────────
async def _do_send(target_url,payload):
    async with httpx.AsyncClient() as client:
        try:
            r=await client.post(target_url,json=payload,timeout=120.0)
            print(f"[OK] API 전송 {'성공' if r.status_code==200 else '완료'}",flush=True)
        except Exception as e: print(f"[!] 네트워크 오류: {e}",flush=True)

# ── FastAPI 라우터 ────────────────────────────────────────────────
def _route_request(raw: UnifiedRequest):
    if raw.type=="minutes":
        return MinutesRequest(req_id=raw.req_id,crw_id=raw.crw_id,type=raw.type,
                              last_data=raw.last_data,file_dir=raw.file_dir,
                              param=MinutesParam(**raw.param),item=raw.item)
    return ScrapeRequest(req_id=raw.req_id,type=raw.type,crw_id=raw.crw_id,file_dir=raw.file_dir,
                         param=ScrapeParam(**raw.param),item=raw.item,
                         last_data=LastData(**raw.last_data) if raw.last_data else None)

@app.post("/crawl", status_code=202)
async def crawl(raw: UnifiedRequest, background_tasks: BackgroundTasks):
    try:
        req=_route_request(raw)
        if isinstance(req,MinutesRequest): background_tasks.add_task(execute_minutes_scraping,req)
        else:                              background_tasks.add_task(execute_bill_scraping,req)
        return {"req_id":raw.req_id,"type":raw.type,"crw_id":raw.crw_id,"file_dir":raw.file_dir,"ok":True,"message":"수집 요청 완료"}
    except Exception as e: return error_response(f"요청 처리 중 오류: {e}")

@app.post("/crawl/test")
async def crawl_test(raw: UnifiedRequest):
    try:
        req=_route_request(raw)
        if isinstance(req,MinutesRequest):
            req.param.max_pages=1  # type: ignore
            return await execute_minutes_scraping(req)
        return await execute_bill_scraping_test(req)
    except Exception as e: return error_response(f"테스트 요청 오류: {e}")

@app.get("/crawl/stop")
async def stop_crawl():
    app.state.stop_scraping=True
    print(f"[!] stop_scraping = True",flush=True)
    return {"ok":True,"message":"수집 중단 요청 완료"}

@app.get("/health")
async def health():
    return {"status":"ok","time":datetime.now().isoformat()}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8900)