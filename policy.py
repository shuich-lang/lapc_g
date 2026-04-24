import asyncio
import os
import re
import hashlib
import httpx
import json
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urljoin, urlparse

from fastapi import BackgroundTasks, FastAPI
from fastapi.responses import JSONResponse
from playwright.async_api import Page, async_playwright
from pydantic import BaseModel, Field, ValidationInfo, field_validator

# ── 공통 상수 ─────────────────────────────────────────────────────────────────
DOWNLOAD_DIR        = "download"
BLOCKED_RESOURCES   = {"image", "stylesheet", "media", "font"}
PAGE_PARAM_PATTERN  = r'([?&](?:page|pageIndex|p|page_no|pageno|cPage|pageNum|page_id|cp))=(\d+)'
VIEW_ID_AUTO_PARAMS = r"[?&](uid|idx|code|no|seq|id|nttId|uuid|bbsSeq|ntNo|articleSeq|list_no|postId|num|docId|bcIdx|parentSeq|bbsIdx|contentsId|dataId)=([^&]+)"
_DATE_PATTERN       = re.compile(r'(\d{4})[./\-](\d{1,2})[./\-](\d{1,2})')
ATTACH_HINT_KEYS    = ["첨부", "파일", "원문", "자료", "다운", "down", "file", "attach"]
IMAGE_EXTS          = {".jpg", ".jpeg", ".png", ".gif", ".bmp", ".webp", ".svg"}

# ── JS 함수명 → view href 조립 규칙 ──────────────────────────────────────────
# 새 JS 패턴 추가 시 여기에만 추가
_JS_VIEW_BUILDERS: Dict[str, Any] = {
    # doBbsFView(cbIdx, bcIdx, menuCode, nttId) — NIA
    "doBbsFView": lambda g: (
        f"/site/nia_kor/ex/bbs/View.do"
        f"?cbIdx={g[0]}&bcIdx={g[1]}&parentSeq={g[3]}"
    ),
    # goDetail(propType, genNum, billNo, billType, billNum) — 서울특별시의회
    "goDetail": lambda g: (
        f"/info/billRead.do?menuId=006002003"
        f"&propTypeCd={g[0]}&generationNum={g[1]}"
        f"&billNo={g[2]}&billTypeCd={g[3]}&billNum={g[4]}"
    ),
}

app = FastAPI(title="Policy Board Scraper API")
app.state.stop_scraping    = False
app.state.current_stop_event: Optional[asyncio.Event] = None  # 실행 중인 태스크의 중단 이벤트


# ── 요청 모델 ─────────────────────────────────────────────────────────────────

class PolicyParam(BaseModel):
    """
    [설계 원칙]
    - 각 셀렉터 필드는 콤마 구분 문자열로 CMS에서 관리.
    - 코드(_resolve_selector)가 순서대로 DOM 존재 여부를 확인해 첫 번째 유효값 사용.
    - 새 사이트 태그 등장 → CMS에서 해당 필드에 콤마로 추가. 코드 수정 불필요.
    """
    # ── 필수 ──────────────────────────────────────────────────────────────────
    list_url: str = Field(..., description="게시판 목록 URL")

    # ── CMS 관리 (콤마 구분 다중 후보) ────────────────────────────────────────
    list_class:        str           = Field("table.board_list",  description="목록 셀렉터 후보 (콤마 구분)")
    paging_selector:   str           = Field("div#pagingNav",     description="페이징 영역 셀렉터 후보 (콤마 구분)")
    next_btn_selector: str           = Field("a.num_right",       description="다음 버튼 셀렉터 후보 (콤마 구분)")
    end_btn_selector:  str           = Field("a.num_last",        description="마지막 버튼 셀렉터 후보 (콤마 구분)")
    view_id_param:     str           = Field("",                  description="상세 ID 파라미터명 (빈값=자동탐지)")
    view_url:          Optional[str] = Field(None,                description="상세 기본 URL (href 우선, fallback용)")
    view_class:        str           = Field("",                  description="상세 콘텐츠 셀렉터 후보 (콤마 구분)")

    # ── 수집 제어 ──────────────────────────────────────────────────────────────
    max_pages:       str  = Field("",   description="최대 수집 페이지 (빈값=전체)")
    download_attach: bool = Field(True, description="첨부파일 실제 다운로드 여부")

    # ── CONTENTS 테이블 고정 메타 ──────────────────────────────────────────────
    site_id:     str = Field("", description="SITEID")
    seed_id:     str = Field("", description="SEEDID")
    site_nm:     str = Field("", description="SITENM")
    seed_nm:     str = Field("", description="SEEDNM")
    category_id: str = Field("", description="CATEGORY_ID")
    category_nm: str = Field("", description="CATEGORY_NAME")
    bbs_id:      str = Field("", description="BBS_ID")
    bbs_nm:      str = Field("", description="BBS_NAME")
    region:      str = Field("", description="REGION")
    doc_type:    str = Field("", description="DOCTYPE")


class PolicyRequest(BaseModel):
    req_id:   str         = Field(..., min_length=1)
    type:     str         = Field(..., min_length=1)
    crw_id:   str         = Field(..., min_length=1)
    file_dir: str         = Field(...)
    param:    PolicyParam = Field(...)

    @field_validator('req_id', 'type', 'crw_id', 'file_dir')
    @classmethod
    def not_empty(cls, v: str, info: ValidationInfo):
        if not v or not v.strip():
            raise ValueError(f"[{info.field_name}] 필수 파라미터가 비어있습니다.")
        return v


# ── 유틸리티 ──────────────────────────────────────────────────────────────────

def clean_text(text: Optional[str]) -> str:
    return re.sub(r'\s+', ' ', text.strip()) if text else ""

def extract_domain(url: str) -> str:
    try:
        netloc = urlparse(url).netloc.split(':')[0].lower()
        for pfx in ("www.", "council.", "office.", "assembly."):
            if netloc.startswith(pfx):
                netloc = netloc[len(pfx):]
                break
        return netloc.split('.')[0]
    except Exception:
        return "unknown"

def normalize_selector(selector: str) -> str:
    if not selector:
        return ""
    s = selector.strip()
    if any(s.startswith(p) for p in (
        ".", "#", "[", "table", "div", "ul", "nav",
        "span", "a", "button", "article", "section"
    )):
        return s
    return f".{s}"

def split_candidates(value: str) -> List[str]:
    """'table.a, ul.b, div.c' → ['table.a', 'ul.b', 'div.c']"""
    return [s.strip() for s in value.split(",") if s.strip()] if value else []

def error_response(msg: str):
    return JSONResponse(status_code=200, content={"ok": False, "message": msg})

def save_to_json(data: Any, domain: str, prefix: str) -> str:
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)
    filename = f"{domain}_{prefix}_{datetime.now():%Y%m%d%H%M%S}.json"
    filepath = os.path.join(DOWNLOAD_DIR, filename)
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)
    print(f"[+] 저장: {filepath}", flush=True)
    return filepath

def _normalize_date(value: str) -> str:
    m = _DATE_PATTERN.search(value)
    if m:
        y, mo, d = m.group(1), m.group(2).zfill(2), m.group(3).zfill(2)
        return f"{y}{mo}{d}"
    return value

def _make_hash(title: str, url: str) -> str:
    return hashlib.sha256(f"{title}|{url}".encode()).hexdigest()

def _file_type(ext: str) -> str:
    return "I" if ext.lower() in IMAGE_EXTS else "A"

def build_attach_save_path(req: "PolicyRequest", year: str, outbbs_cn: str, seq: int, ext: str) -> str:
    path = os.path.join("/", req.file_dir, req.type, req.crw_id, year, f"CLIKC{outbbs_cn}_attach_{seq}{ext}")
    os.makedirs(os.path.dirname(path), exist_ok=True)
    return path

def _build_result(contents: list, log: list, interrupted: bool, error: str = "") -> dict:
    has_timeout = any("Timeout" in (e.get("error") or "") for e in log)
    has_error   = any(e.get("error") for e in log)
    cnt         = len(contents)
    if error:
        status, code, message = "FAILED",  "500", f"수집 실패: {error}"
    elif cnt == 0 and has_timeout:
        status, code, message = "TIMEOUT", "408", "타임아웃으로 수집 불가"
    elif cnt == 0:
        status, code, message = "EMPTY",   "204", "수집 결과 없음"
    elif interrupted or has_timeout or has_error:
        status, code, message = "PARTIAL", "206", "일부 수집 완료 (오류/중단 포함)"
    else:
        status, code, message = "SUCCESS", "200", "수집 완료"
    return {"status": status, "code": code, "message": message,
            "dataCount": cnt, "interrupted": interrupted}


# ── 라벨 → CONTENTS 컬럼 매핑 ────────────────────────────────────────────────
_LABEL_MAP: Dict[str, str] = {
    "제목": "TITLE",       "Title": "TITLE",
    "작성자": "WRITER",    "등록자": "WRITER",    "담당자": "WRITER",
    "작성부서": "WRITER",  "등록부서": "WRITER",  "작성": "WRITER",
    "작성일": "CDATE",     "등록일": "CDATE",     "게시일": "CDATE",
    "공고일": "CDATE",     "날짜": "CDATE",       "일시": "CDATE",
    "내용": "CONTENT",     "본문": "CONTENT",
    "위원회": "CMIT",      "소관위원회": "CMIT",  "담당위원회": "CMIT",
}

def _label_to_col(label: str) -> Optional[str]:
    norm = label.replace(" ", "")
    if norm in _LABEL_MAP:
        return _LABEL_MAP[norm]
    if any(k in norm for k in ("제목", "Title")):                              return "TITLE"
    if any(k in norm for k in ("작성자", "등록자", "담당자", "부서")):         return "WRITER"
    if any(k in norm for k in ("작성일", "등록일", "게시일", "공고일", "날짜", "일시")): return "CDATE"
    if any(k in norm for k in ("내용", "본문")):                               return "CONTENT"
    if "위원회" in norm:                                                        return "CMIT"
    return None


# ── 브라우저 헬퍼 ─────────────────────────────────────────────────────────────

async def _setup_browser(pw):
    browser = await pw.chromium.launch(headless=True)
    page    = await browser.new_page()
    await page.route(
        "**/*",
        lambda r: r.abort() if r.request.resource_type in BLOCKED_RESOURCES else r.continue_()
    )
    return browser, page

async def _try_url_fallback(page, next_page: int):
    url     = page.url
    new_url = re.sub(PAGE_PARAM_PATTERN, rf'\g<1>={next_page}', url, flags=re.IGNORECASE)
    if new_url != url:
        try:
            await page.goto(new_url, wait_until="domcontentloaded", timeout=30000)
        except Exception as e:
            print(f"[!] URL fallback 실패: {e}", flush=True)


# ── 핵심: 콤마 구분 후보 → DOM 존재하는 첫 번째 셀렉터 확정 ──────────────────

async def _resolve_selector(page: Page, candidates_str: str, check_children: str = None) -> Optional[str]:
    """
    CMS에서 콤마로 관리하는 후보 문자열을 순서대로 시도.
    check_children 지정 시 해당 하위 요소까지 존재해야 통과.

    예) candidates_str = "table.board_list, ul.bbs_list, div.board_type01"
        → 'table.board_list' 없음 → 'ul.bbs_list' 없음 → 'div.board_type01' 있음 → 반환
    """
    for sel in split_candidates(candidates_str):
        try:
            query = f"{normalize_selector(sel)} {check_children}" if check_children else normalize_selector(sel)
            if await page.query_selector(query):
                print(f"[*] 셀렉터 확정: '{sel}'", flush=True)
                return sel
        except:
            continue
    return None


# ── 크롤링 엔진 ───────────────────────────────────────────────────────────────

class PolicyCrawler:

    # ── JS onclick → href 조립 ────────────────────────────────────────────────
    @staticmethod
    def _build_href_from_js(onclick: str) -> Optional[str]:
        """_JS_VIEW_BUILDERS 순회. 신규 JS 패턴은 딕셔너리에만 추가."""
        for fn_name, builder in _JS_VIEW_BUILDERS.items():
            m = re.search(rf"{re.escape(fn_name)}\s*\(([^)]+)\)", onclick)
            if m:
                args = [a.strip().strip("'\"") for a in m.group(1).split(",")]
                try:
                    return builder(args)
                except Exception as e:
                    print(f"[-] JS href 조립 실패 ({fn_name}): {e}", flush=True)
        return None

    # ── view_id 추출 ─────────────────────────────────────────────────────────
    @staticmethod
    def _extract_view_id(href: str, onclick: str, row_html: str, view_id_param: str) -> Optional[str]:
        clean = href.replace("&amp;", "&") if href else ""

        if not view_id_param:
            if clean and not clean.startswith(("javascript", "#")):
                m = re.search(r"/(\d+)(?:[/?#]|$)", clean)
                if m: return m.group(1)

        if clean and not clean.startswith(("javascript", "#")):
            if view_id_param:
                m = re.search(rf"[?&]{re.escape(view_id_param)}=([^&]+)", clean)
                if m: return m.group(1)
            m = re.search(VIEW_ID_AUTO_PARAMS, clean, re.IGNORECASE)
            if m: return m.group(2)

        if view_id_param:
            m = re.search(rf"[?&]?{re.escape(view_id_param)}=([^&\"'>\s]+)", row_html)
            if m: return m.group(1)

        js = onclick or (href if href and href.startswith("javascript") else "")
        if js:
            m = re.search(r"\(['\"]?([^'\"),]+)['\"]?\)", js)
            if m: return m.group(1)

        return None

    # ── 행 링크 추출 ─────────────────────────────────────────────────────────
    @staticmethod
    async def _get_row_link(row, tds) -> dict:
        info       = {"href": "", "onclick": "", "title": ""}
        tr_onclick = await row.get_attribute("onclick") or ""
        a_tag      = await row.query_selector("a")

        if a_tag:
            raw_href        = await a_tag.get_attribute("href") or ""
            onclick         = await a_tag.get_attribute("onclick") or tr_onclick
            info["onclick"] = onclick

            # JS 함수 기반 href 조립 (href가 앵커/javascript인 경우)
            if raw_href.startswith(("#", "javascript")) and onclick:
                built = PolicyCrawler._build_href_from_js(onclick)
                info["href"] = built if built else raw_href
            else:
                info["href"] = raw_href

            # 제목: title 속성 우선 (후미 "첨부파일 있음" 제거) → inner_text fallback
            title_attr  = await a_tag.get_attribute("title") or ""
            title_clean = re.sub(r'\s*[-–]\s*첨부파일\s*있음\s*$', '', title_attr)
            info["title"] = clean_text(title_clean) or clean_text(await a_tag.inner_text())
        else:
            info["onclick"] = tr_onclick
            for td in tds:
                t = await td.get_attribute("title")
                if t:
                    info["title"] = clean_text(t)
                    break

        return info

    # ── 목록 페이지 추출 ─────────────────────────────────────────────────────
    @staticmethod
    async def extract_list_page(page: Page, list_class: str, view_id_param: str = "") -> List[Dict[str, Any]]:
        """list_class 는 _resolve_selector() 로 확정된 단일 셀렉터"""
        selector = normalize_selector(list_class)
        await page.wait_for_selector(selector, timeout=10000)

        items = []
        rows  = await page.query_selector_all(
            f"{selector} tbody tr, {selector} ul > li, {selector} .list_row, {selector} .board-item"
        )
        for row in rows:
            tds  = await row.query_selector_all("td") or await row.query_selector_all("div, span")
            if not tds: continue
            item = {"row_texts": [clean_text(await td.inner_text()) for td in tds]}
            link = await PolicyCrawler._get_row_link(row, tds)
            if link["title"]: item["title"] = link["title"]
            item["link_href"] = link["href"]
            vid = PolicyCrawler._extract_view_id(
                link["href"], link["onclick"], await row.inner_html(), view_id_param
            )
            if vid: item["view_id"] = vid
            items.append(item)
        return items

    # ── 전체 페이지 수 탐지 ──────────────────────────────────────────────────
    @staticmethod
    async def get_total_pages(page: Page, end_btn_selector: str = "") -> int:
        """end_btn_selector: 콤마 구분 다중 후보 → Playwright CSS 다중셀렉터로 직접 사용"""
        try:
            sels = end_btn_selector or "a.last, a.num_last, a.btn-last, a[title*='마지막']"
            for btn in reversed(await page.query_selector_all(sels)):
                combined = " ".join(filter(None, [
                    await btn.get_attribute("href")    or "",
                    await btn.get_attribute("onclick") or "",
                    await btn.inner_text()             or "",
                ]))
                m = re.search(
                    r'(?:fn[a-zA-Z_]*|pageIndex|pageNum|pageNo|page|go|move|schPageNo|cp)\s*[\(=]\s*[\'"]?(\d+)[\'"]?',
                    combined, re.IGNORECASE
                )
                if m:
                    total = int(m.group(1))
                    if total > 1: return total

            mx = 1
            for b in await page.query_selector_all(
                ".paging a, .paging2 a, .pagination a, #pagingNav a, .paging strong, ul.paging li a, div.paginate a"
            ):
                t = (await b.inner_text()).strip()
                if t.isdigit(): mx = max(mx, int(t))
            return mx
        except Exception as e:
            print(f"[-] 페이지 수 탐지 실패: {e}", flush=True)
            return 1

    # ── 페이지 이동 ───────────────────────────────────────────────────────────
    @staticmethod
    async def go_to_page(page: Page, next_page: int, paging_sel: str, next_btn_sel: str) -> bool:
        """paging_sel / next_btn_sel: _resolve_selector()로 확정된 단일 셀렉터"""
        p_sel = normalize_selector(paging_sel or "")
        n_sel = normalize_selector(next_btn_sel or "")
        try:
            if await page.evaluate("typeof fn_egov_link_page === 'function'"):
                await page.evaluate(f"fn_egov_link_page({next_page});")
                await page.wait_for_load_state("domcontentloaded")
                return True
        except: pass
        try:
            if p_sel:
                link = page.locator(p_sel).get_by_text(re.compile(f"^{next_page}$"), exact=True).first
                if await link.count() > 0:
                    await link.click()
                    try:
                        await page.wait_for_load_state("domcontentloaded", timeout=10000)
                    except:
                        await page.wait_for_timeout(1000)
                    return True
            if n_sel:
                nxt = page.locator(n_sel).first
                if await nxt.count() > 0:
                    await nxt.click()
                    await page.wait_for_load_state("domcontentloaded")
                    await page.wait_for_timeout(500)
                    return True
        except Exception as e:
            print(f"[!] 페이지 이동 실패: {e}", flush=True)
        return False

    # ── 첨부파일 수집 ────────────────────────────────────────────────────────
    @staticmethod
    async def _extract_attachments(
        container, page: Page, base_url: str,
        req: "PolicyRequest", outbbs_cn: str, year: str,
        download: bool = True,
    ) -> list:
        attachments = []
        seq = 0

        for el in await container.query_selector_all("a, span[onclick], [style*='cursor: pointer']"):
            raw = clean_text(await el.inner_text())
            if not raw: continue
            if any(k in raw for k in ["바로보기", "바로듣기", "미리보기", "뷰어"]): continue

            href    = await el.get_attribute("href")    or ""
            onclick = await el.get_attribute("onclick") or ""
            is_js   = href.startswith(("javascript", "#")) or (onclick and not href)
            url_val = onclick if is_js else (href or onclick)
            if url_val and not url_val.startswith("http"):
                url_val = urljoin(base_url, url_val)

            original_name = raw
            down_path     = ""
            _, ext        = os.path.splitext(raw)

            if download:
                print(f"[*] 첨부 다운로드: {raw}", flush=True)
                try:
                    await el.evaluate("node => { if(node.tagName==='A') node.removeAttribute('target'); }")
                    async with page.expect_download(timeout=15000) as dl_info:
                        await el.click()
                    dl = await dl_info.value
                    suggested = dl.suggested_filename or ""
                    _, ext_dl = os.path.splitext(suggested)
                    if ext_dl: ext = ext_dl
                    original_name = suggested or raw
                    seq += 1
                    down_path = build_attach_save_path(req, year, outbbs_cn, seq, ext or ".bin")
                    await dl.save_as(down_path)
                    down_path = down_path.replace("\\", "/")
                    url_val   = dl.url
                    print(f"[+] 저장: {down_path}", flush=True)
                except Exception as e:
                    print(f"[-] 다운로드 실패 ({raw}): {str(e)[:100]}", flush=True)
                    seq += 1
                    if page.url != base_url:
                        try: await page.goto(base_url, wait_until="domcontentloaded", timeout=5000)
                        except: pass
            else:
                seq += 1

            attachments.append({
                "ORG_FILE_NM": original_name,
                "DOWNPATH":    down_path,
                "DOWNURL":     url_val,
                "FILE_TYPE":   _file_type(ext),
                "CUD_CODE":    "C",
                "REG_DATE":    datetime.now().strftime("%Y-%m-%d"),
            })

        return attachments

    # ── 상세 페이지 파싱 ─────────────────────────────────────────────────────
    @staticmethod
    async def extract_view_detail(
        page: Page,
        view_class_candidates: str,     # 콤마 구분 후보 문자열 (CMS 관리)
        base_url: str,
        req: "PolicyRequest",
        outbbs_cn: str,
        list_title: str = "",
        list_url:   str = "",
    ) -> Tuple[dict, list]:
        p    = req.param
        year = datetime.now().strftime("%Y")

        contents = {
            "OUTBBS_CN":     f"CLIKC{outbbs_cn}",
            "TITLE":         list_title,
            "CONTENT":       "",
            "WRITER":        "",
            "CDATE":         "",
            "URL":           page.url,
            "SEEDURL":       list_url or p.list_url,
            "CATEGORY_ID":   p.category_id,
            "CATEGORY_NAME": p.category_nm,
            "BBS_ID":        p.bbs_id,
            "BBS_NAME":      p.bbs_nm,
            "REGION":        p.region,
            "DOCTYPE":       p.doc_type,
            "ISVIEW":        "Y",
            "SITEID":        p.site_id,
            "SEEDID":        p.seed_id,
            "SITENM":        p.site_nm,
            "SEEDNM":        p.seed_nm,
            "CUD_CODE":      "C",
            "REG_DATE":      datetime.now().strftime("%Y-%m-%d"),
            "FILENAME":      "",
            "FILEPATH":      "",
        }
        attachments = []

        # view_class: 후보군 순서대로 시도 → 확정
        view_class = await _resolve_selector(page, view_class_candidates) if view_class_candidates else None
        if not view_class:
            best, best_len = None, 0
            for t in await page.query_selector_all("table"):
                txt = clean_text(await t.inner_text())
                if len(txt) > best_len:
                    best, best_len = t, len(txt)
            view_class = "table" if best else "body"
            print(f"[*] view_class fallback: {view_class}", flush=True)

        selector = normalize_selector(view_class)
        try:
            await page.wait_for_selector(selector, timeout=10000)
        except Exception as e:
            print(f"[!] 상세 셀렉터 대기 실패 ({selector}): {e}", flush=True)

        await PolicyCrawler._parse_thead_meta(page, selector, contents)
        await PolicyCrawler._parse_label_value_rows(page, selector, contents, attachments, base_url, req, outbbs_cn, year)
        await PolicyCrawler._parse_content_block(page, selector, contents)
        await PolicyCrawler._parse_attachments_fallback(page, selector, attachments, base_url, req, outbbs_cn, year)

        if not contents["TITLE"]:
            for sel in ("thead th strong", "thead th b", "caption",
                        "h1,h2,h3", ".view_title", ".board_title", ".subject"):
                try:
                    el = await page.query_selector(sel)
                    if el:
                        t = clean_text(await el.inner_text())
                        if t:
                            contents["TITLE"] = t
                            break
                except: pass

        if contents.get("CDATE"):
            contents["CDATE"] = _normalize_date(contents["CDATE"])

        contents["DUPLICATION_HASH"] = _make_hash(contents["TITLE"], contents["URL"])

        if attachments:
            contents["FILENAME"] = attachments[0]["ORG_FILE_NM"]
            contents["FILEPATH"] = attachments[0]["DOWNURL"]

        return contents, attachments

    # ── thead 메타 파싱 ───────────────────────────────────────────────────────
    @staticmethod
    async def _parse_thead_meta(page: Page, selector: str, contents: dict):
        try:
            thead_th = await page.query_selector(f"{selector} thead th, {selector} thead td")
            if not thead_th:
                return
            title_el = await thead_th.query_selector("strong, b, h2, h3, .title")
            if title_el:
                t = clean_text(await title_el.inner_text())
                if t and not contents.get("TITLE"):
                    contents["TITLE"] = t
            col_box = await thead_th.query_selector(".colBox, .info, .board_info, .meta, .view_info, p")
            if col_box:
                for span in await col_box.query_selector_all("span, em, li"):
                    cls  = (await span.get_attribute("class") or "").lower()
                    text = clean_text(await span.inner_text())
                    if not text: continue
                    if any(k in cls for k in ("date", "time", "day")):
                        if not contents.get("CDATE"): contents["CDATE"] = text
                    elif any(k in cls for k in ("writer", "author", "dept")):
                        if not contents.get("WRITER"): contents["WRITER"] = text
                    elif any(k in cls for k in ("count", "hit", "view")):
                        pass
                    else:
                        if _DATE_PATTERN.search(text) and not contents.get("CDATE"):
                            contents["CDATE"] = text
                        elif not any(k in text for k in ("조회", "hit", "view")):
                            if not contents.get("CMIT"): contents["CMIT"] = text
            print(f"[+] thead 메타: TITLE={contents.get('TITLE','')[:20]} CDATE={contents.get('CDATE','')}", flush=True)
        except Exception as e:
            print(f"[-] thead 메타 파싱 실패: {e}", flush=True)

    # ── th/td · dt/dd 라벨-값 쌍 파싱 ────────────────────────────────────────
    @staticmethod
    async def _parse_label_value_rows(
        page: Page, selector: str, contents: dict,
        attachments: list, base_url: str,
        req: "PolicyRequest", outbbs_cn: str, year: str,
    ):
        try:
            rows = await page.query_selector_all(
                f"{selector} tbody tr, {selector} dl, "
                f"{selector} .view-row, {selector} .board-field"
            )
            for row in rows:
                ths = await row.query_selector_all("th, dt, .label, .field-label")
                tds = await row.query_selector_all("td, dd, .value, .field-value")
                if not ths or not tds:
                    continue
                for th, td in zip(ths, tds):
                    label = clean_text(await th.inner_text())
                    val   = clean_text(await td.inner_text())
                    col   = _label_to_col(label)
                    if col:
                        if not contents.get(col):
                            contents[col] = val
                    elif any(k in label for k in ATTACH_HINT_KEYS):
                        att = await PolicyCrawler._extract_attachments(
                            td, page, base_url, req, outbbs_cn, year,
                            download=req.param.download_attach,
                        )
                        attachments.extend(att)
        except Exception as e:
            print(f"[-] 라벨-값 파싱 실패: {e}", flush=True)

    # ── 본문 블록 수집 ────────────────────────────────────────────────────────
    @staticmethod
    async def _parse_content_block(page: Page, selector: str, contents: dict):
        if contents.get("CONTENT"):
            return
        for content_sel in (
            f"{selector} td.onlyCont",
            f"{selector} .content",
            f"{selector} .view_content_area",
            f"{selector} .bbs_cont",
            f"{selector} .board_content",
            f"{selector} td.cont",
            f"{selector} div.text",
        ):
            try:
                el = await page.query_selector(content_sel)
                if el:
                    contents["CONTENT"] = await el.inner_html()
                    return
            except: pass

    # ── 첨부파일 fallback ─────────────────────────────────────────────────────
    @staticmethod
    async def _parse_attachments_fallback(
        page: Page, selector: str, attachments: list,
        base_url: str, req: "PolicyRequest", outbbs_cn: str, year: str,
    ):
        if attachments:
            return
        for att_sel in (
            f"{selector} .attach_area", f"{selector} .file_list",
            f"{selector} .attached-files", f"{selector} ul.file",
            f"{selector} .board_attach", "div.attach", "#attachFileList",
        ):
            try:
                el = await page.query_selector(att_sel)
                if el:
                    att = await PolicyCrawler._extract_attachments(
                        el, page, base_url, req, outbbs_cn, year,
                        download=req.param.download_attach,
                    )
                    attachments.extend(att)
                    if att: return
            except: pass
        body = await page.query_selector(selector)
        if body:
            attachments.extend(await PolicyCrawler._extract_attachments(
                body, page, base_url, req, outbbs_cn, year,
                download=req.param.download_attach,
            ))


# ── 공통 목록 수집 루프 ────────────────────────────────────────────────────────

async def _collect_list_pages(page: Page, req: PolicyRequest, stop_event: asyncio.Event) -> Tuple[list, list]:
    p          = req.param
    error_logs = []

    await page.goto(p.list_url, wait_until="domcontentloaded", timeout=30000)

    # ── 셀렉터 확정: 콤마 후보 → _resolve_selector → 단일값 ──────────────────
    list_class = await _resolve_selector(page, p.list_class, check_children="tbody tr")
    if not list_class:
        # ul > li 구조 재시도
        list_class = await _resolve_selector(page, p.list_class, check_children="li")
    if not list_class:
        msg = f"list_class 후보 모두 불일치 → CMS에 태그 추가 필요: [{p.list_class}]"
        error_logs.append({"step": "목록탐지", "error": msg})
        print(f"[!] {msg}", flush=True)
        return [], error_logs

    paging_sel = await _resolve_selector(page, p.paging_selector)
    next_sel   = await _resolve_selector(page, p.next_btn_selector)
    end_sel    = await _resolve_selector(page, p.end_btn_selector)
    print(f"[+] 확정 → list='{list_class}' paging='{paging_sel}' next='{next_sel}' end='{end_sel}'", flush=True)

    try:
        await page.wait_for_selector(normalize_selector(list_class), timeout=10000)
    except Exception as e:
        error_logs.append({"step": "목록대기", "selector": list_class, "error": str(e)[:300]})
        return [], error_logs

    # get_total_pages 에는 원본 다중 후보 문자열 그대로 (Playwright CSS 다중셀렉터 지원)
    total    = await PolicyCrawler.get_total_pages(page, p.end_btn_selector)
    safe_max = int(p.max_pages.strip()) if p.max_pages and p.max_pages.strip().isdigit() else 0
    target   = total if safe_max == 0 else min(safe_max, total)
    all_items = []

    for cp in range(1, target + 1):
        if stop_event.is_set():
            print("[!] 중단 요청 감지 (목록 루프)", flush=True)
            break
        print(f"[*] 목록 수집: {cp}/{target}p", flush=True)
        try:
            all_items.extend(
                await PolicyCrawler.extract_list_page(page, list_class, p.view_id_param)
            )
        except Exception as e:
            error_logs.append({"step": f"목록수집_{cp}p", "selector": list_class, "error": str(e)[:300]})
            print(f"[!] {cp}p 목록 실패: {e}", flush=True)

        if cp < target and not stop_event.is_set():
            ok = await PolicyCrawler.go_to_page(page, cp + 1, paging_sel, next_sel)
            if not ok:
                await _try_url_fallback(page, cp + 1)

    return all_items, error_logs


# ── 실행 엔진 ─────────────────────────────────────────────────────────────────

async def execute_policy_scraping(req: PolicyRequest):
    # ── 태스크 전용 중단 이벤트 생성 후 app.state에 등록 ──────────────────────
    stop_event = asyncio.Event()
    app.state.current_stop_event = stop_event
    # ─────────────────────────────────────────────────────────────────────────
    p             = req.param
    domain        = extract_domain(p.list_url)
    contents_list: list = []
    error_logs:    list = []
    filepath = None

    async with async_playwright() as playwright:
        browser, page = await _setup_browser(playwright)
        try:
            print(f"\n{'='*60}", flush=True)
            print(f"[*] [1단계] 목록 수집: {p.list_url}", flush=True)
            list_data, collect_errs = await _collect_list_pages(page, req, stop_event)
            error_logs.extend(collect_errs)

            if not list_data:
                error_logs.append({"step": "1단계", "url": p.list_url, "error": "목록 0건"})

            total = len(list_data)
            print(f"\n[*] [2단계] 상세 수집 시작 ({total}건)", flush=True)

            for idx, item in enumerate(list_data):
                if stop_event.is_set():
                    print(f"[!] {idx}번째에서 중단 (stop_event)", flush=True)
                    break

                href    = item.get("link_href", "")
                vid     = item.get("view_id")
                is_real = href and not href.startswith(("#", "javascript"))

                if is_real:
                    target_url = urljoin(p.list_url, href)
                elif p.view_url and vid:
                    sep        = "&" if "?" in p.view_url else "?"
                    target_url = f"{p.view_url}{sep}{p.view_id_param or 'id'}={vid}"
                else:
                    print(f"[!] 상세 URL 구성 불가 (idx={idx}) href={href!r} vid={vid!r}", flush=True)
                    error_logs.append({
                        "step": "2단계_URL구성불가", "idx": idx,
                        "href": href, "view_id": vid,
                        "error": "_JS_VIEW_BUILDERS 에 JS 패턴 추가 필요",
                    })
                    continue

                print(f"[*] 상세 ({idx+1}/{total}) → {target_url}", flush=True)
                outbbs_cn = str(time.time_ns())[:16]

                try:
                    # ── page.goto 전 재확인: 네트워크 대기 중 stop 눌렸을 때 즉시 탈출 ──
                    if stop_event.is_set():
                        print(f"[!] goto 진입 전 중단 감지 (idx={idx})", flush=True)
                        break

                    await page.goto(target_url, wait_until="domcontentloaded", timeout=15000)
                    parsed = urlparse(target_url)
                    base   = f"{parsed.scheme}://{parsed.netloc}"

                    conts, _ = await PolicyCrawler.extract_view_detail(
                        page, p.view_class, base, req, outbbs_cn,
                        list_title=item.get("title", ""),
                        list_url=p.list_url,
                    )
                    conts["URL"] = target_url
                    contents_list.append(conts)

                except Exception as e:
                    print(f"    [!] 상세 실패 ({vid}): {e}", flush=True)
                    error_logs.append({
                        "step": "2단계_상세수집", "view_id": vid,
                        "url": target_url, "error": str(e)[:300],
                    })

            result_block = _build_result(contents_list, error_logs, stop_event.is_set())
            full_payload = {
                "reqId":    req.req_id,  "type":    req.type,
                "crwId":    req.crw_id,  "fileDir": req.file_dir,
                "result":   result_block,
                "data":     contents_list,
                "log":      error_logs,
            }

            if contents_list or error_logs:
                label    = req.type if contents_list else req.type + "_error"
                filepath = save_to_json(full_payload, domain, label)
                print(f"[OK] 저장 완료 ({len(contents_list)}건): {filepath}", flush=True)
                await send_to_insert_api(
                    req_id=req.req_id, type_val=req.type, crw_id=req.crw_id,
                    file_dir=req.file_dir, result=result_block,
                    contents=contents_list, error_logs=error_logs,
                )
            else:
                print("[!] 수집 데이터 없음, 전송 생략", flush=True)

            return {
                "req_id": req.req_id, "type": req.type, "crw_id": req.crw_id,
                "file_dir": req.file_dir, "ok": True,
                "interrupted":    stop_event.is_set(),
                "contents_count": len(contents_list),
                "saved_file":     filepath,
            }

        except Exception as e:
            print(f"\n[!] 전체 에러: {e}", flush=True)
            return {"req_id": req.req_id, "type": req.type, "crw_id": req.crw_id,
                    "file_dir": req.file_dir, "ok": False, "error_msg": str(e)}
        finally:
            app.state.current_stop_event = None   # 태스크 종료 후 이벤트 슬롯 해제
            await browser.close()


async def execute_policy_scraping_test(req: PolicyRequest) -> dict:
    """테스트: 1건만 수집"""
    p = req.param
    contents_list = []

    async with async_playwright() as playwright:
        browser, page = await _setup_browser(playwright)
        try:
            await page.goto(p.list_url, wait_until="domcontentloaded", timeout=30000)

            list_class = (
                await _resolve_selector(page, p.list_class, check_children="tbody tr") or
                await _resolve_selector(page, p.list_class, check_children="li") or
                await _resolve_selector(page, p.list_class)
            )
            if not list_class:
                return {"req_id": req.req_id, "type": req.type, "crw_id": req.crw_id,
                        "file_dir": req.file_dir, "data": [],
                        "error": f"list_class 후보 불일치 → CMS에 태그 추가 필요: [{p.list_class}]"}

            await page.wait_for_selector(normalize_selector(list_class), timeout=10000)
            list_data = await PolicyCrawler.extract_list_page(page, list_class, p.view_id_param)

            if not list_data:
                return {"req_id": req.req_id, "type": req.type, "crw_id": req.crw_id,
                        "file_dir": req.file_dir, "data": []}

            item    = list_data[0]
            href    = item.get("link_href", "")
            vid     = item.get("view_id", "")
            is_real = href and not href.startswith(("#", "javascript"))

            if is_real:
                target_url = urljoin(p.list_url, href)
            elif p.view_url and vid:
                target_url = f"{p.view_url}?{p.view_id_param or 'id'}={vid}"
            else:
                return {"req_id": req.req_id, "type": req.type, "crw_id": req.crw_id,
                        "file_dir": req.file_dir, "data": [],
                        "error": f"상세 URL 구성 불가 href={href!r} vid={vid!r}"}

            outbbs_cn = str(time.time_ns())[:16]
            await page.goto(target_url, wait_until="domcontentloaded", timeout=15000)
            parsed = urlparse(target_url)
            base   = f"{parsed.scheme}://{parsed.netloc}"

            conts, _ = await PolicyCrawler.extract_view_detail(
                page, p.view_class, base, req, outbbs_cn,
                list_title=item.get("title", ""),
                list_url=p.list_url,
            )
            conts["URL"] = target_url
            contents_list.append(conts)
            print(f"[TEST] 완료: OUTBBS_CN={conts['OUTBBS_CN']}", flush=True)

            return {"req_id": req.req_id, "type": req.type,
                    "crw_id": req.crw_id, "file_dir": req.file_dir,
                    "data": contents_list}

        except Exception as e:
            print(f"[TEST] 에러: {e}", flush=True)
            return {"req_id": req.req_id, "type": req.type, "crw_id": req.crw_id,
                    "file_dir": req.file_dir, "data": []}
        finally:
            await browser.close()


# ── insert API 전송 ────────────────────────────────────────────────────────────

async def send_to_insert_api(
    req_id: str, type_val: str, crw_id: str, file_dir: str,
    result: dict, contents: list, error_logs: list = None,
):
    target_url = "http://10.201.38.157:8080/insert_api.do"
    payload = {
        "reqId": req_id, "type": type_val, "crwId": crw_id, "fileDir": file_dir,
        "result": result, "data": contents,
        "log": error_logs or [],
    }
    print(f"\n[*] [3단계] 전송 시도", flush=True)
    await _do_send(target_url, payload)
    print(f"[OK] 전송 접수 완료", flush=True)
    return True

async def _do_send(target_url: str, payload: dict):
    async with httpx.AsyncClient() as client:
        try:
            resp = await client.post(target_url, json=payload, timeout=120.0)
            print(f"[OK] API 전송 성공" if resp.status_code == 200 else f"[!] 응답: {resp.status_code}", flush=True)
        except Exception as e:
            print(f"[!] 네트워크 오류: {str(e)}", flush=True)

async def handle_policy_request(req: PolicyRequest, background_tasks: BackgroundTasks):
    try:
        background_tasks.add_task(execute_policy_scraping, req)
        return {"req_id": req.req_id, "type": req.type, "crw_id": req.crw_id,
                "file_dir": req.file_dir, "ok": True, "message": "수집 요청 완료"}
    except Exception as e:
        return error_response(f"요청 처리 중 오류: {str(e)}")