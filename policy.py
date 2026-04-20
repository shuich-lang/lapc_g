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
VIEW_ID_AUTO_PARAMS = r"[?&](uid|idx|code|no|seq|id|nttId|uuid|bbsSeq|ntNo|articleSeq|list_no|postId|num|docId)=([^&]+)"
_DATE_PATTERN       = re.compile(r'(\d{4})[./\-](\d{1,2})[./\-](\d{1,2})')
ATTACH_HINT_KEYS    = ["첨부", "파일", "원문", "자료", "다운", "down", "file", "attach"]
IMAGE_EXTS          = {".jpg", ".jpeg", ".png", ".gif", ".bmp", ".webp", ".svg"}
 
app = FastAPI(title="Policy Board Scraper API")
app.state.stop_scraping = False
 
 
# ── 요청 모델 ─────────────────────────────────────────────────────────────────
 
class PolicyParam(BaseModel):
    """
    게시판형 정책자료 수집 파라미터.
    bill.py 와 동일한 최소 파라미터 세트 + CONTENTS 메타 고정값.
    대수(회기) 검색 없음.
    """
    # ── 필수 ──────────────────────────────────────────────────────────────────
    list_url:          str           = Field(...,                  description="게시판 목록 URL")
    list_class:        str           = Field("table.board_list",   description="목록 테이블/컨테이너 CSS 셀렉터")
    paging_selector:   str           = Field("div#pagingNav",      description="페이징 영역 셀렉터")
    next_btn_selector: str           = Field("a.num_right",        description="다음 버튼 셀렉터")
    end_btn_selector:  str           = Field("a.num_last",         description="마지막 페이지 버튼 셀렉터")
 
    # ── 선택 (없으면 자동 탐지) ───────────────────────────────────────────────
    view_url:          Optional[str] = Field(None, description="상세 기본 URL (href 우선, 없을 때 파라미터 조립용)")
    view_id_param:     str           = Field("",   description="상세 식별 파라미터명 (빈값=path 타입 자동 탐지)")
    view_class:        Optional[str] = Field(None, description="상세 콘텐츠 영역 CSS 셀렉터 (None=자동 탐지)")
    max_pages:         str           = Field("",   description="최대 수집 페이지 수 (빈값/0=전체)")
    download_attach:   bool          = Field(True, description="첨부파일 실제 다운로드 여부")
 
    # ── CONTENTS 테이블 고정 메타 (사이트마다 CMS에서 주입) ──────────────────
    site_id:           str           = Field("", description="SITEID")
    seed_id:           str           = Field("", description="SEEDID")
    site_nm:           str           = Field("", description="SITENM (사이트 명칭)")
    seed_nm:           str           = Field("", description="SEEDNM (수집 경로)")
    category_id:       str           = Field("", description="CATEGORY_ID")
    category_nm:       str           = Field("", description="CATEGORY_NAME")
    bbs_id:            str           = Field("", description="BBS_ID")
    bbs_nm:            str           = Field("", description="BBS_NAME")
    region:            str           = Field("", description="REGION (지역코드, 예: 051)")
    doc_type:          str           = Field("", description="DOCTYPE")
 
 
class PolicyRequest(BaseModel):
    req_id:   str         = Field(..., min_length=1, description="요청 식별자")
    type:     str         = Field(..., min_length=1, description="수집 타입 (policyinfo)")
    crw_id:   str         = Field(..., min_length=1, description="크롤러 식별자")
    file_dir: str         = Field(...,               description="파일 저장 디렉토리 루트")
    param:    PolicyParam = Field(...,               description="크롤링 상세 설정")
 
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
# 신규 라벨 추가는 이 딕셔너리만 수정
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
    # 포함 검사 fallback
    if any(k in norm for k in ("제목", "Title")):                        return "TITLE"
    if any(k in norm for k in ("작성자","등록자","담당자","부서")):       return "WRITER"
    if any(k in norm for k in ("작성일","등록일","게시일","공고일","날짜","일시")): return "CDATE"
    if any(k in norm for k in ("내용", "본문")):                         return "CONTENT"
    if "위원회" in norm:                                                  return "CMIT"
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
 
 
# ── 크롤링 엔진 ───────────────────────────────────────────────────────────────
 
class PolicyCrawler:
 
    # ── (1) view_id 추출 ──────────────────────────────────────────────────────
    @staticmethod
    def _extract_view_id(href: str, onclick: str, row_html: str, view_id_param: str) -> Optional[str]:
        clean = href.replace("&amp;", "&") if href else ""
 
        # path 타입 (view_id_param 빈값)
        if not view_id_param or not view_id_param.strip():
            if clean and not clean.startswith(("javascript", "#")):
                m = re.search(r"/(\d+)(?:[/?#]|$)", clean)
                if m: return m.group(1)
 
        if clean and not clean.startswith(("javascript", "#")):
            m = re.search(rf"[?&]{re.escape(view_id_param)}=([^&]+)", clean)
            if m: return m.group(1)
            m = re.search(VIEW_ID_AUTO_PARAMS, clean, re.IGNORECASE)
            if m: return m.group(2)
 
        m = re.search(rf"[?&]?{re.escape(view_id_param)}=([^&\"'>\s]+)", row_html)
        if m: return m.group(1)
 
        js = onclick or (href if href and href.startswith("javascript") else "")
        if js:
            m = re.search(r"\(['\"]?([^'\"),]+)['\"]?\)", js)
            if m: return m.group(1)
 
        return None
 
    # ── (2) 행 링크 추출 ──────────────────────────────────────────────────────
    @staticmethod
    async def _get_row_link(row, tds) -> dict:
        info       = {"href": "", "onclick": "", "title": ""}
        tr_onclick = await row.get_attribute("onclick") or ""
        a_tag      = await row.query_selector("a")
        if a_tag:
            info["href"]    = await a_tag.get_attribute("href") or ""
            info["onclick"] = await a_tag.get_attribute("onclick") or tr_onclick
            text            = clean_text(await a_tag.inner_text())
            if text: info["title"] = text
        else:
            info["onclick"] = tr_onclick
            for td in tds:
                t = await td.get_attribute("title")
                if t:
                    info["title"] = clean_text(t)
                    break
        return info
 
    # ── (3) 목록 페이지 추출 ─────────────────────────────────────────────────
    @staticmethod
    async def extract_list_page(page: Page, list_class: str, view_id_param: str = "") -> List[Dict[str, Any]]:
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
 
    # ── (4) 전체 페이지 수 탐지 ──────────────────────────────────────────────
    @staticmethod
    async def get_total_pages(page: Page, end_btn_selector: str = None) -> int:
        try:
            base = [
                "a.last", "a.num_last", "a.btn-last", "a.direction.last",
                "a.btn.end", "a[title*='마지막']", "a:has-text('»')", "a:has-text('>>')",
            ]
            sels = ([end_btn_selector] + base) if end_btn_selector else base
            for btn in reversed(await page.query_selector_all(", ".join(sels))):
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
                ".paging a, .paging2 a, .pagination a, #pagingNav a, .paging strong, ul.paging li a"
            ):
                t = (await b.inner_text()).strip()
                if t.isdigit(): mx = max(mx, int(t))
            return mx
        except Exception as e:
            print(f"[-] 페이지 수 탐지 실패: {e}", flush=True)
            return 1
 
    # ── (5) 페이지 이동 ───────────────────────────────────────────────────────
    @staticmethod
    async def go_to_page(page: Page, next_page: int, paging_sel: str, next_btn_sel: str) -> bool:
        p_sel = normalize_selector(paging_sel)
        n_sel = normalize_selector(next_btn_sel)
        try:
            if await page.evaluate("typeof fn_egov_link_page === 'function'"):
                await page.evaluate(f"fn_egov_link_page({next_page});")
                await page.wait_for_load_state("domcontentloaded")
                return True
        except: pass
        try:
            link = page.locator(p_sel).get_by_text(re.compile(f"^{next_page}$"), exact=True).first
            if await link.count() > 0:
                await link.click()
                try:
                    await page.wait_for_load_state("domcontentloaded", timeout=10000)
                except:
                    await page.wait_for_timeout(1000)
                return True
            nxt = page.locator(n_sel).first
            if await nxt.count() > 0:
                await nxt.click()
                await page.wait_for_load_state("domcontentloaded")
                await page.wait_for_timeout(500)
                return True
        except Exception as e:
            print(f"[!] 페이지 이동 실패: {e}", flush=True)
        return False
 
    # ── (6) 첨부파일 수집 ────────────────────────────────────────────────────
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
 
    # ── (7) 상세 페이지 파싱 ─────────────────────────────────────────────────
    @staticmethod
    async def extract_view_detail(
        page: Page,
        view_class: Optional[str],
        base_url: str,
        req: "PolicyRequest",
        outbbs_cn: str,
        list_title: str = "",
        list_url:   str = "",
    ) -> Tuple[dict, list]:
        p         = req.param
        year      = datetime.now().strftime("%Y")
 
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
 
        # view_class 자동 탐지
        if not view_class:
            view_class = await PolicyCrawler._detect_view_class(page)
 
        selector = normalize_selector(view_class) if view_class else "body"
        try:
            if view_class:
                await page.wait_for_selector(selector, timeout=10000)
        except Exception as e:
            print(f"[!] 상세 셀렉터 대기 실패 ({selector}): {e}", flush=True)
 
        # ── 파싱 전략 (순서대로) ──────────────────────────────────────────────
        # 전략 1: thead 안 메타 (부산광역시의회 등 strong+span 구조)
        await PolicyCrawler._parse_thead_meta(page, selector, contents)
        # 전략 2: th/td, dt/dd 라벨-값 쌍 (일반 게시판 상세)
        await PolicyCrawler._parse_label_value_rows(page, selector, contents, attachments, base_url, req, outbbs_cn, year)
        # 전략 3: 본문 블록 직접 수집
        await PolicyCrawler._parse_content_block(page, selector, contents)
        # 전략 4: 첨부파일 전용 영역 / 전체 스캔 fallback
        await PolicyCrawler._parse_attachments_fallback(page, selector, attachments, base_url, req, outbbs_cn, year)
 
        # 제목 최종 보완 (위 전략들 모두 실패 시)
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
 
        # CDATE 정규화
        if contents.get("CDATE"):
            contents["CDATE"] = _normalize_date(contents["CDATE"])
 
        # DUPLICATION_HASH
        contents["DUPLICATION_HASH"] = _make_hash(contents["TITLE"], contents["URL"])
 
        # FILENAME / FILEPATH 레거시 호환 (첫 번째 첨부 기록)
        if attachments:
            contents["FILENAME"] = attachments[0]["ORG_FILE_NM"]
            contents["FILEPATH"] = attachments[0]["DOWNURL"]
 
        return contents, attachments
 
    # ── (7-1) thead 메타 파싱 ────────────────────────────────────────────────
    # 대상 구조 (부산광역시의회 등):
    #   <thead><tr><th>
    #     <strong>제목텍스트</strong>
    #     <p class="colBox">
    #       <span class="name">제330회임시회</span>
    #       <span class="name">운영위원회</span>
    #       <span class="date">2025.07.28</span>
    #       <span class="count">조회수 : 545</span>
    #     </p>
    #   </th></tr></thead>
    @staticmethod
    async def _parse_thead_meta(page: Page, selector: str, contents: dict):
        try:
            thead_th = await page.query_selector(f"{selector} thead th, {selector} thead td")
            if not thead_th:
                return
 
            # 제목: <strong> / <b> 우선
            title_el = await thead_th.query_selector("strong, b, h2, h3, .title")
            if title_el:
                t = clean_text(await title_el.inner_text())
                if t and not contents.get("TITLE"):
                    contents["TITLE"] = t
 
            # colBox(또는 유사 클래스) 안 span 들
            col_box = await thead_th.query_selector(
                ".colBox, .info, .board_info, .meta, .view_info, p"
            )
            if col_box:
                for span in await col_box.query_selector_all("span, em, li"):
                    cls  = (await span.get_attribute("class") or "").lower()
                    text = clean_text(await span.inner_text())
                    if not text:
                        continue
 
                    # 클래스명으로 1차 분류
                    if any(k in cls for k in ("date", "time", "day")):
                        if not contents.get("CDATE"):
                            contents["CDATE"] = text
                    elif any(k in cls for k in ("writer", "author", "dept")):
                        if not contents.get("WRITER"):
                            contents["WRITER"] = text
                    elif any(k in cls for k in ("count", "hit", "view")):
                        pass  # 조회수 무시
                    else:
                        # 클래스 단서 없으면 텍스트 패턴으로 판단
                        if _DATE_PATTERN.search(text) and not contents.get("CDATE"):
                            contents["CDATE"] = text
                        elif any(k in text for k in ("조회", "hit", "view")):
                            pass
                        else:
                            # 나머지: 회기/위원회명 등 → CMIT
                            if not contents.get("CMIT"):
                                contents["CMIT"] = text
 
            print(
                f"[+] thead 메타: TITLE={contents.get('TITLE','')[:20]} "
                f"CDATE={contents.get('CDATE','')} CMIT={contents.get('CMIT','')}",
                flush=True,
            )
        except Exception as e:
            print(f"[-] thead 메타 파싱 실패: {e}", flush=True)
 
    # ── (7-2) th/td · dt/dd 라벨-값 쌍 파싱 ─────────────────────────────────
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
 
    # ── (7-3) 본문 블록 수집 ──────────────────────────────────────────────────
    @staticmethod
    async def _parse_content_block(page: Page, selector: str, contents: dict):
        if contents.get("CONTENT"):
            return
        # td.onlyCont → 부산광역시의회 본문 클래스 (명시 우선)
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
                    html  = await el.inner_html()           # ← <p>, &nbsp; 등 원본 HTML 그대로
                    plain = clean_text(await el.inner_text()) # 길이 판단용으로만 사용 (빈 컨테이너 제외)
                    contents["CONTENT"] = html
            except: pass
 
    # ── (7-4) 첨부파일 fallback ───────────────────────────────────────────────
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
 
        # 최후 수단: 상세 영역 전체 스캔
        body = await page.query_selector(selector)
        if body:
            att = await PolicyCrawler._extract_attachments(
                body, page, base_url, req, outbbs_cn, year,
                download=req.param.download_attach,
            )
            attachments.extend(att)
 
    # ── (7-5) view_class 자동 탐지 ────────────────────────────────────────────
    @staticmethod
    async def _detect_view_class(page: Page) -> Optional[str]:
        for sel in (
            "div.view_content", "div.board_view", "div#content .view",
            "div.cont_area", "div.article_view", "article.post",
            "div#boardView", "div.bbs-view", "table.board_view",
            "div.view-box", "div.board-view-wrap",
            "table.cellType_a",  # 부산광역시의회
            "table.view",
        ):
            el = await page.query_selector(sel)
            if el:
                print(f"[*] view_class 자동 탐지: {sel}", flush=True)
                return sel
        # fallback: 텍스트가 가장 긴 <table>
        best, best_len = None, 0
        for t in await page.query_selector_all("table"):
            txt = clean_text(await t.inner_text())
            if len(txt) > best_len:
                best, best_len = t, len(txt)
        return "table" if best else None
 
 
# ── 공통 목록 수집 루프 ────────────────────────────────────────────────────────
 
async def _collect_list_pages(page: Page, req: PolicyRequest, stop_check) -> Tuple[list, list]:
    p          = req.param
    error_logs = []
 
    await page.goto(p.list_url, wait_until="domcontentloaded", timeout=30000)
 
    try:
        await page.wait_for_selector(normalize_selector(p.list_class), timeout=10000)
    except Exception as e:
        error_logs.append({"step": "목록대기", "selector": p.list_class, "error": str(e)[:300]})
        print(f"[!] 목록 셀렉터 대기 실패: {e}", flush=True)
        return [], error_logs
 
    total    = await PolicyCrawler.get_total_pages(page, p.end_btn_selector)
    safe_max = int(p.max_pages.strip()) if p.max_pages and p.max_pages.strip().isdigit() else 0
    target   = total if safe_max == 0 else min(safe_max, total)
    all_items = []
 
    for cp in range(1, target + 1):
        if stop_check():
            print("[!] 중단 요청 감지", flush=True)
            break
        print(f"[*] 목록 수집: {cp}/{target}p", flush=True)
        try:
            all_items.extend(await PolicyCrawler.extract_list_page(page, p.list_class, p.view_id_param))
        except Exception as e:
            error_logs.append({"step": f"목록수집_{cp}p", "selector": p.list_class, "error": str(e)[:300]})
            print(f"[!] {cp}p 목록 실패: {e}", flush=True)
 
        if cp < target:
            ok = await PolicyCrawler.go_to_page(page, cp + 1, p.paging_selector, p.next_btn_selector)
            if not ok:
                await _try_url_fallback(page, cp + 1)
 
    return all_items, error_logs
 
 
# ── 실행 엔진 ─────────────────────────────────────────────────────────────────
 
async def execute_policy_scraping(req: PolicyRequest):
    app.state.stop_scraping = False
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
            list_data, collect_errs = await _collect_list_pages(page, req, lambda: app.state.stop_scraping)
            error_logs.extend(collect_errs)
 
            if not list_data:
                error_logs.append({"step": "1단계", "url": p.list_url, "error": "목록 0건"})
 
            total = len(list_data)
            print(f"\n[*] [2단계] 상세 수집 시작 ({total}건)", flush=True)
 
            for idx, item in enumerate(list_data):
                if app.state.stop_scraping:
                    print(f"[!] {idx}번째에서 중단", flush=True)
                    break
 
                vid  = item.get("view_id")
                href = item.get("link_href", "")
                if not vid and not href:
                    continue
 
                is_real = href and not href.startswith(("#", "javascript"))
                if is_real:
                    target_url = urljoin(p.list_url, href)
                elif p.view_url and vid:
                    sep        = "&" if "?" in p.view_url else "?"
                    target_url = f"{p.view_url}{sep}{p.view_id_param}={vid}"
                else:
                    print(f"[!] 상세 URL 구성 불가 (idx={idx}), 건너뜀", flush=True)
                    continue
 
                print(f"[*] 상세 ({idx+1}/{total}) → {target_url}", flush=True)
                outbbs_cn = str(time.time_ns())[:16]
 
                try:
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
 
            result_block = _build_result(contents_list, error_logs, app.state.stop_scraping)
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
                "interrupted":    app.state.stop_scraping,
                "contents_count": len(contents_list),
                "saved_file":     filepath,
            }
 
        except Exception as e:
            print(f"\n[!] 전체 에러: {e}", flush=True)
            return {"req_id": req.req_id, "type": req.type, "crw_id": req.crw_id,
                    "file_dir": req.file_dir, "ok": False, "error_msg": str(e)}
        finally:
            await browser.close()
 
 
async def execute_policy_scraping_test(req: PolicyRequest) -> dict:
    """테스트: 1건만 수집"""
    p = req.param
    contents_list = []
 
    async with async_playwright() as playwright:
        browser, page = await _setup_browser(playwright)
        try:
            await page.goto(p.list_url, wait_until="domcontentloaded", timeout=30000)
            await page.wait_for_selector(normalize_selector(p.list_class), timeout=10000)
            list_data = await PolicyCrawler.extract_list_page(page, p.list_class, p.view_id_param)
 
            if not list_data:
                return {"req_id": req.req_id, "type": req.type, "crw_id": req.crw_id,
                        "file_dir": req.file_dir, "data": []}
 
            item    = list_data[0]
            vid     = item.get("view_id", "")
            href    = item.get("link_href", "")
            is_real = href and not href.startswith(("#", "javascript"))
 
            if not is_real and not (p.view_url and vid):
                return {"req_id": req.req_id, "type": req.type, "crw_id": req.crw_id,
                        "file_dir": req.file_dir, "data": []}
 
            target_url = urljoin(p.list_url, href) if is_real else f"{p.view_url}?{p.view_id_param}={vid}"
            outbbs_cn      = str(time.time_ns())[:16]
 
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
 
            return {
                "req_id": req.req_id, "type": req.type,
                "crw_id": req.crw_id, "file_dir": req.file_dir,
                "data": contents_list,
            }
 
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
 
@app.get("/policy/stop")
async def api_stop():
    app.state.stop_scraping = True
    print("[!] 외부 중단 요청 수신", flush=True)
    return {"ok": True, "message": "Stop requested."}