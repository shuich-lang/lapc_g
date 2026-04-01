import asyncio
import json
import os
import re
from datetime import datetime
from typing import List, Dict, Any, Optional
from urllib.parse import urljoin, urlparse

from fastapi import FastAPI, Query, Request
from playwright.async_api import async_playwright, Page

try:
    from field_maps.field_map import FIELD_MAP, SECTION_FIELD_MAP
except ImportError:
    print("[!] field_maps/field_map.py 로드 실패, 기본값 사용")
    FIELD_MAP, SECTION_FIELD_MAP = {}, {}

app = FastAPI(title="Enterprise Council Scraper API")
app.state.stop_scraping = False
DOWNLOAD_DIR = "download"
FILE_DOWNLOAD_DIR = "attachment"
JOB_STORE: Dict[str, Dict[str, Any]] = {}

# 차단할 리소스 타입
BLOCKED_RESOURCES = {"image", "stylesheet", "media", "font"}
# view_id 자동 탐지용 파라미터 패턴
VIEW_ID_AUTO_PARAMS = r"[?&](uid|idx|code|no|seq|id|bill_no|billNo|idx_no|nttId|uuid)=([^&]+)"
# 페이지 파라미터 패턴 (페이지네이션 URL 치환용)
PAGE_PARAM_PATTERN = r'([?&](?:page|pageIndex|p|page_no|pageno|cPage|pageNum|page_id))=(\d+)'
# 검색 버튼 함정 단어 (상단 메뉴 탭 배제용)
TRAP_WORDS = ["엑셀", "초기화", "취소", "통합", "메뉴", "상세", "회기", "의안", "별검색", "다운", "연혁"]


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
    """
    클래스명(.name)이나 ID(#name)만 들어와도 Playwright가 인식할 수 있는 표준 셀렉터로 반환합니다.
    """
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


# --- 브라우저 / 페이지네이션 헬퍼 ---

async def _setup_browser(pw):
    """헤드리스 브라우저 생성 + 불필요 리소스 차단"""
    browser = await pw.chromium.launch(headless=True)
    page = await browser.new_page()
    await page.route("**/*", lambda r: r.abort() if r.request.resource_type in BLOCKED_RESOURCES else r.continue_())
    return browser, page

async def _collect_pages(page, list_url, numpr, list_class, vid_param,
                         max_pages, paging_sel, next_btn_sel, extractor, stop_check, search_form_selector, numpr_select_selector, search_btn_selector):
    """공통 리스트 수집 루프 (필터 + 페이지네이션)"""
    await page.goto(list_url, wait_until="domcontentloaded", timeout=30000)
    
    if numpr and numpr.strip():
        # 검색 후 list_class가 나타날 때까지 기다리도록 인자 추가
        await UniversalCrawler.apply_filter_and_search(page, numpr.strip(), list_class, search_form_selector, numpr_select_selector, search_btn_selector)
    else:
        # 검색 안 할 때도 리스트는 기다려야 함
        await page.wait_for_selector(normalize_selector(list_class), timeout=10000)

    total = await UniversalCrawler.get_total_pages(page)
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
            print(f"[!] {cp}p 실패 (건너뜀): {e}", flush=True)
        if cp < target and not await UniversalCrawler.go_to_page(page, cp + 1, paging_sel, next_btn_sel):
            await _try_url_fallback(page, cp + 1)
    return data

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
                    async with page.expect_navigation(timeout=10000):
                        await btn.click()
                    print("[+] 검색 버튼 클릭 성공", flush=True)
            except:
                # 버튼 클릭 실패 시 폼 직접 제출 (form_sel 활용)
                print(f"[!] 버튼 클릭 실패, 폼({form_sel}) 직접 제출 시도...", flush=True)
                await page.evaluate(f"document.querySelector('{form_sel}')?.submit()")
                await page.wait_for_load_state("networkidle")

            # 3. 결과 로딩 확인
            if list_class:
                await page.wait_for_selector(normalize_selector(list_class), timeout=10000)
                print("[+] 리스트 로드 완료", flush=True)

        except Exception as e:
            print(f"[!] 필터 적용 중 오류: {e}", flush=True)

    @staticmethod
    def _extract_view_id(href: str, onclick: str, row_html: str, view_id_param: str) -> Optional[str]:
        """3중 ID 추출: href 파라미터 -> row HTML -> JS 함수 인자"""
        clean_href = href.replace("&amp;", "&") if href else ""
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
            for td in tds:
                title = await td.get_attribute("title")
                if title:
                    info["bi_sj"] = clean_text(title)
                    break
        return info

    @staticmethod
    async def scrape_list_page(page: Page, list_class: str, view_id_param: str = "code") -> List[Dict[str, Any]]:
        """목록 파싱: thead 2차원 그리드 → 필드 매핑"""
        selector = normalize_selector(list_class)
        await page.wait_for_selector(selector, timeout=10000)

        # thead 헤더 그리드 구축 (colspan/rowspan 대응)
        thead_rows = await page.query_selector_all(f"{selector} thead tr")
        col_keys = []
        if thead_rows:
            grid, max_cols = {}, 0
            for ri, tr in enumerate(thead_rows):
                ci = 0
                for th in await tr.query_selector_all("th, td"):
                    while grid.get((ri, ci)): ci += 1
                    text = clean_text(await th.inner_text())
                    cs = int(await th.get_attribute("colspan") or 1)
                    rs = int(await th.get_attribute("rowspan") or 1)
                    for i in range(rs):
                        for j in range(cs):
                            grid[(ri + i, ci + j)] = text
                    ci += cs
                    max_cols = max(max_cols, ci)
            for c in range(max_cols):
                parts = []
                for r in range(len(thead_rows)):
                    v = grid.get((r, c))
                    if v and v not in parts: parts.append(v)
                col_keys.append(get_mapped_key(parts[-1], parts[0] if len(parts) > 1 else None) if parts else f"UNKNOWN_{c}")

        # tbody 행 파싱
        items = []
        for row in await page.query_selector_all(f"{selector} tbody tr, {selector} ul > li, {selector} .list_row"):
            tds = await row.query_selector_all("td") or await row.query_selector_all("div, span")
            if not tds: continue
            item = {}
            if col_keys:
                for i, td in enumerate(tds):
                    if i >= len(col_keys): break
                    key, val = col_keys[i], clean_text(await td.inner_text())
                    if not val: continue
                    if key == "PROPSR": val = ", ".join(v for v in val.split() if v)
                    item[key] = f"{item[key]}, {val}" if key in item and item[key] else val

            link = await UniversalCrawler._get_row_link(row, tds)
            if link["bi_sj"]: item["BI_SJ"] = link["bi_sj"]
            href = link["href"]
            is_real = href and not href.startswith(("javascript", "#"))
            item["link_href"] = href.replace("&amp;", "&") if is_real else (href or link["onclick"])

            vid = UniversalCrawler._extract_view_id(href, link["onclick"], await row.inner_html(), view_id_param)
            if vid: item["view_id"] = vid
            items.append(item)
        return items

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
    async def extract_view_detail(page: Page, view_class: str, base_url: str) -> Dict[str, Any]:
        selector = normalize_selector(view_class)
        await page.wait_for_selector(selector, timeout=10000)

        result = {"sections": {}}
        section, rs_counter = None, 0

        # 셀렉터 확장: tbody tr 뿐만 아니라 ul > li 도 탐색
        rows = await page.query_selector_all(f"{selector} tbody tr, {selector} > li, {selector} .view_row")

        for row in rows:
            try:
                if rs_counter > 0: rs_counter -= 1
                if rs_counter == 0: section = None

                # [수정 포인트 1] 원주시 같은 LI 구조(strong/span)와 기존 Table 구조(th/td) 통합 추출
                ths = await row.query_selector_all("th, dt, strong, .label, .title")
                tds = await row.query_selector_all("td, dd, span, .value, .cont")
                pairs = []

                # [수정 포인트 2] 데이터가 없는 row 스킵 방지 및 매핑
                if not ths and tds:
                    # 텍스트가 있는 첫 번째 요소만 본문으로 취급
                    text_content = clean_text(await tds[0].inner_text())
                    if text_content:
                        html = await tds[0].inner_html()
                        label = "본문내용_첨부파일" if any(k in html.lower() for k in ["down", "첨부", "file"]) else "본문내용"
                        pairs.append((label, tds[0]))
                else:
                    ti, di = 0, 0
                    while ti < len(ths) and di < len(tds):
                        # rowspan 처리 (기존 로직 유지)
                        rs = await ths[ti].get_attribute("rowspan")
                        th_text = clean_text(await ths[ti].inner_text())
                        
                        if rs and int(rs) > 1 and ti == 0:
                            section, rs_counter = th_text, int(rs)
                            ti += 1
                            if ti >= len(ths): break
                            th_text = clean_text(await ths[ti].inner_text())
                        
                        pairs.append((th_text, tds[di]))
                        ti += 1; di += 1

                # 데이터 가공 및 저장 (기존 로직 동일)
                for label, td_el in pairs:
                    val = clean_text(await td_el.inner_text())
                    # 원주시는 의안명 옆에 바로 파일이 있으므로 label에 '의안명'이 포함되어도 체크
                    is_file = any(x in label for x in ["첨부", "파일", "원문", "의안명"]) 
                    is_meeting = "회의록" in label

                    if is_file or is_meeting:
                        names, urls = await UniversalCrawler._extract_attachments(td_el, page, base_url, is_file)
                        # 파일 저장 로직
                        if is_file and names:
                            result["BI_FILE_NM"] = names[0] if len(names) == 1 else names
                            result["BI_FILE_URL"] = urls[0] if len(urls) == 1 else urls
                        # (중략 - 회의록 및 섹션 매핑 로직)
                        if not is_file: # 파일이 아닌 경우에만 일반 매핑 (원주시는 의안명 텍스트도 보존)
                            pass
                    
                    # 일반 텍스트 매핑
                    mapped = get_mapped_key(label, section)
                    if section:
                        sec = "위원회" if "위원회" in section else ("본회의" if "본회의" in section else section)
                        result["sections"].setdefault(sec, {})[mapped] = val
                    else:
                        result[mapped] = val

            except Exception as e:
                print(f"[-] row 파싱 에러: {e}", flush=True)
                continue

        if not result.get("sections"): result.pop("sections", None)
        return result

    @staticmethod
    async def _extract_attachments(td_el, page: Page, base_url: str, is_file: bool) -> tuple:
        """
        첨부파일/회의록 링크 추출 및 파일 다운로드 (범용 엔진)
        특징: 동적 폼 생성(평택시), 직접 다운로드, JS 호출 대응
        """
        os.makedirs(FILE_DOWNLOAD_DIR, exist_ok=True)
        names, urls = [], []
        
        # 평택시처럼 span이나 기타 태그에 onclick이 걸린 경우를 위해 [onclick] 포함 탐색
        elements = await td_el.query_selector_all("a, span[onclick], [style*='cursor: pointer']")
        
        for el in elements:
            raw = clean_text(await el.inner_text())
            title = clean_text(await el.get_attribute("title")) or ""
            
            # 필터링 로직 (유지보수 용이하도록 목록화)
            skip_keywords = ["바로보기", "바로듣기", "미리보기", "뷰어"]
            if is_file and (any(k in raw for k in skip_keywords) or any(k in title for k in skip_keywords)):
                continue
            if not raw: continue

            # 메타데이터 수집
            href = await el.get_attribute("href") or ""
            onclick = await el.get_attribute("onclick") or ""
            is_js = href.startswith(("javascript", "#")) or (onclick and not href)
            url_val = onclick if is_js else (href or onclick)

            if is_file:
                print(f"[*] 다운로드 시도: {raw}", flush=True)
                try:
                    # [유지보수 포인트] target="_blank"는 새 탭을 띄워 이벤트를 분산시키므로 현재 창으로 강제
                    await el.evaluate("node => { if(node.tagName === 'A') node.removeAttribute('target'); }")

                    # [핵심] expect_download는 '클릭'에 의한 결과물로 다운로드 이벤트를 기다림
                    # 평택시처럼 폼을 생성해서 날리는 경우도 Playwright는 이 이벤트를 캐치함
                    async with page.expect_download(timeout=15000) as dl_info:
                        # 클릭 시 자바스크립트 에러 방지를 위해 dispatch_event 또는 click 사용
                        await el.click()

                    download = await dl_info.value
                    
                    # 파일명 결정 로직 (서버 제안 이름 vs 웹 표시 이름)
                    suggested_filename = download.suggested_filename
                    _, ext = os.path.splitext(suggested_filename)
                    
                    # 웹상 이름(raw)에 확장자가 없으면 서버가 준 확장자 붙여줌
                    final_name = raw if re.search(r'\.[a-zA-Z0-9]{2,4}$', raw) else f"{raw}{ext}"
                    final_name = re.sub(r'[\\/*?:"<>|]', "", final_name) # 파일명 정규화
                    
                    save_path = os.path.join(FILE_DOWNLOAD_DIR, final_name)
                    await download.save_as(save_path)
                    
                    print(f"[+] 다운로드 완료: {final_name}", flush=True)
                    raw = final_name
                    url_val = download.url # 실제 다운로드된 최종 URL 저장

                except Exception as e:
                    # 실패 시 로그를 상세히 남기되 프로세스는 유지
                    print(f"[-] 다운로드 건너뜀 ({raw}): {str(e)[:100]}", flush=True)
                    
                    # [보충] 다운로드 실패 시에도 최소한 URL은 절대 경로로 확보 시도
                    if not url_val.startswith("http") and url_val:
                        url_val = urljoin(base_url, url_val)

                    # Context Destroyed 방지용 복구: URL이 변했다면 다시 돌아옴
                    # 평택시 같은 경우 폼 전송 실패 시 빈 페이지가 남을 수 있음
                    if page.url != base_url:
                        try:
                            await page.goto(base_url, wait_until="domcontentloaded", timeout=5000)
                        except:
                            pass

            names.append(raw)
            urls.append(url_val)
            
        return names, urls

    @staticmethod
    async def get_total_pages(page: Page) -> int:
        """마지막 페이지 번호 탐지 (last 버튼 -> 숫자 버튼 최대값)"""
        try:
            btn = await page.query_selector("a.last, a.num_last, a[title*='마지막'], a.btn-last, a.direction.last")
            if btn:
                href = await btn.get_attribute("href") or ""
                m = re.search(PAGE_PARAM_PATTERN, href, re.IGNORECASE)
                if m: return int(m.group(2))
                m = re.search(r'=(\d+)$', href)
                if m: return int(m.group(1))
            mx = 1
            for b in await page.query_selector_all(".paging a, .pagination a, #pagingNav a"):
                t = (await b.inner_text()).strip()
                if t.isdigit(): mx = max(mx, int(t))
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
            # 이렇게 하면 <a>2</a> 뿐만 아니라 <button>2</button> 도 클릭 가능합니다.
            link = page.locator(p_sel).get_by_text(re.compile(f"^{next_page}$"), exact=True).first
            
            if await link.count() > 0:
                print(f"[*] 페이지 번호 클릭: {next_page}p (Selector: {p_sel})", flush=True)
                await link.click()
                await page.wait_for_load_state("domcontentloaded")
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


# --- API 엔드포인트 ---

@app.get("/crawl/billList")
async def api_scrape_list(
    request: Request,
    list_url: str = Query(..., description="의회 리스트 URL"),
    view_url: Optional[str] = Query(None, description="상세 진입 URL"),
    view_id_param: str = Query("uuid", description="상세 식별 파라미터명"),
    rasmbly_numpr: str = Query("", description="대수 (공백=전체)"),
    list_class: str = Query("table.board_list", description="리스트 테이블 셀렉터"),
    view_class: Optional[str] = Query(None, description="상세 테이블 셀렉터"),
    max_pages: str = Query("", description="수집 페이지 수 (공백/0=전체)"),
    paging_selector: str = Query("div%23pagingNav, div%23pagingNew", description="페이징 영역 셀렉터 (ex: div.paging)"),
    next_btn_selector: str = Query("a.num_right, a.next, a[title='다음'], .btn-next", description="다음 버튼 셀렉터 (ex: a.next)"),
    search_form_selector: str = Query("form%23search_form", description="검색 폼 셀렉터"),
    numpr_select_selector: str = Query("select%23th_sch", description="대수 선택 셀렉터"),
    search_btn_selector: str = Query("button.btn.blue, button[type='submit'], #btnSearch, .btn_search", description="검색 버튼 셀렉터"),
    req_id: str = Query("", description="요청 ID (yyyyMMddHHmmssSSS)"),
    type: str = Query("", description="구분 (의안, 회의록 등)"),
    crw_id: str = Query("", description="수집 설정 ID")
):
    app.state.stop_scraping = False
    domain = extract_domain(list_url)
    list_data = []

    async with async_playwright() as p:
        browser, page = await _setup_browser(p)
        try:
            print(f"[*] 리스트 수집 시작: {list_url}", flush=True)
            list_data = await _collect_pages(
                page, list_url, rasmbly_numpr, list_class, view_id_param,
                max_pages, paging_selector, next_btn_selector,
                UniversalCrawler.scrape_list_page, lambda: app.state.stop_scraping,
                search_form_selector, numpr_select_selector, search_btn_selector,
            )
            filepath = save_to_json(list_data, domain, "list_all")
            return {"ok": True, "total_count": len(list_data), "saved_file": filepath, "is_stopped_early": app.state.stop_scraping, "req_id": req_id, "type": type, "crw_id": crw_id, "data": list_data}
        except Exception as e:
            print(f"[!] 리스트 수집 에러: {e}", flush=True)
            filepath = save_to_json(list_data, domain, "list_error_partial")
            return {"ok": False, "error_msg": str(e), "saved_file": filepath, "req_id": req_id, "type": type, "crw_id": crw_id, "data": list_data}
        finally:
            await browser.close()


@app.get("/crawl/bill")
async def api_scrape_view(
    list_url: str = Query(..., description="의회 리스트 URL"),
    view_url: str = Query(..., description="상세 진입 URL"),
    view_id_param: str = Query("uuid", description="상세 식별 파라미터명"),
    rasmbly_numpr: str = Query("", description="대수 (공백=전체)"),
    list_class: str = Query("table.board_list", description="리스트 테이블 셀렉터"),
    view_class: str = Query("table.board_view", description="상세 테이블 셀렉터"),
    max_pages: str = Query("", description="수집 페이지 수 (공백/0=전체)"),
    paging_selector: str = Query("div%23pagingNav, div%23pagingNew", description="페이징 영역 셀렉터 (ex: div.paging)"),
    next_btn_selector: str = Query("a.num_right, a.next, a[title='다음'], .btn-next", description="다음 버튼 셀렉터 (ex: a.next)"),
    search_form_selector: str = Query("form%23search_form", description="검색 폼 셀렉터"),
    numpr_select_selector: str = Query("select%23th_sch", description="대수 선택 셀렉터"),
    search_btn_selector: str = Query("button.btn.blue, button[type='submit'], #btnSearch, .btn_search", description="검색 버튼 셀렉터"),
    req_id: str = Query("", description="요청 ID (yyyyMMddHHmmssSSS)"),
    type: str = Query("", description="구분 (의안, 회의록 등)"),
    crw_id: str = Query("", description="수집 설정 ID")
):
    app.state.stop_scraping = False
    domain = extract_domain(list_url)
    list_data, view_data = [], []

    async with async_playwright() as p:
        browser, page = await _setup_browser(p)
        try:
            # 1단계: 리스트 수집
            print(f"[*] [1단계] 리스트 수집: {list_url}", flush=True)
            list_data = await _collect_pages(
                page, list_url, rasmbly_numpr, list_class, view_id_param,
                max_pages, paging_selector, next_btn_selector,
                UniversalCrawler.extract_list_page, lambda: app.state.stop_scraping,
                search_form_selector, numpr_select_selector, search_btn_selector,
            )

            # 2단계: 상세 뷰 수집
            total = len(list_data)
            print(f"[*] [2단계] 상세 뷰 {total}건 수집 시작", flush=True)

            for idx, item in enumerate(list_data):
                if app.state.stop_scraping:
                    print(f"[!] 중단 요청: {idx}번째에서 중단", flush=True)
                    break

                vid = item.get("view_id")
                if not vid: continue

                href = item.get("link_href", "")
                is_real = href and not href.startswith(("#", "javascript"))
                target_url = urljoin(list_url, href) if is_real else f"{view_url}{'&' if '?' in view_url else '?'}{view_id_param}={vid}"

                print(f"[*] 상세 ({idx+1}/{total}) ID: {vid}", flush=True)
                try:
                    await page.goto(target_url, wait_until="domcontentloaded", timeout=15000)
                    parsed = urlparse(target_url)
                    base = f"{parsed.scheme}://{parsed.netloc}"
                    detail = await UniversalCrawler.extract_view_detail(page, view_class, base)
                    sections = detail.pop("sections", {})
                    view_data.append({"view_id": vid, "view_url": target_url, **detail, "sections": sections})
                except Exception as e:
                    print(f"[!] {vid} 상세 실패: {e}", flush=True)
                    view_data.append({"view_id": vid, "view_url": target_url, "view_error": str(e)})

            filepath = save_to_json(view_data, domain, "view_all")
            return {"ok": True, "domain": domain, "saved_file": filepath, "total_count": len(view_data),
                    "is_stopped_early": app.state.stop_scraping, "req_id": req_id, "type": type, "crw_id": crw_id,"data": view_data}
        except Exception as e:
            filepath = save_to_json(view_data, domain, "view_error_partial")
            return {"ok": False, "error_msg": str(e), "saved_file": filepath,
                    "is_stopped_early": app.state.stop_scraping, "req_id": req_id, "type": type, "crw_id": crw_id,"data": view_data}
        finally:
            await browser.close()


@app.get("/crawl/bill/status")
async def api_status(job_id: str):
    """작업 진행 상태 조회"""
    return JOB_STORE.get(job_id, {"ok": False, "msg": "Job ID 없음"})

@app.get("/crawl/bill/stop")
async def api_stop():
    """크롤링 루프 즉시 중단"""
    app.state.stop_scraping = True
    print("[!] 외부 중단 요청 수신", flush=True)
    return {"ok": True, "message": "Stop requested. Current process will halt and save progress."}