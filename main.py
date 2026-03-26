import asyncio
import json
import os
import re
from datetime import datetime
from typing import List, Dict, Any, Optional
from urllib.parse import urljoin, urlparse
import urllib.request

from fastapi import FastAPI, Query, Request
from playwright.async_api import async_playwright, Page

# 분리된 필드맵 임포트 (이전과 동일)
try:
    from field_maps.field_map import FIELD_MAP, SECTION_FIELD_MAP
except ImportError:
    print("[!] field_maps/field_map.py 파일을 찾을 수 없습니다. 기본값을 사용합니다.")
    FIELD_MAP, SECTION_FIELD_MAP = {}, {}

app = FastAPI(title="Enterprise Council Scraper API")

# 전역 상태 관리 (Stop Flag)
app.state.stop_scraping = False
DOWNLOAD_DIR = "download"

# 백그라운드 작업 상태를 저장할 메모리 DB
JOB_STORE: Dict[str, Dict[str, Any]] = {}

# --- 고급 유틸리티 함수 ---

def clean_text(text: Optional[str]) -> str:
    return re.sub(r'\s+', ' ', text.strip()) if text else ""

def extract_domain(url: str) -> str:
    """URL에서 도메인 이름만 추출 (예: www.guroc.go.kr -> guroc)"""
    try:
        netloc = urlparse(url).netloc.split(':')[0]
        if netloc.startswith("www."):
            netloc = netloc[4:]
        return netloc.split('.')[0]
    except Exception:
        return "unknown"

def save_to_json(data: Any, domain: str, prefix: str) -> str:
    """지정된 포맷으로 JSON 파일 저장 및 경로 반환"""
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    filename = f"{domain}_{prefix}_{timestamp}.json"
    filepath = os.path.join(DOWNLOAD_DIR, filename)
    
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)
    
    print(f"[+] 파일 저장 완료: {filepath}", flush=True)
    return filepath

def get_mapped_key(label: str, section: Optional[str] = None) -> str:
    """라벨(th) 텍스트를 Field Map의 표준 키로 변환"""
    label = clean_text(label)
    
    if section:
        # '본회의', '위원회' 등 핵심 키워드 추출
        sec_key = "위원회" if "위원회" in section else ("본회의" if "본회의" in section else section)
        
        # 1. 섹션 맵에서 먼저 찾기
        if sec_key in SECTION_FIELD_MAP:
            for map_key, map_val in SECTION_FIELD_MAP[sec_key].items():
                if map_key.replace(" ", "") == label.replace(" ", ""):
                    return map_val
                    
    # 2. 일반 맵에서 찾기
    for map_key, map_val in FIELD_MAP.items():
        if map_key.replace(" ", "") == label.replace(" ", ""):
            return map_val
            
    return label # 매핑 실패 시 원본 라벨 반환


# --- 범용 크롤링 코어 엔진 ---

class UniversalCrawler:
    @staticmethod
    async def apply_filter_and_search(page: Page, numpr: str):
        print(f"[*] 필터 적용 시도: 대수={numpr}", flush=True)

        try:
            if len(page.listeners("dialog")) == 0:
                page.on("dialog", lambda dialog: asyncio.create_task(dialog.accept()))
        except: pass

        try:
            # 1. 대수 선택기
            selects = await page.query_selector_all("select")
            changed_count = 0
            
            for select in selects:
                options = await select.query_selector_all("option")
                target_value = None
                target_text = None
                is_assembly = False
                
                for opt in options:
                    val = (await opt.get_attribute("value") or "").strip()
                    text = clean_text(await opt.inner_text())
                    
                    if "대" in text and any(str(i) in text for i in range(1, 20)) and "회" not in text:
                        is_assembly = True
                    if val == numpr or val == f"0{numpr}" or f"제{numpr}대" in text or f"{numpr}대" == text:
                        target_value = val
                        target_text = text
                        
                if is_assembly and target_value is not None:
                    current_val = await select.evaluate("node => node.value")
                    if current_val != target_value:
                        await select.select_option(value=target_value)
                        await select.evaluate("node => { node.dispatchEvent(new Event('change', { bubbles: true })); if (typeof jQuery !== 'undefined') jQuery(node).trigger('change'); }")
                        await page.wait_for_timeout(1000)
                        print(f"[+] 대수 세팅 완료: {target_text} (value={target_value})", flush=True)
                    else:
                        # 💡 [버그 수정 1] 이미 9대로 세팅되어 있다면 에러로 치부하지 않고 정상 인지!
                        print(f"[+] 대수가 이미 '{target_text}'(으)로 설정되어 있습니다. (유지)", flush=True)
                    
                    changed_count += 1 # 무조건 카운트 증가

            if changed_count == 0:
                print(f"[-] 대수({numpr}) 옵션을 찾을 수 없습니다.", flush=True)

            # 2. 💡 [핵심] 가짜 탭 버튼 배제 및 진짜 버튼 탐색
            # 평범한 <a> 태그(상단 메뉴 등)는 제외하고, 진짜 '버튼' 역할을 하는 요소만 긁어모읍니다.
            search_elements = await page.query_selector_all(
                "button, input[type='submit'], input[type='button'], input[type='image'], a[class*='btn'], a[class*='search']"
            )
            clicked = False
            
            for el in search_elements:
                if not await el.is_visible(): continue
                
                tag_name = (await el.evaluate("node => node.tagName")).lower()
                text = clean_text(await el.inner_text()) or clean_text(await el.get_attribute("value")) or clean_text(await el.get_attribute("title")) or clean_text(await el.get_attribute("alt"))
                id_attr = (await el.get_attribute("id") or "").lower()
                
                # "검색/조회" 글자가 있거나, 태그 ID 자체가 'search'인 이미지/버튼
                is_search_btn = text and ("검색" in text or "조회" in text)
                if "search" in id_attr and tag_name in ["button", "input", "a"]: 
                    is_search_btn = True
                
                if is_search_btn:
                    # 💡 [함정 완벽 방어] 의안검색, 회기별검색 등 상단 메뉴 탭에 자주 쓰이는 단어 추가
                    trap_words = ["엑셀", "초기화", "취소", "통합", "메뉴", "상세", "회기", "의안", "별검색", "다운", "연혁"]
                    
                    # 텍스트 안에 함정 단어가 포함되어 있다면 무조건 건너뜀 (단, 버튼 이름이 순수하게 "검색"인 경우는 제외)
                    if any(trap in text for trap in trap_words) and text not in ["검색", "조회"]:
                        continue
                        
                    print(f"[+] 진짜 폼(Form) 검색 버튼 발견! (태그:<{tag_name}>, 식별텍스트:'{text}') -> 클릭 시도", flush=True)
                    
                    try:
                        async with page.expect_navigation(wait_until="domcontentloaded", timeout=5000):
                            await el.evaluate("node => node.click()")
                    except Exception:
                        await page.wait_for_timeout(3000)
                        
                    clicked = True
                    print("[+] 화면 갱신 및 렌더링 완료", flush=True)
                    break

            # 3. 폼 전송
            if not clicked:
                forms = await page.query_selector_all("form")
                for form in forms:
                    action = await form.get_attribute("action") or ""
                    if any(k in action.lower() for k in ["search", "list", "bill", "minutes"]):
                        print("[+] 검색 버튼 탐색 실패. Form Submit 강제 전송 시도", flush=True)
                        try:
                            async with page.expect_navigation(wait_until="domcontentloaded", timeout=5000):
                                await form.evaluate("node => { if(typeof node.onsubmit === 'function'){ node.onsubmit(); } else { node.dispatchEvent(new Event('submit', {cancelable: true, bubbles: true})); } }")
                        except:
                            await page.wait_for_timeout(3000)
                        break

            await page.wait_for_load_state("networkidle", timeout=3000)
            
        except Exception as e:
            print(f"[!] 필터 적용 중 예외 발생: {e}", flush=True)

    @staticmethod
    async def scrape_list_page(page: Page, list_class: str, view_id_param: str = "code") -> List[Dict[str, Any]]:
        # 💡 [범용 셀렉터 엔진] table, div, ul 완벽 지원
        selector = list_class.strip()
        if not selector.startswith(".") and not selector.startswith("table") and not selector.startswith("div") and not selector.startswith("ul"):
            selector = f"table.{selector}"
            
        await page.wait_for_selector(selector, timeout=10000)
        
        # 💡 [핵심 기술 1] 표의 헤더(thead)를 분석하여 2차원 매트릭스 그리드를 생성합니다.
        thead_rows = await page.query_selector_all(f"{selector} thead tr")
        col_keys = []
        
        if thead_rows:
            header_grid = {}
            max_cols = 0
            for r_idx, tr in enumerate(thead_rows):
                ths = await tr.query_selector_all("th, td")
                c_idx = 0
                for th in ths:
                    while header_grid.get((r_idx, c_idx)) is not None:
                        c_idx += 1
                    text = clean_text(await th.inner_text())
                    colspan = int(await th.get_attribute("colspan") or 1)
                    rowspan = int(await th.get_attribute("rowspan") or 1)
                    for i in range(rowspan):
                        for j in range(colspan):
                            header_grid[(r_idx + i, c_idx + j)] = text
                    c_idx += colspan
                    max_cols = max(max_cols, c_idx)
                    
            for c in range(max_cols):
                col_texts = []
                for r in range(len(thead_rows)):
                    val = header_grid.get((r, c))
                    if val and val not in col_texts:
                        col_texts.append(val)
                if not col_texts:
                    col_keys.append(f"UNKNOWN_{c}")
                    continue
                
                section = col_texts[0] if len(col_texts) > 1 else None
                label = col_texts[-1]
                # 범용 키 매핑 적용
                col_keys.append(get_mapped_key(label, section))

        # 💡 [핵심 기술 3] 파악된 헤더 구조를 바탕으로 본문(tbody 또는 li) 데이터를 결합
        rows = await page.query_selector_all(f"{selector} tbody tr, {selector} ul > li, {selector} .list_row")
        items = []
        
        for row in rows:
            # table이면 td를 찾고, div/ul 리스트면 내부 div나 span을 셀(cell)로 간주
            tds = await row.query_selector_all("td")
            if not tds:
                tds = await row.query_selector_all("div, span")
                
            if not tds: continue
            
            item = {}
            if col_keys:
                for i, td in enumerate(tds):
                    if i >= len(col_keys): break
                    key = col_keys[i]
                    val = clean_text(await td.inner_text())
                    
                    if not val: continue
                    if key == "PROPSR":
                        val = ", ".join(v for v in val.split() if v)
                        
                    if key in item and item[key]:
                        item[key] = f"{item[key]}, {val}"
                    else:
                        item[key] = val
                    
            # 💡 [초강력 3중망 ID 추출기]
            a_tag = await row.query_selector("a")
            tr_onclick = await row.get_attribute("onclick") or ""
            
            a_href = ""
            a_onclick = ""
            
            if a_tag:
                a_text = clean_text(await a_tag.inner_text())
                if a_text:
                    item["BI_SJ"] = a_text 
                a_href = await a_tag.get_attribute("href") or ""
                a_onclick = await a_tag.get_attribute("onclick") or ""
            else:
                for td in tds:
                    title_attr = await td.get_attribute("title")
                    if title_attr:
                        item["BI_SJ"] = clean_text(title_attr)
                        break

            view_id = None
            
            # 1망: 원본 href 
            if a_href and not a_href.startswith("javascript") and not a_href.startswith("#"):
                clean_href = a_href.replace("&amp;", "&")
                item["link_href"] = clean_href
                match = re.search(rf"[?&]{view_id_param}=([^&]+)", clean_href)
                if match: 
                    view_id = match.group(1)
                else:
                    auto_match = re.search(r"[?&](uid|idx|code|no|seq|id|bill_no|billNo|idx_no|nttId|uuid)=([^&]+)", clean_href, re.IGNORECASE)
                    if auto_match:
                        view_id = auto_match.group(2)
            else:
                item["link_href"] = a_href if a_href else (a_onclick or tr_onclick)
                
            # 2망: Row 전체 HTML
            if not view_id:
                row_html = await row.inner_html()
                match = re.search(rf"[?&]?{view_id_param}=([^&\"'>\s]+)", row_html)
                if match: view_id = match.group(1)
                
            # 3망: JS 함수 인자값 강제 추출
            if not view_id:
                js_code = a_onclick if a_onclick else tr_onclick
                if not js_code and a_href.startswith("javascript"):
                    js_code = a_href
                match = re.search(r"\(['\"]?([^'\"),]+)['\"]?\)", js_code)
                if match: view_id = match.group(1)

            if view_id:
                item["view_id"] = view_id
            
            items.append(item)
            
        return items

    @staticmethod
    async def extract_list_page(page: Page, list_class: str, view_id_param: str = "code") -> List[Dict[str, Any]]:
        # 💡 [범용 셀렉터 엔진]
        selector = list_class.strip()
        if not selector.startswith(".") and not selector.startswith("table") and not selector.startswith("div") and not selector.startswith("ul"):
            selector = f"table.{selector}"
            
        await page.wait_for_selector(selector, timeout=10000)
        
        rows = await page.query_selector_all(f"{selector} tbody tr, {selector} ul > li, {selector} .list_row")
        items = []
        for row in rows:
            tds = await row.query_selector_all("td")
            if not tds:
                tds = await row.query_selector_all("div, span")
            if not tds: continue
            
            item = {}
            item["row_texts"] = [clean_text(await td.inner_text()) for td in tds]
            
            view_id = None
            link_href = ""
            onclick_text = ""
            
            tr_onclick = await row.get_attribute("onclick") or ""
            if tr_onclick:
                onclick_text = tr_onclick

            a_tag = await row.query_selector("a")
            if a_tag:
                a_text = clean_text(await a_tag.inner_text())
                if a_text:
                    item["BI_SJ"] = a_text 
                    
                a_href = await a_tag.get_attribute("href") or ""
                link_href = a_href
                
                a_onclick = await a_tag.get_attribute("onclick") or ""
                if a_onclick:
                    onclick_text = a_onclick
                
                if a_href and not a_href.startswith("javascript") and not a_href.startswith("#"):
                    clean_href = a_href.replace("&amp;", "&")
                    match = re.search(rf"[?&]{view_id_param}=([^&]+)", clean_href)
                    if match: 
                        view_id = match.group(1)
                    else:
                        auto_match = re.search(r"[?&](uid|idx|code|no|seq|id|bill_no|billNo|idx_no|nttId)=([^&]+)", clean_href, re.IGNORECASE)
                        if auto_match:
                            view_id = auto_match.group(2)
            else:
                for td in tds:
                    title_attr = await td.get_attribute("title")
                    if title_attr:
                        item["BI_SJ"] = clean_text(title_attr)
                        break

            item["link_href"] = link_href
            
            if not view_id:
                if onclick_text:
                    match_js = re.search(r"['\"]([^'\"]+)['\"]", onclick_text)
                    if match_js:
                        view_id = match_js.group(1)
                
                if not view_id:
                    row_html = await row.inner_html()
                    match_param = re.search(rf"[?&]?{view_id_param}=([^&\"'>\s]+)", row_html)
                    match_js_html = re.search(r"onclick\s*=\s*[\"'][a-zA-Z0-9_]+\([\"']([^\"']+)[\"']\)", row_html)
                    
                    if match_param:
                        view_id = match_param.group(1)
                    elif match_js_html:
                        view_id = match_js_html.group(1)

            if view_id:
                item["view_id"] = view_id
                
            items.append(item)
        return items
    
    @staticmethod
    async def extract_view_detail(page: Page, view_class: str, base_url: str) -> Dict[str, Any]:
        # 💡 [범용 셀렉터 엔진]
        selector = view_class.strip()
        if not selector.startswith(".") and not selector.startswith("table") and not selector.startswith("div") and not selector.startswith("ul"):
            selector = f"table.{selector}"
            
        await page.wait_for_selector(selector, timeout=10000)
        
        rows = await page.query_selector_all(f"{selector} tbody tr, {selector} ul > li, {selector} .view_row")
        
        result = {"sections": {}}
        current_section = None
        rowspan_counter = 0

        for row in rows:
            if rowspan_counter > 0:
                rowspan_counter -= 1
            if rowspan_counter == 0:
                current_section = None

            ths = await row.query_selector_all("th, dt, .label, .title")
            tds = await row.query_selector_all("td, dd, .value, .cont")
            
            pairs = []
            
            if len(ths) == 0 and len(tds) > 0:
                td_el = tds[0]
                inner_html = await td_el.inner_html()
                
                if "down" in inner_html.lower() or "첨부" in inner_html or "file" in inner_html.lower():
                    fake_label = "본문내용_첨부파일"
                else:
                    fake_label = "본문내용"
                pairs.append((fake_label, td_el))
            else:
                th_idx = 0
                td_idx = 0
                while th_idx < len(ths) and td_idx < len(tds):
                    th_el = ths[th_idx]
                    rowspan = await th_el.get_attribute("rowspan")
                    th_text = clean_text(await th_el.inner_text())
                    
                    if rowspan and int(rowspan) > 1 and th_idx == 0:
                        current_section = th_text
                        rowspan_counter = int(rowspan)
                        th_idx += 1 
                        if th_idx >= len(ths): break
                        th_el = ths[th_idx]
                        th_text = clean_text(await th_el.inner_text())

                    pairs.append((th_text, tds[td_idx]))
                    th_idx += 1
                    td_idx += 1

            for label, td_el in pairs:
                val = clean_text(await td_el.inner_text())
                
                is_file = any(x in label for x in ["첨부", "파일", "원문"])
                is_meeting = "회의록" in label
                
                if is_file or is_meeting:
                    clickables = await td_el.query_selector_all("a, [onclick]")
                    names = []
                    urls = []
                    
                    download_dir = r"C:\lapc_download"
                    os.makedirs(download_dir, exist_ok=True)
                    
                    for el in clickables:
                        raw_name = clean_text(await el.inner_text())
                        title_attr = clean_text(await el.get_attribute("title")) or ""
                        
                        if is_file and (raw_name in ["바로보기", "바로듣기", "미리보기", "뷰어"] or title_attr in ["바로보기", "바로듣기"]):
                            continue
                                
                        if not raw_name: continue
                            
                        href = await el.get_attribute("href") or ""
                        onclick = await el.get_attribute("onclick") or ""
                        
                        is_js_link = href.startswith("javascript") or href.startswith("#")
                        url_val = onclick if (is_js_link and onclick) else (href if href else onclick)
                        if not is_js_link and url_val and not url_val.startswith("http"):
                            url_val = urljoin(base_url, url_val)
                            
                        if is_file:
                            print(f"[*] 동적 파일 다운로드 시도: {raw_name}", flush=True)
                            try:
                                await el.evaluate("node => node.removeAttribute('target')")
                                async with page.expect_download(timeout=10000) as download_info:
                                    await el.click()
                                
                                download = await download_info.value
                                
                                # 💡 [하이브리드 네이밍 시스템]
                                # 1. 서버가 보내준 진짜 파일명 (예: 09GC0F01.HWP)
                                real_server_name = download.suggested_filename
                                
                                # 2. 확장자 추출 (예: .HWP)
                                _, ext = os.path.splitext(real_server_name)
                                
                                # 3. 게시판에 적힌 예쁜 이름(raw_name)에 확장자가 없다면 붙여줍니다.
                                # (구로구처럼 이름은 "조례안"인데 파일이 "09GC.HWP"인 경우 완벽 대응)
                                if not re.search(r'\.[a-zA-Z0-9]{2,4}$', raw_name):
                                    final_file_name = f"{raw_name}{ext}"
                                else:
                                    final_file_name = raw_name
                                    
                                # 파일명에 쓸 수 없는 특수문자 제거
                                final_file_name = re.sub(r'[\\/*?:"<>|]', "", final_file_name)
                                    
                                save_path = os.path.join(download_dir, final_file_name)
                                
                                await download.save_as(save_path)
                                print(f"[+] 다운로드 성공: {save_path} (서버 원본명: {real_server_name})", flush=True)
                                
                                raw_name = final_file_name
                                url_val = f"Downloaded (Trigger: {url_val})"
                            except Exception as e:
                                print(f"[-] 동적 다운로드 실패 ({raw_name}): {e}", flush=True)
                                if page.url != base_url and not is_js_link:
                                    await page.go_back(wait_until="domcontentloaded")
                                
                        names.append(raw_name)
                        urls.append(url_val)

                    if is_file and names:
                        result["BI_FILE_NM"] = names[0] if len(names) == 1 else names
                        result["BI_FILE_URL"] = urls[0] if len(urls) == 1 else urls
                    if is_meeting and names:
                        target = result["sections"].setdefault(current_section, {}) if current_section else result
                        target["RELATED_MEETING_NM"] = names[0] if len(names) == 1 else names
                        target["RELATED_MEETING_URL"] = urls[0] if len(urls) == 1 else urls
                else:
                    mapped_key = get_mapped_key(label, current_section)
                    if mapped_key == "PROPSR" and val:
                        val = ", ".join(v for v in val.split() if v)

                    if current_section:
                        sec_name = "위원회" if "위원회" in current_section else ("본회의" if "본회의" in current_section else current_section)
                        if sec_name not in result["sections"]:
                            result["sections"][sec_name] = {}
                        result["sections"][sec_name][mapped_key] = val
                    else:
                        result[mapped_key] = val

        if not result.get("sections"):
            result.pop("sections", None)

        return result

    @staticmethod
    async def get_total_pages(page: Page) -> int:
        try:
            # 💡 [방어막 1] 대한민국 모든 관공서의 '마지막 페이지' 버튼 클래스 총망라
            last_btn = await page.query_selector("a.last, a.num_last, a[title*='마지막'], a.btn-last, a.direction.last")
            if last_btn:
                href = await last_btn.get_attribute("href")
                if href:
                    # 💡 [핵심] pageNum, page_id 등 변칙 파라미터 완벽 대응
                    match = re.search(r'[?&](?:page|pageIndex|p|page_no|pageno|cPage|pageNum|page_id)=(\d+)', href, re.IGNORECASE)
                    if match:
                        return int(match.group(1))
                    
                    # 만약 파라미터 이름조차 이상하다면? 제일 끝에 있는 숫자를 강제로 뜯어옴
                    match_end = re.search(r'=(\d+)$', href)
                    if match_end:
                        return int(match_end.group(1))

            # 💡 [방어막 2] '마지막' 버튼이 아예 없는 사이트라면? 눈에 보이는 숫자 버튼 중 가장 큰 값을 찾음
            num_btns = await page.query_selector_all(".paging a, .pagination a, #pagingNav a")
            max_num = 1
            for btn in num_btns:
                text = await btn.inner_text()
                if text.strip().isdigit():
                    max_num = max(max_num, int(text.strip()))
            return max_num
            
        except Exception as e:
            print(f"[-] 총 페이지 수 계산 실패, 기본값(1) 적용: {e}", flush=True)
            return 1

    @staticmethod
    async def go_to_page(page: Page, next_page: int, paging_sel: str, next_btn_sel: str) -> bool:
        """지능형 다음 페이지 이동 (완벽 분기 처리)"""
        try:
            # 1. 전자정부 프레임워크 (JS 방식)
            if await page.evaluate("typeof fn_egov_link_page === 'function'"):
                await page.evaluate(f"fn_egov_link_page({next_page});")
                await page.wait_for_load_state("domcontentloaded")
                return True
        except: pass
        
        try:
            # 2. 직접 <a> 태그 클릭 방식 (외부 주입 셀렉터)
            link = page.locator(f"{paging_sel} a").filter(has_text=re.compile(f"^{next_page}$")).first
            
            # 💡 [핵심 해결] 번호가 있으면 누르고, 없으면 '다음 10페이지' 버튼을 누른다!
            if await link.count() > 0:
                await link.click()
                await page.wait_for_load_state("domcontentloaded")
                return True
            else:
                next_block = page.locator(next_btn_sel).first
                if await next_block.count() > 0:
                    print(f"[*] 다음 페이지 블록(> 버튼) 클릭 시도", flush=True)
                    await next_block.click()
                    await page.wait_for_load_state("domcontentloaded")
                    
                    # 💡 페이지가 완전히 전환(Reload)될 수 있도록 짧은 안전 대기
                    await page.wait_for_timeout(100)
                    return True
        except Exception as e:
            print(f"[!] 페이지 이동 에러: {e}", flush=True)
            
        return False

# --- REST API Endpoints ---

@app.get("/crawl/billList")
async def api_scrape_list(
    request: Request,
    list_url: str = Query(..., description="의회 리스트 URL"),
    view_url: Optional[str] = Query(None, description="상세 진입 URL (예: .../billview.do)"),
    view_id_param: str = Query("uuid", description="상세 페이지 식별 파라미터명 (예: code, idx, no, uuid 등)"),
    rasmbly_numpr: str = Query("", description="대수 (예: 9, 공백 시 필터 없이 전체/기본 검색)"),
    list_class: str = Query("table.board_list", description="리스트 테이블 클래스"),
    view_class: Optional[str] = Query(None,  description="상세 테이블 클래스"),
    max_pages: str = Query("", description="공백 또는 0이면 전체 페이지 자동 수집, 숫자면 해당 페이지까지만"),
    paging_selector: str = Query(".pagination, .paging, #pagingNav", description="페이징 영역 클래스"),
    next_btn_selector: str = Query(".num_right, .next, [title='다음'], .btn-next", description="다음 페이지 블록 버튼 클래스")
):
    app.state.stop_scraping = False 
    domain = extract_domain(list_url)
    list_data = []
    safe_max_pages = int(max_pages.strip()) if max_pages and max_pages.strip().isdigit() else 0

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        
        # 쓸데없는 리소스 차단하여 광속 수집
        await page.route("**/*", lambda route: route.abort() 
            if route.request.resource_type in ["image", "stylesheet", "media", "font"] 
            else route.continue_()
        )

        try:
            print(f"[*] 리스트 페이지 진입: {list_url}", flush=True)
            await page.goto(list_url, wait_until="domcontentloaded", timeout=30000)
            
            # 💡 [핵심 2] 파라미터가 비어있지 않을 때만 필터 조작, 비어있으면 기본 목록 쿨하게 수집
            if rasmbly_numpr and rasmbly_numpr.strip():
                await UniversalCrawler.apply_filter_and_search(page, rasmbly_numpr.strip())
            else:
                print("[*] 대수(rasmbly_numpr) 파라미터가 비어있습니다. 필터 조작 없이 사이트 기본 목록을 수집합니다.", flush=True)
            
            total_pages = await UniversalCrawler.get_total_pages(page)
            target_pages = total_pages if safe_max_pages == 0 else min(safe_max_pages, total_pages)
            
            for current_page in range(1, target_pages + 1):
                if app.state.stop_scraping:
                    print("[!] 중단 요청 감지: 리스트 수집을 즉시 멈춥니다.", flush=True)
                    break

                print(f"[*] 리스트 수집 중: {current_page}/{target_pages} 페이지", flush=True)
                
                try:
                    # 💡 스네이크 케이스 파라미터 및 view_id_param 완벽 매핑
                    items = await UniversalCrawler.scrape_list_page(page, list_class, view_id_param)
                    list_data.extend(items)
                except Exception as e:
                    print(f"[!] {current_page}페이지 목록 추출 실패 (건너뜀): {e}", flush=True)

                if current_page < target_pages:
                    # 💡 매개변수 4개 (page, 다음페이지, 페이징셀렉터, 버튼셀렉터) 정확히 매핑
                    moved = await UniversalCrawler.go_to_page(page, current_page + 1, paging_selector, next_btn_selector)
                    
                    if not moved:
                        print(f"[!] {current_page + 1}페이지 UI 이동 실패. 강제 URL 점프를 시도합니다.", flush=True)
                        current_url = page.url
                        fallback_url = re.sub(
                            r'([?&](?:page|pageIndex|p|page_no|pageno|cPage))=(\d+)', 
                            rf'\g<1>={current_page + 1}', 
                            current_url, 
                            flags=re.IGNORECASE
                        )
                        if fallback_url != current_url:
                            try:
                                await page.goto(fallback_url, wait_until="domcontentloaded", timeout=30000)
                            except Exception as fallback_err:
                                print(f"[!] 강제 진입 실패: {fallback_err}", flush=True)

            filepath = save_to_json(list_data, domain, "list_all")
            return {
                "ok": True, 
                "total_count": len(list_data), 
                "is_stopped_early": app.state.stop_scraping, 
                "data": list_data, 
                "saved_file": filepath
            }
            
        except Exception as e: 
            print(f"[!] 리스트 수집 중 치명적 에러 발생: {e}", flush=True)
            filepath = save_to_json(list_data, domain, "list_error_partial")
            return {"ok": False, "error_msg": str(e), "data": list_data, "saved_file": filepath}
        finally: 
            await browser.close()


@app.get("/crawl/bill")
async def api_scrape_view(
    list_url: str = Query(..., description="의회 리스트 URL"),
    view_url: str = Query(..., description="상세 진입 URL (예: .../billview.do)"),
    view_id_param: str = Query("uuid", description="상세 페이지 식별 파라미터명 (예: code, idx, no, uuid 등)"),
    rasmbly_numpr: str = Query("", description="대수 (예: 9, 공백 시 필터 없이 전체/기본 검색)"),
    list_class: str = Query("table.board_list", description="리스트 테이블 클래스"),
    view_class: str = Query("table.board_view", description="상세 테이블 클래스"),
    max_pages: str = Query("", description="공백 또는 0이면 전체 페이지 자동 수집, 숫자면 해당 페이지까지만"),
    paging_selector: str = Query(".pagination, .paging, #pagingNav", description="페이징 영역 클래스"),
    next_btn_selector: str = Query(".num_right, .next, [title='다음'], .btn-next", description="다음 페이지 블록 버튼 클래스")
):
    app.state.stop_scraping = False
    domain = extract_domain(list_url)
    
    list_data = []
    view_data_list = []

    safe_max_pages = int(max_pages.strip()) if max_pages and max_pages.strip().isdigit() else 0

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        
        await page.route("**/*", lambda route: route.abort() 
            if route.request.resource_type in ["image", "stylesheet", "media", "font"] 
            else route.continue_()
        )
        
        try:
            # 1. 리스트 수집
            print(f"[*] [통합수집 1단계] 리스트 페이지 진입: {list_url}", flush=True)
            await page.goto(list_url, wait_until="domcontentloaded", timeout=30000)
            
            # 💡 [핵심 2] 파라미터가 정상적으로 들어왔을 때만 필터를 세팅하고, 공백이면 쿨하게 패스!
            if rasmbly_numpr and rasmbly_numpr.strip():
                await UniversalCrawler.apply_filter_and_search(page, rasmbly_numpr.strip())
            else:
                print("[*] 대수(rasmbly_numpr) 파라미터가 비어있습니다. 필터 조작 없이 사이트 기본 목록을 수집합니다.", flush=True)
            
            total_pages = await UniversalCrawler.get_total_pages(page)
            target_pages = total_pages if safe_max_pages == 0 else min(safe_max_pages, total_pages)
            
            for current_page in range(1, target_pages + 1):
                if app.state.stop_scraping:
                    print("[!] 중단 요청 감지: 리스트 수집을 즉시 멈춥니다.", flush=True)
                    break

                print(f"[*] 리스트 수집 중: {current_page}/{target_pages} 페이지", flush=True)
                
                # 💡 [방어 로직 1] 추출 실패 시 시스템을 멈추지 않고 건너뜀 (Skip & Continue)
                try:
                    items = await UniversalCrawler.extract_list_page(page, list_class, view_id_param)
                    list_data.extend(items)
                except Exception as e:
                    print(f"[!] {current_page}페이지 목록 추출 실패 (서버 에러 페이지로 추정). 데이터를 건너뜁니다.", flush=True)

                # 다음 페이지 이동
                if current_page < target_pages:
                    moved = await UniversalCrawler.go_to_page(page, current_page + 1, paging_selector, next_btn_selector)
                    
                    # 💡 [방어 로직 2] break(중단)를 없애고, 실패 시 URL 강제 조작으로 다음 페이지 점프 시도!
                    if not moved:
                        print(f"[!] {current_page + 1}페이지로 UI 이동 실패. URL을 직접 조작하여 강제 진입(Jump)을 시도합니다.", flush=True)
                        current_url = page.url
                        
                        # 정규식을 이용해 어떤 형태의 파라미터(page, pageIndex, p 등)든 다음 번호로 강제 치환
                        fallback_url = re.sub(
                            r'([?&](?:page|pageIndex|p|page_no|pageno|cPage))=(\d+)', 
                            rf'\g<1>={current_page + 1}', 
                            current_url, 
                            flags=re.IGNORECASE
                        )
                        
                        if fallback_url != current_url:
                            try:
                                await page.goto(fallback_url, wait_until="domcontentloaded", timeout=30000)
                            except Exception as fallback_err:
                                print(f"[!] 강제 진입마저 실패했습니다 (서버 완전 다운 의심): {fallback_err}", flush=True)
                        
                        # break 대신 continue 처럼 다음 루프(196페이지)를 돌게 놔둠!

            # 2. 뷰 수집
            total_items = len(list_data)
            print(f"[*] [통합수집 2단계] 총 {total_items}건의 상세 뷰 수집을 시작합니다.", flush=True)

            for idx, item in enumerate(list_data):
                if app.state.stop_scraping:
                    print(f"[!] 중단 요청 감지: {idx}번째 상세 수집 중 즉시 멈춥니다.", flush=True)
                    break

                view_id = item.get("view_id")
                link_href = item.get("link_href")

                # 아이디가 없으면 무조건 패스
                if not view_id: continue

                # 💡 [만능 라우터] "#"이나 "javascript"로 시작하는지 철저히 검증
                is_real_link = link_href and not link_href.startswith("#") and not link_href.startswith("javascript")
                
                if is_real_link:
                    # [Case A: 금천구] 진짜 GET 링크가 있으면 원본 URL에 도메인을 붙여서 사용
                    target_url = urljoin(list_url, link_href)
                else:
                    # [Case B: 구로구] 파라미터 조립 (view_url + ? + code=2812)
                    separator = "&" if "?" in view_url else "?"
                    target_url = f"{view_url}{separator}{view_id_param}={view_id}"
                
                print(f"[*] 상세 수집 중 ({idx+1}/{total_items}) ... ID: {view_id}", flush=True)
                # (이하 try-except 상세 페이지 접속 코드는 기존과 동일)
                
                try:
                    await page.goto(target_url, wait_until="domcontentloaded", timeout=15000)
                    parsed = urlparse(target_url)
                    base_url = f"{parsed.scheme}://{parsed.netloc}"
                    
                    view_detail = await UniversalCrawler.extract_view_detail(page, view_class, base_url)
                    sections_data = view_detail.pop("sections", {})
                    
                    view_item = {
                        "view_id": view_id,
                        "view_url": target_url,
                        **view_detail,
                        "sections": sections_data
                    }
                    view_data_list.append(view_item)
                    
                except Exception as e:
                    print(f"[!] {view_id} 상세 수집 실패: {e}", flush=True)
                    view_data_list.append({
                        "view_id": view_id,
                        "view_url": target_url,
                        "view_error": str(e)
                    })

            filepath = save_to_json(view_data_list, domain, "view_all")
            
            return {
                "ok": True,
                "domain": domain,
                "saved_file": filepath,
                "total_count": len(view_data_list),
                "is_stopped_early": app.state.stop_scraping,
                "data": view_data_list
            }

        except Exception as e:
            filepath = save_to_json(view_data_list, domain, "view_error_partial")
            return {
                "ok": False, 
                "error_msg": str(e), 
                "saved_file": filepath, 
                "is_stopped_early": app.state.stop_scraping,
                "data": view_data_list
            }
        finally:
            await browser.close()

@app.get("/status")
async def api_status(job_id: str):
    """현재 작업이 몇 퍼센트 진행되었는지 확인합니다."""
    return JOB_STORE.get(job_id, {"ok": False, "msg": "Job ID를 찾을 수 없습니다."})

@app.get("/stop")
async def api_stop():
    """크롤링 루프를 즉시 중단시키는 엔드포인트"""
    app.state.stop_scraping = True
    print("[!] 외부에서 중단(/stop) 요청이 들어왔습니다.", flush=True)
    return {"ok": True, "message": "Scraping stop requested. The current process will halt and save its progress."}

if __name__ == "__main__":
    import uvicorn
    # Java CMS와 통신할 포트 설정
    uvicorn.run(app, host="0.0.0.0", port=8900)