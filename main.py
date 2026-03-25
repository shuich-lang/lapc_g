import asyncio
import json
import os
import re
from datetime import datetime
from typing import List, Dict, Any, Optional
from urllib.parse import urljoin, urlparse

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
            selects = await page.query_selector_all("select")
            for select in selects:
                options = await select.query_selector_all("option")
                for opt in options:
                    val = (await opt.get_attribute("value") or "").strip()
                    text = clean_text(await opt.inner_text())
                    
                    # [핵심 방어 로직] "9"가 "339"에 포함되는 오작동 방지
                    is_exact_val = (val == numpr or val == f"0{numpr}")
                    is_exact_text = (f"제{numpr}대" in text)
                    
                    if is_exact_val or is_exact_text:
                        # "회" (예: 제339회) 라는 글자가 포함되어 있으면 회기(Session)이므로 건너뜀
                        if "회" in text and "대" not in text:
                            continue
                            
                        await select.select_option(value=val)
                        print(f"[+] 대수 선택 완료: {text} (value={val})", flush=True)
                        break # 현재 select 박스 처리가 끝났으므로 다음 select 박스로 넘어감
            
            # 검색 버튼 클릭 로직
            search_btns = await page.query_selector_all("button, input[type='submit'], a.btn")
            for btn in search_btns:
                text = clean_text(await btn.inner_text()) or clean_text(await btn.get_attribute("value"))
                if text and ("검색" in text or "조회" in text):
                    await btn.click()
                    await page.wait_for_load_state("domcontentloaded")
                    print("[+] 검색 버튼 클릭 완료", flush=True)
                    break
        except Exception as e:
            print(f"[!] 필터 적용 중 예외 발생 (진행 유지): {e}", flush=True)

    @staticmethod
    async def scrape_list_page(page: Page, list_class: str, view_id_param: str = "code") -> List[Dict[str, Any]]:
        selector = f"table.{list_class}"
        await page.wait_for_selector(selector, timeout=10000)
        
        # 💡 [핵심 기술 1] 표의 헤더(thead)를 분석하여 2차원 매트릭스 그리드를 생성합니다.
        # (금천구처럼 rowspan, colspan이 복잡하게 얽힌 다중 헤더를 완벽히 평탄화)
        thead_rows = await page.query_selector_all(f"{selector} thead tr")
        header_grid = {}
        max_cols = 0
        
        for r_idx, tr in enumerate(thead_rows):
            ths = await tr.query_selector_all("th, td")
            c_idx = 0
            for th in ths:
                # 이미 병합(span)으로 채워진 칸은 건너뜀
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
                
        # 💡 [핵심 기술 2] 각 열(Column)별로 최종 필드명(BI_NO, PROPSR 등)을 동적 매핑합니다.
        col_keys = []
        for c in range(max_cols):
            col_texts = []
            for r in range(len(thead_rows)):
                val = header_grid.get((r, c))
                if val and val not in col_texts:
                    col_texts.append(val)
                    
            if not col_texts:
                col_keys.append(f"UNKNOWN_{c}")
                continue
                
            # 부모 카테고리(section)와 실제 라벨(label) 분리 (예: section="본회의", label="처리결과")
            section = col_texts[0] if len(col_texts) > 1 else None
            label = col_texts[-1]
            
            # 뷰 수집에서 쓰던 get_mapped_key를 리스트 수집에서도 동일하게 재활용!
            mapped_key = get_mapped_key(label, section)
            col_keys.append(mapped_key)

        # 💡 [핵심 기술 3] 파악된 헤더 구조(col_keys)를 바탕으로 본문(tbody) 데이터를 결합합니다.
        rows = await page.query_selector_all(f"{selector} tbody tr")
        items = []
        for row in rows:
            tds = await row.query_selector_all("td")
            if not tds: continue
            
            item = {}
            for i, td in enumerate(tds):
                if i >= len(col_keys): break
                key = col_keys[i]
                val = clean_text(await td.inner_text())
                
                if not val: continue
                
                # 콤마(,) 정규화: 스페이스바로 띄어진 이름들을 콤마로 예쁘게 연결
                if key == "PROPSR":
                    val = ", ".join(v for v in val.split() if v)
                
                # 중복 키 결합 (예: 금천구의 '대표발의자'와 '공동발의자'가 모두 PROPSR로 매핑되었을 때 병합)
                if key in item and item[key]:
                    item[key] = f"{item[key]}, {val}"
                else:
                    item[key] = val
                    
            # a 태그에서 링크, 아이디, 최종 제목(BI_SJ) 추출 (불변의 규칙)
            a_tag = await row.query_selector("a")
            if a_tag:
                a_text = clean_text(await a_tag.inner_text())
                if a_text:
                    item["BI_SJ"] = a_text # a 태그 안의 텍스트가 가장 정확한 안건 제목
                    
                onclick = await a_tag.get_attribute("onclick") or ""
                href = await a_tag.get_attribute("href") or ""
                
                if href and not href.startswith("javascript") and href != "#":
                    item["link_href"] = href
                    match = re.search(rf"[?&]{view_id_param}=([^&]+)", href)
                    if match: item["view_id"] = match.group(1)
                else:
                    item["link_href"] = href if href else onclick
                    js_code = onclick if onclick else href
                    match = re.search(r"\(['\"]?([^'\"),]+)['\"]?\)", js_code)
                    if match: item["view_id"] = match.group(1)
            
            items.append(item)
            
        return items

    @staticmethod
    async def extract_list_page(page: Page, list_class: str, view_id_param: str = "code") -> List[Dict[str, Any]]:
        selector = f"table.{list_class}"
        await page.wait_for_selector(selector, timeout=10000)
        rows = await page.query_selector_all(f"{selector} tbody tr")
        items = []
        for row in rows:
            tds = await row.query_selector_all("td")
            if not tds: continue
            
            item = {}
            # 💡 [방어 로직 1] 각 의회마다 컬럼 개수와 순서가 다르므로, 원본 텍스트 배열을 통째로 보존합니다.
            item["row_texts"] = [clean_text(await td.inner_text()) for td in tds]
            
            # 💡 [핵심 최적화] tds[1] 같은 하드코딩 제거! 몇 번째 칸이든 상관없이 해당 줄(tr)에서 첫 번째 <a> 태그를 색출!
            a_tag = await row.query_selector("a")
            if a_tag:
                # 링크가 걸린 텍스트가 곧 '안건 제목'이므로 정확하게 덮어쓰기
                item["bill_name"] = clean_text(await a_tag.inner_text())
                
                onclick = await a_tag.get_attribute("onclick") or ""
                href = await a_tag.get_attribute("href") or ""
                
                # GET 방식 (금천구 스타일)
                if href and not href.startswith("javascript") and href != "#":
                    item["link_href"] = href
                    match = re.search(rf"[?&]{view_id_param}=([^&]+)", href)
                    if match: item["view_id"] = match.group(1)
                
                # JS 함수 방식 (구로구 스타일)
                else:
                    js_code = onclick if onclick else href
                    match = re.search(r"\(['\"]?([^'\"),]+)['\"]?\)", js_code)
                    if match: item["view_id"] = match.group(1)
                    
            items.append(item)
        return items
    
    @staticmethod
    async def extract_view_detail(page: Page, view_class: str, base_url: str) -> Dict[str, Any]:
        """상태 기계(State Machine) 패턴을 적용한 만능 테이블 파서"""
        selector = f"table.{view_class}"
        # 금천구처럼 board_view가 아닌 normal_list를 상세로 쓰는 경우 방어
        if await page.locator("table.normal_list").count() > 0 and await page.locator(selector).count() == 0:
            selector = "table.normal_list"
            
        await page.wait_for_selector(selector, timeout=10000)
        rows = await page.query_selector_all(f"{selector} tbody tr")
        
        result = {"sections": {}}
        current_section = None
        rowspan_counter = 0  # 💡 [핵심] 현재 구역이 몇 줄짜리인지 기억하는 카운터

        for row in rows:
            # 💡 [상태 기계] 줄이 바뀔 때마다 카운터를 깎고, 0이 되면 구역(섹션)을 초기화
            if rowspan_counter > 0:
                rowspan_counter -= 1
            if rowspan_counter == 0:
                current_section = None

            ths = await row.query_selector_all("th")
            tds = await row.query_selector_all("td")
            
            th_idx = 0
            td_idx = 0
            
            # 한 줄(tr) 안에 여러 쌍의 th-td 가 있을 수 있으므로 while 루프로 쌍을 맞춤
            while th_idx < len(ths) and td_idx < len(tds):
                th_el = ths[th_idx]
                rowspan = await th_el.get_attribute("rowspan")
                th_text = clean_text(await th_el.inner_text())
                
                # [Case A] rowspan이 걸린 th를 만나면 새로운 구역(섹션) 진입
                if rowspan and int(rowspan) > 1 and th_idx == 0:
                    current_section = th_text
                    rowspan_counter = int(rowspan)
                    th_idx += 1  # 섹션 제목 th는 건너뛰고 다음 th(진짜 라벨)로 이동
                    if th_idx >= len(ths): break
                    th_el = ths[th_idx]
                    th_text = clean_text(await th_el.inner_text())

                label = th_text
                td_el = tds[td_idx]
                val = clean_text(await td_el.inner_text())
                
                # --- 파일/회의록 특수 처리 ---
                is_file = any(x in label for x in ["첨부", "파일", "원문"])
                is_meeting = "회의록" in label
                
                if is_file or is_meeting:
                    links = await td_el.query_selector_all("a")
                    names = [clean_text(await l.inner_text()) for l in links]
                    urls = [urljoin(base_url, await l.get_attribute("href") or "") for l in links]
                    
                    if is_file and names:
                        result["BI_FILE_NM"] = names[0] if len(names) == 1 else names
                        result["BI_FILE_URL"] = urls[0] if len(urls) == 1 else urls
                    if is_meeting and names:
                        target = result["sections"].setdefault(current_section, {}) if current_section else result
                        target["RELATED_MEETING_NM"] = names[0] if len(names) == 1 else names
                        target["RELATED_MEETING_URL"] = urls[0] if len(urls) == 1 else urls
                # --- 일반 텍스트 처리 ---
                else:
                    mapped_key = get_mapped_key(label, current_section)
                    
                    if mapped_key == "PROPSR" and val:
                        val = ", ".join(v for v in val.split() if v)

                    if current_section:
                        # "본회의 처리사항" 등을 "본회의"로 정규화
                        sec_name = "위원회" if "위원회" in current_section else ("본회의" if "본회의" in current_section else current_section)
                        if sec_name not in result["sections"]:
                            result["sections"][sec_name] = {}
                        result["sections"][sec_name][mapped_key] = val
                    else:
                        result[mapped_key] = val
                
                th_idx += 1
                td_idx += 1

        # 빈 섹션 껍데기 제거
        if not result.get("sections"):
            result.pop("sections", None)

        return result

    @staticmethod
    async def get_total_pages(page: Page) -> int:
        """스마트 전체 페이지 수 추출기"""
        try:
            # 1. '마지막' 버튼에서 추출 시도 (가장 정확함)
            last_btn = page.locator(".num_last, .last, [title='마지막']").first
            if await last_btn.count() > 0:
                html = await last_btn.evaluate("el => el.outerHTML")
                # fn_egov_link_page(64) 형태 파싱
                nums = re.findall(r'\((\d+)\)', html)
                if nums: return int(nums[0])
                # page=64 형태 파싱
                nums = re.findall(r'page=(\d+)', html)
                if nums: return int(nums[0])
            
            # 2. 텍스트 정보에서 추출 (예: 1 / 64 page)
            board_total = page.locator(".board_total, .total").first
            if await board_total.count() > 0:
                text_info = await board_total.inner_text()
                nums = re.findall(r'/(\d+)', text_info.replace(' ', ''))
                if nums: return int(nums[0])
        except Exception as e:
            print(f"[!] 전체 페이지 수 추출 실패 (기본값 1 사용): {e}")
            
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

@app.get("/scrapeList")
async def api_scrape_list(
    request: Request,
    list_url: str = Query(..., description="의회 리스트 URL"),
    view_url: Optional[str] = Query(None, description="상세 진입 URL (예: .../billview.do)"),
    view_id_param: str = Query("uuid", description="상세 페이지 식별 파라미터명 (예: code, idx, no, uuid 등)"),
    rasmbly_numpr: str = Query("9", description="대수 (예: 9)"),
    list_class: str = Query("stable", description="리스트 테이블 클래스"),
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
            await UniversalCrawler.apply_filter_and_search(page, rasmbly_numpr)
            
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


@app.get("/scrapeView")
async def api_scrape_view(
    list_url: str = Query(..., description="의회 리스트 URL"),
    view_url: str = Query(..., description="상세 진입 URL (예: .../billview.do)"),
    view_id_param: str = Query("uuid", description="상세 페이지 식별 파라미터명 (예: code, idx, no, uuid 등)"),
    rasmbly_numpr: str = Query("9", description="대수 (예: 9)"),
    list_class: str = Query("stable", description="리스트 테이블 클래스"),
    view_class: str = Query("board_view", description="상세 테이블 클래스"),
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
            await UniversalCrawler.apply_filter_and_search(page, rasmbly_numpr)
            
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

                # 💡 [예외 처리] 아이디도 없고 링크도 없으면 패스
                if not view_id and not link_href: continue

                # 💡 [만능 라우터] 링크 형태에 맞춰 GET / 동적 URL 조립을 알아서 분기합니다.
                if link_href:
                    # [Case A] 금천구처럼 원본 링크에 수많은 필수 파라미터가 섞여 있는 경우
                    target_url = urljoin(list_url, link_href)
                else:
                    # [Case B] 구로구처럼 JS 함수만 있어서 URL을 우리가 직접 창조해야 하는 경우
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