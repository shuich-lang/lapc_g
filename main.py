import asyncio
import json
import os
import re
from datetime import datetime
from typing import List, Dict, Any, Optional
from urllib.parse import urljoin, urlparse, parse_qs

from fastapi import FastAPI, Query, HTTPException, Request, BackgroundTasks
from playwright.async_api import async_playwright, Page
import uuid

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
    label = clean_text(label)
    if section:
        sec_key = "위원회" if "위원회" in section else ("본회의" if "본회의" in section else section)
        if sec_key in SECTION_FIELD_MAP and label in SECTION_FIELD_MAP[sec_key]:
            return SECTION_FIELD_MAP[sec_key][label]
    return FIELD_MAP.get(label, label)


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
                    await page.wait_for_load_state("networkidle")
                    print("[+] 검색 버튼 클릭 완료", flush=True)
                    break
        except Exception as e:
            print(f"[!] 필터 적용 중 예외 발생 (진행 유지): {e}", flush=True)

    @staticmethod
    async def extract_list_page(page: Page, list_class: str) -> List[Dict[str, Any]]:
        selector = f"table.{list_class}"
        await page.wait_for_selector(selector, timeout=10000)
        
        rows = await page.query_selector_all(f"{selector} tbody tr")
        items = []
        
        for row in rows:
            tds = await row.query_selector_all("td")
            if len(tds) < 5: continue
            
            item = {
                "bill_num": clean_text(await tds[0].inner_text()),
                "bill_name": clean_text(await tds[1].inner_text()),
                "proposer": clean_text(await tds[2].inner_text()),
                "session": clean_text(await tds[3].inner_text()),
                "result": clean_text(await tds[4].inner_text())
            }
            
            a_tag = await tds[1].query_selector("a")
            if a_tag:
                onclick = await a_tag.get_attribute("onclick")
                href = await a_tag.get_attribute("href")
                
                if onclick:
                    match = re.search(r"fn_view_page\(['\"]([^'\"]+)['\"]", onclick)
                    if match: item["view_id"] = match.group(1)
                elif href and "code=" in href:
                    item["view_id"] = re.search(r"code=([^&]+)", href).group(1)
                    
            items.append(item)
            
        return items
    
    @staticmethod
    async def extract_view_detail(page: Page, view_class: str, base_url: str) -> Dict[str, Any]:
        selector = f"table.{view_class}"
        await page.wait_for_selector(selector, timeout=10000)
        
        rows = await page.query_selector_all(f"{selector} tbody tr")
        result = {"sections": {}}
        current_section = None

        for row in rows:
            ths = await row.query_selector_all("th")
            tds = await row.query_selector_all("td")
            if not ths or not tds: continue

            rowspan = await ths[0].get_attribute("rowspan")
            if rowspan and int(rowspan) > 1 and len(ths) > 1:
                current_section = clean_text(await ths[0].inner_text())
                label = clean_text(await ths[1].inner_text())
                td_el = tds[0]
            elif current_section and len(ths) == 1 and not rowspan:
                label = clean_text(await ths[0].inner_text())
                td_el = tds[0]
                if "공포" in label or "첨부" in label:
                    current_section = None
            else:
                label = clean_text(await ths[0].inner_text())
                td_el = tds[0]
                current_section = None

            val = clean_text(await td_el.inner_text())
            
            is_file = any(x in label for x in ["첨부", "파일", "원문"])
            is_meeting = "회의록" in label

            if is_file or is_meeting:
                links = await td_el.query_selector_all("a")
                names, urls = [], []
                for l in links:
                    names.append(clean_text(await l.inner_text()))
                    urls.append(urljoin(base_url, await l.get_attribute("href")))

                if is_file and names:
                    result["BI_FILE_NM"] = names[0] if len(names) == 1 else names
                    result["BI_FILE_URL"] = urls[0] if len(urls) == 1 else urls
                
                if is_meeting and names:
                    target_dict = result["sections"].setdefault(current_section, {}) if current_section else result
                    target_dict["RELATED_MEETING_NM"] = names[0] if len(names) == 1 else names
                    target_dict["RELATED_MEETING_URL"] = urls[0] if len(urls) == 1 else urls
            else:
                mapped_key = get_mapped_key(label, current_section)
                if current_section:
                    sec_name = "위원회" if "위원회" in current_section else ("본회의" if "본회의" in current_section else current_section)
                    result["sections"].setdefault(sec_name, {})[mapped_key] = val
                else:
                    result[mapped_key] = val

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
    async def go_to_page(page: Page, next_page: int) -> bool:
        """지능형 다음 페이지 이동 알고리즘"""
        try:
            # 1. 전자정부표준프레임워크 (관공서 90% 이상) JS 직접 실행
            is_egov = await page.evaluate("typeof fn_egov_link_page === 'function'")
            if is_egov:
                await page.evaluate(f"fn_egov_link_page({next_page});")
                await page.wait_for_load_state("networkidle")
                return True
        except:
            pass
        
        try:
            # 2. 직접 <a> 태그 텍스트 매칭해서 클릭 (GET 링크나 일반 onclick 대응)
            # 정확히 숫자만 있는 링크를 찾음
            link = page.locator("div.pagination a, div.paging a, #pagingNav a").filter(has_text=re.compile(f"^{next_page}$")).first
            
            if await link.count() > 0:
                # 해당 숫자가 안 보이면 (예: 11페이지로 가야하는데 현재 1~10만 보임) '다음' 버튼 클릭
                if not await link.is_visible():
                    next_block_btn = page.locator(".num_right, .next, [title='다음']").first
                    if await next_block_btn.count() > 0:
                        await next_block_btn.click()
                        await page.wait_for_load_state("networkidle")
                        
                # 버튼이 보이면 클릭
                await link.click()
                await page.wait_for_load_state("networkidle")
                return True
        except:
            pass
            
        return False

    @staticmethod
    async def extract_view_detail(page: Page, view_class: str, base_url: str) -> Dict[str, Any]:
        selector = f"table.{view_class}"
        await page.wait_for_selector(selector, timeout=10000)
        rows = await page.query_selector_all(f"{selector} tbody tr")
        result = {"sections": {}}
        current_section = None

        for row in rows:
            ths, tds = await row.query_selector_all("th"), await row.query_selector_all("td")
            if not ths or not tds: continue

            rowspan = await ths[0].get_attribute("rowspan")
            if rowspan and int(rowspan) > 1 and len(ths) > 1:
                current_section, label, td_el = clean_text(await ths[0].inner_text()), clean_text(await ths[1].inner_text()), tds[0]
            elif current_section and len(ths) == 1 and not rowspan:
                label, td_el = clean_text(await ths[0].inner_text()), tds[0]
                if any(x in label for x in ["공포", "첨부"]): current_section = None
            else:
                label, td_el, current_section = clean_text(await ths[0].inner_text()), tds[0], None

            val = clean_text(await td_el.inner_text())
            is_file, is_meeting = any(x in label for x in ["첨부", "파일", "원문"]), "회의록" in label

            if is_file or is_meeting:
                links = await td_el.query_selector_all("a")
                names = [clean_text(await l.inner_text()) for l in links]
                urls = [urljoin(base_url, await l.get_attribute("href")) for l in links]

                if is_file and names:
                    result["BI_FILE_NM"] = names[0] if len(names) == 1 else names
                    result["BI_FILE_URL"] = urls[0] if len(urls) == 1 else urls
                if is_meeting and names:
                    target = result["sections"].setdefault(current_section, {}) if current_section else result
                    target["RELATED_MEETING_NM"] = names[0] if len(names) == 1 else names
                    target["RELATED_MEETING_URL"] = urls[0] if len(urls) == 1 else urls
            else:
                mapped_key = get_mapped_key(label, current_section)
                if current_section:
                    sec_name = "위원회" if "위원회" in current_section else ("본회의" if "본회의" in current_section else current_section)
                    result["sections"].setdefault(sec_name, {})[mapped_key] = val
                else: result[mapped_key] = val
        return result


# --- REST API Endpoints ---

@app.get("/scrapeList")
async def api_scrape_list(
    request: Request,
    url: str = Query(..., description="의회 리스트 URL"), 
    rasmbly_numpr: str = Query("9", description="대수 (예: 9)"), 
    listClass: str = Query("stable", description="리스트 테이블 클래스"),
    max_pages: int = Query(0, description="0이면 전체 페이지 자동 수집, 숫자면 해당 페이지까지만")
):
    app.state.stop_scraping = False 
    domain = extract_domain(url)
    all_items = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        try:
            await page.goto(url, wait_until="networkidle", timeout=30000)
            await UniversalCrawler.apply_filter_and_search(page, rasmbly_numpr)
            
            # 💡 [핵심] 전체 페이지 수 자동 감지
            total_pages = await UniversalCrawler.get_total_pages(page)
            target_pages = total_pages if max_pages == 0 else min(max_pages, total_pages)
            print(f"[*] 총 {target_pages} 페이지 수집을 시작합니다. (발견된 전체 페이지: {total_pages})", flush=True)

            # 페이지 반복 수집
            for current_page in range(1, target_pages + 1):
                if app.state.stop_scraping:
                    print("[!] /stop 요청 감지: 리스트 수집을 조기 중단합니다.", flush=True)
                    break
                
                print(f"[*] {current_page}/{target_pages} 페이지 파싱 중...", flush=True)
                items = await UniversalCrawler.extract_list_page(page, listClass)
                all_items.extend(items)
                
                # 다음 페이지로 이동
                if current_page < target_pages:
                    moved = await UniversalCrawler.go_to_page(page, current_page + 1)
                    if not moved:
                        print(f"[!] {current_page + 1} 페이지로 이동 실패. 수집을 종료합니다.")
                        break

            # 수집된 데이터 저장
            filepath = save_to_json(all_items, domain, "list")

            return {
                "ok": True,
                "domain": domain,
                "saved_file": filepath,
                "total_count": len(all_items),
                "total_pages_scraped": current_page,
                "is_stopped_early": app.state.stop_scraping,
                "data": all_items
            }

        except Exception as e:
            filepath = save_to_json(all_items, domain, "list_error_partial")
            return {
                "ok": False,
                "error_msg": str(e),
                "saved_file": filepath,
                "total_count": len(all_items),
                "data": all_items
            }
        finally:
            await browser.close()


@app.get("/scrapeView")
async def api_scrape_view(
    url: str = Query(..., description="의회 리스트 URL"),
    view_template: str = Query(..., description="상세 진입 템플릿 URL (예: ...billview.do?code={view_id})"),
    rasmbly_numpr: str = Query("9", description="대수 (예: 9)"),
    listClass: str = Query("stable", description="리스트 테이블 클래스"),
    viewClass: str = Query("board_view", description="상세 테이블 클래스"),
    max_pages: int = Query(0, description="0이면 전체 페이지 자동 수집, 숫자면 해당 페이지까지만")
):
    domain = extract_domain(url)
    all_data = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        try:
            # 1. 먼저 리스트를 모두 수집합니다.
            print(f"[*] [통합수집 1단계] 리스트 페이지 진입: {url}", flush=True)
            await page.goto(url, wait_until="networkidle", timeout=30000)
            await UniversalCrawler.apply_filter_and_search(page, rasmbly_numpr)
            
            total_pages = await UniversalCrawler.get_total_pages(page)
            target_pages = total_pages if max_pages == 0 else min(max_pages, total_pages)
            
            for current_page in range(1, target_pages + 1):
                print(f"[*] 리스트 수집 중: {current_page}/{target_pages} 페이지", flush=True)
                items = await UniversalCrawler.extract_list_page(page, listClass)
                all_data.extend(items)
                
                if current_page < target_pages:
                    moved = await UniversalCrawler.go_to_page(page, current_page + 1)
                    if not moved: break

            # 2. 추출된 ID를 기반으로 뷰 페이지를 순차 수집하여 병합합니다.
            total_items = len(all_data)
            print(f"[*] [통합수집 2단계] 총 {total_items}건의 상세 뷰 수집을 시작합니다.", flush=True)

            for idx, item in enumerate(all_data):
                view_id = item.get("view_id")
                if not view_id: continue

                # 템플릿 URL의 {view_id} 부분을 실제 아이디로 치환하여 상세 URL 생성
                target_url = view_template.replace("{view_id}", view_id)
                print(f"[*] 상세 수집 중 ({idx+1}/{total_items}) ... ID: {view_id}", flush=True)
                
                try:
                    await page.goto(target_url, wait_until="networkidle", timeout=15000)
                    parsed = urlparse(target_url)
                    base_url = f"{parsed.scheme}://{parsed.netloc}"
                    
                    view_detail = await UniversalCrawler.extract_view_detail(page, viewClass, base_url)
                    
                    # Sections 데이터를 마지막으로 내리기 위한 분리
                    sections_data = view_detail.pop("sections", {})
                    
                    # 리스트 원본 + 상세 정보 + Sections 병합
                    all_data[idx] = {
                        "view_id": view_id,
                        "view_url": target_url,
                        **item,
                        **view_detail,
                        "sections": sections_data
                    }
                except Exception as e:
                    print(f"[!] {view_id} 상세 수집 실패: {e}", flush=True)
                    all_data[idx]["view_error"] = str(e) # 한 건이 실패해도 전체 로직은 멈추지 않습니다.

            # 최종 병합 파일 저장
            filepath = save_to_json(all_data, domain, "view_all")
            
            return {
                "ok": True,
                "domain": domain,
                "saved_file": filepath,
                "total_count": len(all_data),
                "data": all_data
            }

        except Exception as e:
            filepath = save_to_json(all_data, domain, "view_error_partial")
            return {"ok": False, "error_msg": str(e), "saved_file": filepath, "data": all_data}
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