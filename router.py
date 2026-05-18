import json
from fastapi import APIRouter, BackgroundTasks, Request
from fastapi.responses import JSONResponse
from pydantic import ValidationError, BaseModel
from minutes import app as minutes_app
from minutes import run_minutes_all_and_callback  # 회의록 실행 함수

# ── bill.py 주석 처리 ─────────────────────────────────────────────
# from bill import (
#     app as bill_app,
#     ScrapeRequest,
#     execute_view_scraping,
#     execute_view_scraping_test as bill_test,
# )

# ── crawler.py (bill + laman) ─────────────────────────────────────
from crawler import (
    app as crawler_app,
    UnifiedRequest,
    ScrapeRequest,
    _route_request,
    execute_bill_scraping,
    execute_bill_scraping_test,
    PolicyRequest,
    execute_policy_scraping,
    execute_policy_scraping_test,
    PrismRequest,
    execute_prism_scraping,
    execute_prism_scraping_test,
)

from minutes import (
    app as minutes_app,
    CrawlRequest,
    parse_crawl_request,
    run_minutes_all_and_callback,
    crawl_minutes_regex_check,
    build_minutes_callback_payload,
)
from five_mins_free_spch import (
    app as free5min_app,
    SpchCrawlRequest,
    crawl_spch_regex_check,
    build_spch_callback_payload,
    run_spch_all_and_callback,
)
from laman import (
    app as laman_app,
    LamanCrawlRequest,
    crawl_laman_regex_check,
    build_laman_callback_payload,
    run_laman_all_and_callback,
)
from old_minutes import (
    OldMinutesCrawlRequest,
    run_old_minutes_and_callback,
    build_old_minutes_callback_payload,
    crawl_old_minutes,
)
from crawl_status import create_job, get_job, set_job_running, set_job_done, set_job_failed

router = APIRouter()

class CrawlStatusRequest(BaseModel):
    req_id: str

# router.include_router(bill_app.router, tags=["Bill"])  # bill.py 비활성화


# ── 백그라운드 작업 래퍼 ──────────────────────────────────────────
async def run_bill_job(req_obj):
    try:
        await set_job_running(req_obj.req_id)
        await execute_bill_scraping(req_obj)
        await set_job_done(req_obj.req_id)
    except Exception:
        await set_job_failed(req_obj.req_id)

async def run_minutes_job(req_obj):
    try:
        await set_job_running(req_obj.req_id)
        await run_minutes_all_and_callback(req_obj)
        await set_job_done(req_obj.req_id)
    except Exception:
        await set_job_failed(req_obj.req_id)

async def run_spch_job(req_obj):
    try:
        await set_job_running(req_obj.req_id)
        await run_spch_all_and_callback(req_obj)
        await set_job_done(req_obj.req_id)
    except Exception:
        await set_job_failed(req_obj.req_id)

async def run_policy_job(req_obj):
    try:
        await set_job_running(req_obj.req_id)
        await execute_policy_scraping(req_obj)
        await set_job_done(req_obj.req_id)
    except Exception:
        await set_job_failed(req_obj.req_id)

async def run_prism_job(req_obj):
    try:
        await set_job_running(req_obj.req_id)
        await execute_prism_scraping(req_obj)
        await set_job_done(req_obj.req_id)
    except Exception:
        await set_job_failed(req_obj.req_id)

async def run_laman_job(req_obj):
    try:
        await set_job_running(req_obj.req_id)
        await run_laman_all_and_callback(req_obj)
        await set_job_done(req_obj.req_id)
    except Exception:
        await set_job_failed(req_obj.req_id)

async def run_old_minutes_job(req_obj):
    try:
        await set_job_running(req_obj.req_id)
        await run_old_minutes_and_callback(req_obj)
        await set_job_done(req_obj.req_id)
    except Exception:
        await set_job_failed(req_obj.req_id)


def handle_validation_error(e: ValidationError):
    errors = e.errors()
    first_err = errors[0]
    field_name = first_err.get("loc")[-1]
    err_type = first_err.get("type")
    msg = "필수값이 누락되었습니다." if err_type == "missing" else first_err.get("msg")
    return JSONResponse(
        status_code=200,
        content={"ok": False, "message": f"파라미터 오류: [{field_name}] {msg}", "detail": errors}
    )


@router.get("/health")
async def health():
    return {"status": "ok"}


@router.post("/crawl")
async def integrated_crawl_api(request: Request, background_tasks: BackgroundTasks):
    try:
        json_data = await request.json()
    except Exception:
        return JSONResponse(status_code=200, content={"ok": False, "message": "JSON 포맷이 올바르지 않습니다."})

    req_type = json_data.get("type")
    if not req_type:
        return JSONResponse(status_code=200, content={"ok": False, "message": "[type] 파라미터는 필수입니다."})

    try:
        if req_type == "bill":
            raw = UnifiedRequest(**json_data)
            req_obj = _route_request(raw)          # ScrapeRequest 반환
            await create_job(req_obj.req_id)
            background_tasks.add_task(run_bill_job, req_obj)
        
        elif "old_minutes" in req_type:
            req_obj = OldMinutesCrawlRequest(**json_data)
            await create_job(req_obj.req_id)
            background_tasks.add_task(run_old_minutes_job, req_obj)

        elif "minutes" in req_type:
            raw = CrawlRequest(**json_data)
            req_obj = parse_crawl_request(raw)
            await create_job(req_obj.req_id)
            background_tasks.add_task(run_minutes_job, req_obj)

        elif req_type == "policy":
            raw = UnifiedRequest(**json_data)
            req_obj = _route_request(raw)
            await create_job(req_obj.req_id)
            background_tasks.add_task(run_policy_job, req_obj)

        elif "free5min" in req_type:
            req_obj = SpchCrawlRequest(**json_data)
            await create_job(req_obj.req_id)
            background_tasks.add_task(run_spch_job, req_obj)

        elif req_type == "prism":
            raw = UnifiedRequest(**json_data)
            req_obj = _route_request(raw)
            await create_job(req_obj.req_id)
            background_tasks.add_task(run_prism_job, req_obj)

        elif "laman" in req_type:
            req_obj = LamanCrawlRequest(**json_data)
            await create_job(req_obj.req_id)
            background_tasks.add_task(run_laman_job, req_obj)

        else:
            return JSONResponse(status_code=200, content={"ok": False, "message": f"지원하지 않는 type: {req_type}"})

    except ValidationError as e:
        return handle_validation_error(e)
    except ValueError as e:
        return JSONResponse(status_code=200, content={"ok": False, "message": str(e)})

    return {
        "req_id":   json_data.get("req_id"),
        "type":     req_type,
        "crw_id":   json_data.get("crw_id"),
        "file_dir": json_data.get("file_dir"),
        "ok":       True,
        "message":  f"[{req_type}] 수집 작업을 시작했습니다."
    }


@router.post("/crawl/test")
async def integrated_crawl_test_api(request: Request):
    try:
        json_data = await request.json()
    except Exception:
        return JSONResponse(status_code=200, content={"ok": False, "message": "JSON 포맷이 올바르지 않습니다."})

    req_type = json_data.get("type")
    if not req_type:
        return JSONResponse(status_code=200, content={"ok": False, "message": "[type] 파라미터는 필수입니다."})

    try:
        if req_type == "bill":
            raw = UnifiedRequest(**json_data)
            req_obj = _route_request(raw)
            return await execute_bill_scraping_test(req_obj)

        elif req_type == "policy":
            raw = UnifiedRequest(**json_data)
            req_obj = _route_request(raw)
            return await execute_policy_scraping_test(req_obj)

        elif req_type == "minutes":
            raw = CrawlRequest(**json_data)
            req_obj = parse_crawl_request(raw)
            crawl_response = await crawl_minutes_regex_check(req_obj, crawl_all=False)
            return build_minutes_callback_payload(req_obj, crawl_response)

        elif req_type == "free5min":
            req_obj = SpchCrawlRequest(**json_data)
            crawl_response = await crawl_spch_regex_check(req_obj, crawl_all=False)
            return build_spch_callback_payload(req_obj, crawl_response)
        
        elif req_type == "prism":
            raw = UnifiedRequest(**json_data)
            req_obj = _route_request(raw)
            return await execute_prism_scraping_test(req_obj)
        
        elif req_type == "laman":
            req_obj = LamanCrawlRequest(**json_data)
            crawl_response = await crawl_laman_regex_check(req_obj, crawl_all=False)
            return build_laman_callback_payload(req_obj, crawl_response)
        
        elif req_type == "old_minutes":
            req_obj = OldMinutesCrawlRequest(**json_data)
            error_logs = []
            items = await crawl_old_minutes(req_obj, error_logs)
            return build_old_minutes_callback_payload(req_obj, items, error_logs)

        else:
            return JSONResponse(status_code=200, content={"ok": False, "message": f"지원하지 않는 type: {req_type}"})

    except ValidationError as e:
        return handle_validation_error(e)
    except ValueError as e:
        return JSONResponse(status_code=200, content={"ok": False, "message": str(e)})


@router.get("/crawl/status")
async def integrated_crawl_status_api(req_id: str):
    job = await get_job(req_id)
    if not job:
        return JSONResponse(status_code=200, content={"req_id": req_id, "status": "NOT_FOUND"})
    return {"req_id": job["req_id"], "status": job["status"]}


@router.post("/insert_api")
async def insert_api(payload: dict):
    print("===== insert_api callback received =====")
    print(f"callback data size: {len(payload.get('data', []))}")
    with open("result.json", "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    return {"result": "ok"}


@router.get("/crawl/stop")
async def integrated_crawl_stop():
    stopped = []

    # ── bill / laman 중단 (crawler.py 공유 state) ─────────────────
    if not crawler_app.state.stop_scraping:
        crawler_app.state.stop_scraping = True
        stopped.append("bill/laman/policy/prism")
        print("[!] stop_scraping = True", flush=True)

    if stopped:
        return {"ok": True, "stopped": stopped, "message": f"{stopped} 중단 요청 완료. 현재 수집 건 완료 후 중단됩니다."}
    return {"ok": True, "stopped": [], "message": "실행 중인 수집 태스크가 없습니다."}