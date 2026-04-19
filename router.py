import json

from fastapi import APIRouter, BackgroundTasks, Request
from fastapi.responses import JSONResponse
from pydantic import ValidationError, BaseModel


from bill import (
    app as bill_app,
    ScrapeRequest,
    execute_view_scraping,
    execute_view_scraping_test as bill_test,
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

from policy import (
    PolicyRequest,
    execute_policy_scraping,
    execute_policy_scraping_test as policy_test,
)

from crawl_status import create_job, get_job, set_job_running, set_job_done, set_job_failed



router = APIRouter()

class CrawlStatusRequest(BaseModel):
    req_id: str

router.include_router(bill_app.router, tags=["Bill"])
#router.include_router(minutes_app.router, tags=["Minutes"])

async def run_bill_job(req_obj):
    try:
        await set_job_running(req_obj.req_id)
        await execute_view_scraping(req_obj)
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

def handle_validation_error(e: ValidationError):
    # 4. Pydantic에서 던지는 ValidationError를 가공하여 응답
    errors = e.errors()
    first_err = errors[0]
    # loc는 ['body', 'crw_id'] 식이므로 마지막 요소가 필드명입니다.
    field_name = first_err.get("loc")[-1]
    err_type = first_err.get("type")

    # 에러 메시지 커스텀
    msg = "필수값이 누락되었습니다." if err_type == "missing" else first_err.get("msg")

    return JSONResponse(
        status_code=200, # 500 에러 방지
        content={
            "ok": False,
            "message": f"파라미터 오류: [{field_name}] {msg}",
            "detail": errors, # 상세 로그 포함
        }
    )

@router.get("/health")
async def health():
	return {"status": "ok"}

@router.post("/crawl")
async def integrated_crawl_api(request: Request, background_tasks: BackgroundTasks):
    # 1. 원본 데이터 로드
    try:
        json_data = await request.json()
    except Exception:
        return JSONResponse(status_code=200, content={"ok": False, "message": "JSON 포맷이 올바르지 않습니다."})

    req_type = json_data.get("type")
    
    # 2. type 파라미터 체크 (가장 기본)
    if not req_type:
        return JSONResponse(status_code=200, content={"ok": False, "message": "[type] 파라미터는 필수입니다. (bill 또는 minutes)"})

    try:
        # 3. 타입에 따른 모델 검증 분기
        if "bill" in req_type:
            req_obj = ScrapeRequest(**json_data)  # 여기서 Pydantic 검증 발생
            await create_job(req_obj.req_id)
            background_tasks.add_task(run_bill_job, req_obj)
            
        elif "minutes" in req_type:
            raw = CrawlRequest(**json_data)
            req_obj = parse_crawl_request(raw)
            await create_job(req_obj.req_id)
            background_tasks.add_task(run_minutes_job, req_obj)
        
        elif "free5min" in req_type:
            req_obj = SpchCrawlRequest(**json_data)
            await create_job(req_obj.req_id)
            background_tasks.add_task(run_spch_job, req_obj)

        elif "policy" in req_type:
            req_obj = PolicyRequest(**json_data)
            await create_job(req_obj.req_id)
            background_tasks.add_task(run_policy_job, req_obj)
            
        else:
            return JSONResponse(status_code=200, content={"ok": False, "message": f"지원하지 않는 type입니다: {req_type}"})

    except ValidationError as e:
        return handle_validation_error(e)

    # 5. 정상 시작 응답
    return {
        "req_id": json_data.get("req_id"),
        "type": req_type,
        "crw_id": json_data.get("crw_id"),
        "file_dir": json_data.get("file_dir"),
        "ok": True,
        "message": f"[{req_type}] 수집 작업을 시작했습니다."
    }

@router.post("/crawl/test")
async def integrated_crawl_test_api(request: Request):
    try:
        json_data = await request.json()
    except Exception:
        return JSONResponse(status_code=200, content={"ok": False, "message": "JSON 포맷이 올바르지 않습니다."})

    req_type = json_data.get("type")

    if not req_type:
        return JSONResponse(status_code=200, content={"ok": False, "message": "[type] 파라미터는 필수입니다. (bill 또는 minutes)"})

    try:
        if req_type == "bill":
            req_obj = ScrapeRequest(**json_data)
            return await bill_test(req_obj)

        elif req_type == "minutes":
            raw = CrawlRequest(**json_data)
            req_obj = parse_crawl_request(raw)
            crawl_response = await crawl_minutes_regex_check(req_obj, crawl_all=False)
            return build_minutes_callback_payload(req_obj, crawl_response)
        
        elif req_type == "free5min":
            req_obj = SpchCrawlRequest(**json_data)
            crawl_response = await crawl_spch_regex_check(req_obj, crawl_all=False)
            return build_spch_callback_payload(req_obj, crawl_response)
        
        elif req_type == "policy":
            req_obj = PolicyRequest(**json_data)
            return await policy_test(req_obj)

        else:
            return JSONResponse(status_code=200, content={"ok": False, "message": f"지원하지 않는 type입니다: {req_type}"})

    except ValidationError as e:
        return handle_validation_error(e)


@router.get("/crawl/status")
async def integrated_crawl_status_api(req_id: str):
    job = await get_job(req_id)

    if not job:
        return JSONResponse(
            status_code=200,
            content={
                "req_id": req_id,
                "status": "NOT_FOUND"
            }
        )

    return {
        "req_id": job["req_id"],
        "status": job["status"]
    }


@router.post("/insert_api")
async def insert_api(payload: dict):
	print("===== insert_api callback received =====")
	print(f"callback data size: {len(payload.get('data', []))}")

	with open("result.json", "w", encoding="utf-8") as f:
		json.dump(payload, f, ensure_ascii=False, indent=2)
	
	return {"result": "ok"}