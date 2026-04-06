from fastapi import APIRouter, BackgroundTasks, Depends, Request
from fastapi.responses import JSONResponse
from pydantic import ValidationError
from bill import app as bill_app
from minutes import app as minutes_app
from bill import execute_view_scraping  # 의안 실행 함수
from minutes import run_minutes_all_and_callback  # 회의록 실행 함수

router = APIRouter()

router.include_router(bill_app.router, tags=["Bill"])
router.include_router(minutes_app.router, tags=["Minutes"])

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
        if req_type == "bill":
            from bill import ScrapeRequest, execute_view_scraping
            req_obj = ScrapeRequest(**json_data)  # 여기서 Pydantic 검증 발생
            background_tasks.add_task(execute_view_scraping, req_obj)
            
        elif req_type == "minutes":
            from minutes import RegexCrawlRequest, run_minutes_all_and_callback
            req_obj = RegexCrawlRequest(**json_data)
            background_tasks.add_task(run_minutes_all_and_callback, req_obj)
            
        else:
            return JSONResponse(status_code=200, content={"ok": False, "message": f"지원하지 않는 type입니다: {req_type}"})

    except ValidationError as e:
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
                "detail": errors # 상세 로그 포함
            }
        )

    # 5. 정상 시작 응답
    return {
        "req_id": json_data.get("req_id"),
        "type": req_type,
        "crw_id": json_data.get("crw_id"),
        "ok": True,
        "message": f"[{req_type}] 수집 작업을 시작했습니다."
    }