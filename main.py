import logging
from logging_config import setup_logging, redirect_print

setup_logging(level=logging.INFO)

logger = logging.getLogger(__name__)

for name in ("uvicorn", "uvicorn.access", "uvicorn.error", "fastapi"):
    uv_logger = logging.getLogger(name)
    uv_logger.handlers = []
    uv_logger.propagate = True

from fastapi import FastAPI
from router import router as api_router

app = FastAPI(title="Integrated Council API")
app.include_router(api_router)

if __name__ == "__main__":
    import uvicorn
    redirect_print()   # 로그 출력을 파일과 콘솔로 모두 처리하도록 설정
    uvicorn.run(app, host="0.0.0.0", port=8900)