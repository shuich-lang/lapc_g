import asyncio
import logging

logger = logging.getLogger(__name__)

JOB_STORE = {}
JOB_LOCK = asyncio.Lock()


async def create_job(req_id: str):
    async with JOB_LOCK:
        JOB_STORE[req_id] = {
            "req_id": req_id,
            "status": "WAITING"
        }


async def set_job_running(req_id: str):
    async with JOB_LOCK:
        if req_id in JOB_STORE:
            JOB_STORE[req_id]["status"] = "RUNNING"


async def set_job_done(req_id: str):
    async with JOB_LOCK:
        if req_id in JOB_STORE:
            JOB_STORE[req_id]["status"] = "DONE"


async def set_job_failed(req_id: str):
    async with JOB_LOCK:
        if req_id in JOB_STORE:
            JOB_STORE[req_id]["status"] = "FAILED"


async def get_job(req_id: str):
    async with JOB_LOCK:
        return JOB_STORE.get(req_id)


# ──────────────────────────────────────────────
# TaskManager
# ──────────────────────────────────────────────
class TaskManager:
    """
    Semaphore 기반 동시성 제어
    - max_concurrency: 최대 동시 실행 수
    - max_queue_size: 대기열 최대 크기 (0은 무제한)
    """

    def __init__(self, max_concurrency: int = 5, max_queue_size: int = 100):
        self._semaphore = asyncio.Semaphore(max_concurrency)
        self._max_queue_size = max_queue_size
        self._pending = 0
        self._running = 0
        self._lock = asyncio.Lock()
    
    async def submit(self, coro_func, *args, **kwargs):
        """작업 제출, 큐 초과 시 False 반환"""
        async with self._lock:
            if self._max_queue_size > 0 and self._pending >= self._max_queue_size:
                logger.warning(
                    f"작업 큐 초과: (대기: {self._pending}, 최대: {self._max_queue_size})"
                )
                return False
            self._pending += 1
        
        asyncio.create_task(self._run(coro_func, *args, **kwargs))
        return True
    
    async def _run(self, coro_func, *args, **kwargs):
        async with self._semaphore:
            async with self._lock:
                self._pending -= 1
                self._running += 1
            logger.info(f"작업 시작 (실행: {self._running}, 대기: {self._pending})")
            try:
                await coro_func(*args, **kwargs)
            except Exception as e:
                logger.error(f"작업 실행 중 오류: {e}", exc_info=True)
            finally:
                async with self._lock:
                    self._running -= 1
                logger.info(f"작업 완료 (실행: {self._running}, 대기: {self._pending})")
    
    async def status(self) -> dict:
        async with self._lock:
            return {
                "running": self._running,
                "pending": self._pending,
                "max_concurrency": self._semaphore._value + self._running,
                "max_queue_size": self._max_queue_size
            }

# 싱글톤 — 필요 시 환경변수 등으로 값 조정
task_manager = TaskManager(max_concurrency=5, max_queue_size=100)