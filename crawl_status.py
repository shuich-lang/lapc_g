import asyncio

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