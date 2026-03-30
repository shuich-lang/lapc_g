from fastapi import APIRouter
from bill import app as bill_app
from minutes import app as minutes_app

router = APIRouter()

router.include_router(bill_app.router, tags=["Bill"])
router.include_router(minutes_app.router, tags=["Minutes"])