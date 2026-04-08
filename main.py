from fastapi import FastAPI
from router import router as api_router

app = FastAPI(title="Integrated Council API")

app.include_router(api_router)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8900)  