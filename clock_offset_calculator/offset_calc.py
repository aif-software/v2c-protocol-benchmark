from fastapi import FastAPI
import time

import uvicorn

app = FastAPI()


@app.get("/sync")
async def sync():
    return {"t_server": time.time_ns()}


if __name__ == "__main__":
    uvicorn.run("offset_calc:app", host="0.0.0.0", port=8000)
