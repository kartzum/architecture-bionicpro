import os
import csv
from fastapi import FastAPI, Depends
import uvicorn

app = FastAPI()


@app.get("/health")
async def health_check():
    return {"status": "OK"}


def main():
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)


if __name__ == "__main__":
    main()
