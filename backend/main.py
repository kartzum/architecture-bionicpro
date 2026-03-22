from fastapi import FastAPI, HTTPException, Depends, status
from fastapi.responses import Response, JSONResponse
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi.middleware.cors import CORSMiddleware
from fastapi.logger import logger
from jose import jwt, JWTError
import csv
import io
import os
import httpx
from typing import Dict, Any
import logging
import clickhouse_connect

app = FastAPI(title="Reports Backend", version="0.0.1")

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://frontend:3000"],
    allow_credentials=True,
    allow_methods=["GET", "OPTIONS"],
    allow_headers=["Authorization", "Content-Type"],
    expose_headers=["Content-Disposition"],
    max_age=3600,
)

security = HTTPBearer()

KEYCLOAK_URL = os.getenv("KEYCLOAK_URL", "http://keycloak:8080")
REALM = os.getenv("KEYCLOAK_REALM", "reports-realm")

JWKS_URL = f"{KEYCLOAK_URL}/realms/{REALM}/protocol/openid-connect/certs"

jwks_cache: Dict[str, Any] = {}


async def get_jwks():
    logger.info(f"get_jwks. url={JWKS_URL}")
    global jwks_cache
    if not jwks_cache:
        async with httpx.AsyncClient() as client:
            resp = await client.get(JWKS_URL)
            resp.raise_for_status()
            jwks_cache = resp.json()
    return jwks_cache


async def get_public_key(kid: str):
    jwks = await get_jwks()
    for key in jwks.get("keys", []):
        if key["kid"] == kid:
            return key
    raise HTTPException(status_code=401, detail="Public key not found")


async def verify_token(credentials: HTTPAuthorizationCredentials = Depends(security)):
    token = credentials.credentials
    headers = jwt.get_unverified_header(token)
    kid = headers.get("kid")
    if not kid:
        raise HTTPException(status_code=401, detail="Invalid token header")

    public_key = await get_public_key(kid)

    try:
        payload = jwt.decode(
            token,
            public_key,
            algorithms=["RS256"],
            audience="reports-frontend",
            options={"verify_exp": True, "verify_aud": True}
        )
        return payload
    except JWTError as e:
        raise HTTPException(status_code=401, detail=f"Invalid token: {str(e)}")


def ch_create_client():
    clickhouse_host = os.getenv("CH_HOST", "clickhouse")
    clickhouse_username = os.getenv("CH_USERNAME", "admin")
    clickhouse_password = os.getenv("CH_PASSWORD", "admin")
    return clickhouse_connect.get_client(
        host=clickhouse_host,
        username=clickhouse_username,
        password=clickhouse_password
    )


def ch_execute_query(client, query, parameters):
    return client.query(query, parameters=parameters)

def ch_execute_report_data(user_name):
    client = ch_create_client()
    parameters = {'user_name': user_name}
    query = "SELECT username, thing_id, thing_param_name, thing_value, processed_at FROM reports_mart WHERE username={user_name:String} ORDER BY processed_at DESC LIMIT 10"
    r = ch_execute_query(client, query, parameters)
    data = []
    for row in r.result_set:
        i = [row[0], row[1], row[2], row[3], row[4]]
        data.append(i)
    return data


@app.get("/reports")
async def get_reports(user: dict = Depends(verify_token)):
    logger.info(f"get_reports. starting")

    user_name = user["preferred_username"]

    logger.info(f"get_reports. user_name={user_name}")

    data = ch_execute_report_data(user_name)

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerows(data)

    return Response(
        content=output.getvalue(),
        media_type="text/csv",
        headers={
            "Content-Disposition": "attachment; filename=reports.csv",
            "Content-Type": "text/csv; charset=utf-8"
        }
    )


@app.get("/health")
async def health_check():
    return {"status": "OK"}


def main():
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)


if __name__ == "__main__":
    main()
