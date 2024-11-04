import uvicorn
from fastapi import FastAPI, HTTPException, BackgroundTasks
from pydantic import BaseModel
import httpx
import datetime
from typing import Optional
from contextlib import asynccontextmanager

SWAGGER_HEADERS = {
    "title": "LINKBRICKS HORIZON-AI LLM BUDDY API ENGINE",
    "version": "100.100.100",
    "description": "## Independent  LLM BUDDY Engine for LINKBRICKS HORIZON-Ai LLM Workflow  \n - API Swagger \n - Multilingual Workflow Support",
    "contact": {
        "name": "Linkbricks Horizon AI",
        "url": "https://www.linkbricks.com",
        "email": "contact@linkbricks.com",
        "license_info": {
            "name": "GNU GPL 3.0",
            "url": "https://www.gnu.org/licenses/gpl-3.0.html",
        },
    },
}

@asynccontextmanager
async def lifespan(app: FastAPI):
    # 전역 AsyncClient 설정
    app.state.client = httpx.AsyncClient(timeout=None)
    yield
    await app.state.client.aclose()

app = FastAPI(**SWAGGER_HEADERS, lifespan=lifespan)

REQUIRED_AUTH_KEY = "linkbricks-saxoji-benedict-ji-01034726435!@#$%231%$#@%"

class TTSRequest(BaseModel):
    auth_key: str
    base_id: str
    table_id: str
    airtable_api_key: str
    flowise_id: str
    id: str
    pwd: str
    timezone: int
    order: str
    chat_id: str
    session_id: str
    category: str

async def create_airtable_record(client: httpx.AsyncClient, request: TTSRequest) -> str:
    """Airtable 레코드 생성"""
    url = f"https://api.airtable.com/v0/{request.base_id}/{request.table_id}"
    headers = {
        "Authorization": f"Bearer {request.airtable_api_key}",
        "Content-Type": "application/json"
    }
    
    record_data = {
        "fields": {
            "user_id": request.id,
            "user_pwd": request.pwd,
            "category": request.category,
            "order": request.order,
            "timezone": request.timezone,
            "status": "running",
            "chat_id": request.chat_id,
            "session_id": request.session_id,
            "start_date": datetime.datetime.utcnow().isoformat()
        }
    }
    
    try:
        response = await client.post(
            url,
            headers=headers,
            json={
                "records": [record_data]
            }
        )
        response.raise_for_status()
        result = response.json()
        return result["records"][0]["id"]
    except httpx.HTTPError as e:
        raise HTTPException(
            status_code=e.response.status_code if hasattr(e, 'response') else 500,
            detail=f"Airtable record creation failed: {str(e)}"
        )

async def update_airtable_record(client: httpx.AsyncClient, request: TTSRequest, record_id: str, update_data: dict):
    """Airtable 레코드 업데이트"""
    url = f"https://api.airtable.com/v0/{request.base_id}/{request.table_id}/{record_id}"
    headers = {
        "Authorization": f"Bearer {request.airtable_api_key}",
        "Content-Type": "application/json"
    }
    
    try:
        response = await client.patch(
            url,
            headers=headers,
            json={"fields": update_data}
        )
        response.raise_for_status()
        return response.json()
    except httpx.HTTPError as e:
        print(f"Error updating Airtable record: {str(e)}")
        if hasattr(e, 'response'):
            print(f"Response content: {e.response.content}")
        raise

async def call_buddy_api(client: httpx.AsyncClient, flowise_id: str, order: str) -> dict:
    """Buddy API 호출"""
    api_url = f"https://ai.linkbricks.com/api/v1/prediction/{flowise_id}"
    
    try:
        response = await client.post(
            api_url,
            json={"question": order}
        )
        response.raise_for_status()
        return response.json()
    except httpx.HTTPError as e:
        raise HTTPException(
            status_code=e.response.status_code if hasattr(e, 'response') else 500,
            detail=f"Buddy API call failed: {str(e)}"
        )

@app.post("/assign_buddy_work/")
async def assign_buddy_work(request: TTSRequest, background_tasks: BackgroundTasks):
    """작업 할당 엔드포인트"""
    # 인증 키 확인
    if request.auth_key != REQUIRED_AUTH_KEY:
        raise HTTPException(status_code=403, detail="Invalid authentication key")

    try:
        # Airtable 레코드 생성 (타임아웃 10초)
        async with httpx.AsyncClient(timeout=10.0) as temp_client:
            record_id = await create_airtable_record(temp_client, request)

        # 백그라운드 작업 스케줄링
        background_tasks.add_task(
            process_buddy_work_background,
            app.state.client,
            request,
            record_id
        )

        # 즉시 응답
        return {
            "message": "Successfully assigned Buddy Work",
            "record_id": record_id,
            "status": "running"
        }

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to assign work: {str(e)}"
        )

async def process_buddy_work_background(
    client: httpx.AsyncClient,
    request: TTSRequest,
    record_id: str
):
    """백그라운드 작업 처리"""
    try:
        # Buddy API 호출
        buddy_result = await call_buddy_api(client, request.flowise_id, request.order)
        result_text = buddy_result.get("text", "No result text available")

        # 성공 상태로 업데이트
        update_data = {
            "status": "finished",
            "result": result_text,
            "end_date": datetime.datetime.utcnow().isoformat()
        }
    except Exception as error:
        # 실패 상태로 업데이트
        update_data = {
            "status": "failed",
            "result": f"Error: {str(error)}",
            "end_date": datetime.datetime.utcnow().isoformat()
        }

    try:
        # Airtable 레코드 업데이트
        await update_airtable_record(client, request, record_id, update_data)
    except Exception as update_error:
        print(f"Failed to update Airtable record: {str(update_error)}")

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
