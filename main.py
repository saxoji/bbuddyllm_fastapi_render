import uvicorn
from fastapi import FastAPI, HTTPException, BackgroundTasks
from pydantic import BaseModel
import httpx
import datetime
from typing import Optional
import asyncio
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
    app.state.client = httpx.AsyncClient(timeout=None)  # 전역 타임아웃을 None으로 설정
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

async def update_airtable_record(client: httpx.AsyncClient, base_id: str, table_id: str, 
                                api_key: str, record_id: str, update_data: dict) -> dict:
    url = f"https://api.airtable.com/v0/{base_id}/{table_id}/{record_id}"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }
    
    async with httpx.AsyncClient(timeout=10.0) as temp_client:  # Airtable 업데이트용 임시 클라이언트
        response = await temp_client.patch(url, json={"fields": update_data}, headers=headers)
        if not response.is_success:
            raise HTTPException(
                status_code=response.status_code, 
                detail=f"Failed to update record: {response.text}"
            )
        return response.json()

async def call_buddy_api(client: httpx.AsyncClient, flowise_id: str, order: str) -> dict:
    api_url = f"https://ai.linkbricks.com/api/v1/prediction/{flowise_id}"
    
    # timeout 없이 LLM API 호출
    response = await client.post(api_url, json={"question": order})
    if not response.is_success:
        raise HTTPException(
            status_code=response.status_code, 
            detail=f"Buddy API call failed: {response.text}"
        )
    return response.json()

@app.post("/assign_buddy_work/")
async def assign_buddy_work(request: TTSRequest, background_tasks: BackgroundTasks):
    if request.auth_key != REQUIRED_AUTH_KEY:
        raise HTTPException(status_code=403, detail="Invalid authentication key")

    # Airtable 레코드 생성을 위한 임시 클라이언트 사용
    async with httpx.AsyncClient(timeout=10.0) as temp_client:
        url = f"https://api.airtable.com/v0/{request.base_id}/{request.table_id}"
        headers = {
            "Authorization": f"Bearer {request.airtable_api_key}",
            "Content-Type": "application/json"
        }
        body = {
            "records": [
                {
                    "fields": {
                        "user_id": request.id,
                        "user_pwd": request.pwd,
                        "category": request.category,
                        "order": request.order,
                        "timezone": int(request.timezone),
                        "status": "running",
                        "chat_id": request.chat_id,
                        "session_id": request.session_id,
                        "start_date": datetime.datetime.utcnow().isoformat()
                    }
                }
            ]
        }

        try:
            response = await temp_client.post(url, json=body, headers=headers)
            if not response.is_success:
                raise HTTPException(
                    status_code=response.status_code, 
                    detail=f"Failed to create record: {response.text}"
                )
            
            data = response.json()
            record_id = data['records'][0]['id']

            # 백그라운드 작업 즉시 스케줄링
            background_tasks.add_task(
                process_buddy_work_background, 
                app.state.client,  # timeout이 없는 메인 클라이언트 사용
                request, 
                record_id
            )

            # 즉시 응답 반환
            return {"message": "Successfully assigned Buddy Work", "record_id": record_id}

        except Exception as e:
            raise HTTPException(
                status_code=500, 
                detail=f"Failed to process request: {str(e)}"
            )

async def process_buddy_work_background(
    client: httpx.AsyncClient,
    request: TTSRequest,
    record_id: str
):
    try:
        # timeout 없이 Buddy API 호출
        buddy_result = await call_buddy_api(client, request.flowise_id, request.order)
        result_text = buddy_result.get("text", "No result text available")

        # 성공 상태로 Airtable 레코드 업데이트
        update_data = {
            "status": "finished",
            "result": result_text,
            "end_date": datetime.datetime.utcnow().isoformat()
        }
        
        await update_airtable_record(
            client,
            request.base_id,
            request.table_id,
            request.airtable_api_key,
            record_id,
            update_data
        )

    except Exception as error:
        # 실패 상태로 Airtable 레코드 업데이트
        update_data = {
            "status": "failed",
            "result": f"Buddy work failed: {str(error)}",
            "end_date": datetime.datetime.utcnow().isoformat()
        }
        try:
            await update_airtable_record(
                client,
                request.base_id,
                request.table_id,
                request.airtable_api_key,
                record_id,
                update_data
            )
        except Exception as update_error:
            print(f"Failed to update error status: {update_error}")

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
