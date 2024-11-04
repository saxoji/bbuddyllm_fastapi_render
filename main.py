import uvicorn
from fastapi import FastAPI, HTTPException, BackgroundTasks
from pydantic import BaseModel
import httpx
import datetime
from typing import Optional
import asyncio
import logging
from contextlib import asynccontextmanager

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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

app = FastAPI(**SWAGGER_HEADERS)

# Airtable API 호출용 타임아웃 설정
AIRTABLE_TIMEOUT = 30.0  # 30초
MAX_RETRIES = 3

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

@asynccontextmanager
async def get_airtable_client():
    """Airtable API 호출용 HTTP 클라이언트"""
    async with httpx.AsyncClient(timeout=AIRTABLE_TIMEOUT) as client:
        yield client

@asynccontextmanager
async def get_buddy_client():
    """Buddy API 호출용 HTTP 클라이언트 (타임아웃 없음)"""
    async with httpx.AsyncClient(timeout=None) as client:
        yield client

async def update_airtable_record(base_id: str, table_id: str, api_key: str, record_id: str, update_data: dict) -> dict:
    url = f"https://api.airtable.com/v0/{base_id}/{table_id}/{record_id}"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }
    
    for attempt in range(MAX_RETRIES):
        try:
            async with get_airtable_client() as client:
                response = await client.patch(url, json={"fields": update_data}, headers=headers)
                if response.is_success:
                    return response.json()
                
                if response.status_code >= 500:  # 서버 에러면 재시도
                    if attempt < MAX_RETRIES - 1:
                        await asyncio.sleep(2 ** attempt)
                        continue
                
                raise HTTPException(status_code=response.status_code, 
                                 detail=f"Failed to update record: {response.text}")
        
        except httpx.TimeoutException:
            if attempt < MAX_RETRIES - 1:
                await asyncio.sleep(2 ** attempt)
                continue
            raise HTTPException(status_code=504, detail="Timeout while updating Airtable record")
        
        except Exception as e:
            logger.error(f"Error updating Airtable record: {str(e)}")
            raise HTTPException(status_code=500, detail=f"Failed to update record: {str(e)}")

async def call_buddy_api(flowise_id: str, order: str) -> dict:
    api_url = f"https://ai.linkbricks.com/api/v1/prediction/{flowise_id}"
    
    try:
        async with get_buddy_client() as client:
            logger.info(f"Starting Buddy API call for flowise_id: {flowise_id}")
            response = await client.post(api_url, json={"question": order})
            
            if response.is_success:
                logger.info(f"Buddy API call successful for flowise_id: {flowise_id}")
                return response.json()
            
            logger.error(f"Buddy API call failed with status {response.status_code}: {response.text}")
            raise HTTPException(status_code=response.status_code,
                             detail=f"Buddy API call failed: {response.text}")
    
    except Exception as e:
        logger.error(f"Error calling Buddy API: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Buddy API call failed: {str(e)}")

async def create_airtable_record(request: TTSRequest) -> tuple[str, dict]:
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
        async with get_airtable_client() as client:
            response = await client.post(url, json=body, headers=headers)
            if response.is_success:
                data = response.json()
                return data['records'][0]['id'], body
            raise HTTPException(status_code=response.status_code,
                             detail=f"Failed to create record: {response.text}")
    
    except Exception as e:
        logger.error(f"Error creating Airtable record: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to create record: {str(e)}")

@app.post("/assign_buddy_work/")
async def assign_buddy_work(request: TTSRequest, background_tasks: BackgroundTasks):
    if request.auth_key != REQUIRED_AUTH_KEY:
        raise HTTPException(status_code=403, detail="Invalid authentication key")

    try:
        record_id, _ = await create_airtable_record(request)
        
        logger.info(f"Starting background task for record {record_id}")
        background_tasks.add_task(process_buddy_work_background, request, record_id)
        
        return {
            "message": "Successfully assigned Buddy Work",
            "record_id": record_id,
            "status": "running"
        }
    
    except Exception as e:
        logger.error(f"Failed to initiate task: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to initiate task: {str(e)}")

async def process_buddy_work_background(request: TTSRequest, record_id: str):
    try:
        logger.info(f"Processing buddy work for record {record_id}")
        
        # Buddy API 호출 - 타임아웃 없음
        buddy_result = await call_buddy_api(request.flowise_id, request.order)
        result_text = buddy_result.get("text", "No result text available")
        
        logger.info(f"Buddy API call successful for record {record_id}")

        # Airtable 레코드 업데이트
        update_data = {
            "status": "finished",
            "result": result_text,
            "end_date": datetime.datetime.utcnow().isoformat()
        }
        
        await update_airtable_record(
            request.base_id,
            request.table_id,
            request.airtable_api_key,
            record_id,
            update_data
        )
        
        logger.info(f"Successfully completed buddy work for record {record_id}")

    except Exception as error:
        logger.error(f"Error in background task for record {record_id}: {str(error)}")
        
        try:
            # 실패 상태 업데이트
            update_data = {
                "status": "failed",
                "result": f"Processing failed: {str(error)}",
                "end_date": datetime.datetime.utcnow().isoformat()
            }
            
            await update_airtable_record(
                request.base_id,
                request.table_id,
                request.airtable_api_key,
                record_id,
                update_data
            )
        except Exception as update_error:
            logger.error(f"Failed to update error status for record {record_id}: {str(update_error)}")

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
