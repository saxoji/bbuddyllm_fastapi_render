import uvicorn
from fastapi import FastAPI, HTTPException, BackgroundTasks
from pydantic import BaseModel
import httpx
import datetime
import json
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

AIRTABLE_TIMEOUT = 30.0
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
    async with httpx.AsyncClient(timeout=AIRTABLE_TIMEOUT) as client:
        yield client

@asynccontextmanager
async def get_buddy_client():
    async with httpx.AsyncClient(timeout=None) as client:
        yield client

async def update_airtable_record(base_id: str, table_id: str, api_key: str, record_id: str, result: str) -> dict:
    url = f"https://api.airtable.com/v0/{base_id}/{table_id}/{record_id}"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }
    
    # result 필드만 업데이트
    fields = {
        "result": result
    }
    
    for attempt in range(MAX_RETRIES):
        try:
            async with get_airtable_client() as client:
                response = await client.patch(url, json={"fields": fields}, headers=headers)
                if response.is_success:
                    return response.json()
                
                if response.status_code >= 500:
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

async def create_airtable_record(request: TTSRequest) -> str:
    url = f"https://api.airtable.com/v0/{request.base_id}/{request.table_id}"
    headers = {
        "Authorization": f"Bearer {request.airtable_api_key}",
        "Content-Type": "application/json"
    }
    
    # 초기 상태 정보를 result 필드에 JSON으로 저장
    initial_status = {
        "status": "running",
        "user_id": request.id,
        "user_pwd": request.pwd,
        "category": request.category,
        "order": request.order,
        "timezone": request.timezone,
        "chat_id": request.chat_id,
        "session_id": request.session_id,
        "timestamp": datetime.datetime.utcnow().isoformat()
    }
    
    body = {
        "records": [
            {
                "fields": {
                    "result": json.dumps(initial_status)
                }
            }
        ]
    }

    try:
        async with get_airtable_client() as client:
            response = await client.post(url, json=body, headers=headers)
            if response.is_success:
                data = response.json()
                return data['records'][0]['id']
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
        record_id = await create_airtable_record(request)
        
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
        
        buddy_result = await call_buddy_api(request.flowise_id, request.order)
        result_text = buddy_result.get("text", "No result text available")
        
        logger.info(f"Buddy API call successful for record {record_id}")

        # 결과 정보를 JSON으로 구성
        result_data = {
            "status": "finished",
            "result": result_text,
            "timestamp": datetime.datetime.utcnow().isoformat()
        }
        
        await update_airtable_record(
            request.base_id,
            request.table_id,
            request.airtable_api_key,
            record_id,
            json.dumps(result_data)
        )
        
        logger.info(f"Successfully completed buddy work for record {record_id}")

    except Exception as error:
        logger.error(f"Error in background task for record {record_id}: {str(error)}")
        
        try:
            # 에러 정보를 JSON으로 구성
            error_data = {
                "status": "failed",
                "error": str(error),
                "timestamp": datetime.datetime.utcnow().isoformat()
            }
            
            await update_airtable_record(
                request.base_id,
                request.table_id,
                request.airtable_api_key,
                record_id,
                json.dumps(error_data)
            )
        except Exception as update_error:
            logger.error(f"Failed to update error status for record {record_id}: {str(update_error)}")

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
