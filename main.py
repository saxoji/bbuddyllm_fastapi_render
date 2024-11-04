import uvicorn
from fastapi import FastAPI, HTTPException, BackgroundTasks
from pydantic import BaseModel
import httpx
import datetime
from typing import Optional
import asyncio

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
http_client = httpx.AsyncClient()

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

async def update_airtable_record(base_id: str, table_id: str, api_key: str, record_id: str, update_data: dict) -> dict:
    url = f"https://api.airtable.com/v0/{base_id}/{table_id}/{record_id}"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }
    
    async with httpx.AsyncClient() as client:
        response = await client.patch(url, json={"fields": update_data}, headers=headers)
        if not response.is_success:
            raise HTTPException(status_code=response.status_code, detail=f"Failed to update record: {response.text}")
        return response.json()

async def call_buddy_api(flowise_id: str, order: str) -> dict:
    api_url = f"https://ai.linkbricks.com/api/v1/prediction/{flowise_id}"
    
    async with httpx.AsyncClient() as client:
        response = await client.post(api_url, json={"question": order})
        if response.is_success:
            return response.json()
        else:
            raise HTTPException(status_code=response.status_code, detail=f"Buddy API call failed: {response.text}")

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
                    "session_id": request.session_id
                }
            }
        ]
    }

    async with httpx.AsyncClient() as client:
        response = await client.post(url, json=body, headers=headers)
        if not response.is_success:
            raise HTTPException(status_code=response.status_code, detail=f"Failed to create record: {response.text}")
        
        data = response.json()
        return data['records'][0]['id'], body

@app.post("/assign_buddy_work/")
async def assign_buddy_work(request: TTSRequest, background_tasks: BackgroundTasks):
    # Validate auth key
    if request.auth_key != REQUIRED_AUTH_KEY:
        raise HTTPException(status_code=403, detail="Invalid authentication key")

    try:
        # Create record immediately and get ID
        record_id, _ = await create_airtable_record(request)
        
        # Add background task and return immediately
        background_tasks.add_task(process_buddy_work_background, request, record_id)
        
        return {
            "message": "Successfully assigned Buddy Work",
            "record_id": record_id,
            "status": "running"
        }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to initiate task: {str(e)}")

async def process_buddy_work_background(request: TTSRequest, record_id: str):
    try:
        # Call Buddy API
        buddy_result = await call_buddy_api(request.flowise_id, request.order)
        result_text = buddy_result.get("text", "No result text available")

        # Update Airtable record with success status
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

    except Exception as error:
        # Update Airtable record with failure status
        update_data = {
            "status": "failed",
            "result": f"Buddy work failed: {str(error)}",
            "end_date": datetime.datetime.utcnow().isoformat()
        }
        await update_airtable_record(
            request.base_id,
            request.table_id,
            request.airtable_api_key,
            record_id,
            update_data
        )

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
