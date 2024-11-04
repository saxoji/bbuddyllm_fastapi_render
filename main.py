import uvicorn
from fastapi import FastAPI, HTTPException, BackgroundTasks
from pydantic import BaseModel
import httpx
import asyncio
import datetime
from typing import Optional

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

# Async HTTP client
async_client = httpx.AsyncClient(timeout=30.0)

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
    
    try:
        async with async_client as client:
            response = await client.patch(url, json={"fields": update_data}, headers=headers)
            response.raise_for_status()
            return response.json()
    except httpx.HTTPError as e:
        raise HTTPException(status_code=response.status_code, detail=f"Failed to update record: {str(e)}")

async def call_buddy_api(flowise_id: str, order: str) -> dict:
    api_url = f"https://ai.linkbricks.com/api/v1/prediction/{flowise_id}"
    
    try:
        async with async_client as client:
            response = await client.post(api_url, json={"question": order})
            response.raise_for_status()
            return response.json()
    except httpx.HTTPError as e:
        raise HTTPException(status_code=500, detail=f"Buddy API call failed: {str(e)}")

@app.post("/assign_buddy_work/")
async def assign_buddy_work(request: TTSRequest, background_tasks: BackgroundTasks):
    # Auth key check
    if request.auth_key != REQUIRED_AUTH_KEY:
        raise HTTPException(status_code=403, detail="Invalid authentication key")

    # Prepare Airtable record data
    initial_record = {
        "records": [{
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
        }]
    }

    # Create Airtable record with async client
    try:
        url = f"https://api.airtable.com/v0/{request.base_id}/{request.table_id}"
        headers = {
            "Authorization": f"Bearer {request.airtable_api_key}",
            "Content-Type": "application/json"
        }
        
        async with async_client as client:
            response = await client.post(url, json=initial_record, headers=headers)
            response.raise_for_status()
            data = response.json()
            record_id = data['records'][0]['id']
    except httpx.HTTPError as e:
        raise HTTPException(status_code=500, detail=f"Failed to create record: {str(e)}")

    # Schedule background task with error handling
    background_tasks.add_task(
        process_buddy_work_background,
        request=request,
        record_id=record_id
    )

    # Return immediately with success message and record ID
    return {
        "message": "Successfully assigned Buddy Work",
        "record_id": record_id,
        "status": "running"
    }

async def process_buddy_work_background(request: TTSRequest, record_id: str):
    try:
        # Call Buddy API with timeout
        buddy_result = await asyncio.wait_for(
            call_buddy_api(request.flowise_id, request.order),
            timeout=60.0  # 60 seconds timeout
        )
        
        result_text = buddy_result.get("text", "No result text available")
        
        # Update Airtable with success status
        update_data = {
            "status": "finished",
            "result": result_text,
            "end_date": datetime.datetime.utcnow().isoformat()
        }
        
    except asyncio.TimeoutError:
        update_data = {
            "status": "timeout",
            "result": "Processing took too long and timed out",
            "end_date": datetime.datetime.utcnow().isoformat()
        }
        
    except Exception as error:
        update_data = {
            "status": "failed",
            "result": f"Buddy work failed: {str(error)}",
            "end_date": datetime.datetime.utcnow().isoformat()
        }
    
    finally:
        # Ensure we always try to update the record status
        try:
            await update_airtable_record(
                request.base_id,
                request.table_id,
                request.airtable_api_key,
                record_id,
                update_data
            )
        except Exception as e:
            print(f"Failed to update final status: {str(e)}")

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
