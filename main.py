import uvicorn
from fastapi import FastAPI, HTTPException, BackgroundTasks
from pydantic import BaseModel
import requests
import datetime
import logging
import time
import asyncio

# Logging configuration
logging.basicConfig(level=logging.INFO)

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

def update_airtable_record(base_id, table_id, api_key, record_id, update_data):
    url = f"https://api.airtable.com/v0/{base_id}/{table_id}/{record_id}"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }
    try:
        response = requests.patch(url, json={"fields": update_data}, headers=headers, timeout=10)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to update record: {e}")

def call_buddy_api(flowise_id, order, retries=3, delay=5):
    api_url = f"https://ai.linkbricks.com/api/v1/prediction/{flowise_id}"
    for attempt in range(retries):
        try:
            response = requests.post(api_url, json={"question": order})
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logging.error(f"Buddy API call failed (attempt {attempt + 1} of {retries}): {e}")
            if attempt < retries - 1:
                time.sleep(delay)
            else:
                return None

@app.post("/assign_buddy_work/")
async def assign_buddy_work(request: TTSRequest, background_tasks: BackgroundTasks):
    try:
        # Check the auth key
        if request.auth_key != REQUIRED_AUTH_KEY:
            logging.error("Invalid authentication key")
            raise HTTPException(status_code=403, detail="Invalid authentication key")

        # Create Airtable record
        url = f"https://api.airtable.com/v0/{request.base_id}/{request.table_id}"
        headers = {
            "Authorization": f"Bearer {request.airtable_api_key}",
            "Content-Type": "application/json"
        }
        body = {
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

        logging.info("Creating Airtable record...")
        response = requests.post(url, json=body, headers=headers, timeout=10)
        response.raise_for_status()

        data = response.json()
        record_id = data['id']

        logging.info("Successfully created Airtable record. Scheduling background task...")
        # Schedule the background task without waiting
        background_tasks.add_task(process_buddy_work_background, request, record_id)

        logging.info("Returning response to client...")
        # Return response to client immediately
        return {"message": "Successfully assigned Buddy Work"}
    except Exception as e:
        logging.error(f"Unexpected error occurred: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")

async def process_buddy_work_background(request: TTSRequest, record_id: str):
    try:
        # Call Buddy API
        logging.info("Calling Buddy API...")
        buddy_result = call_buddy_api(request.flowise_id, request.order)
        if buddy_result is None:
            raise Exception("Buddy API call failed")

        result_text = buddy_result.get("text", "No result text available")

        # Update Airtable record with success status
        update_data = {
            "status": "finished",
            "result": result_text,
            "end_date": datetime.datetime.utcnow().isoformat()
        }
        logging.info("Updating Airtable record with success status...")
        update_airtable_record(request.base_id, request.table_id, request.airtable_api_key, record_id, update_data)

    except Exception as error:
        # Update Airtable record with failure status
        logging.error(f"Error occurred: {error}")
        update_data = {
            "status": "failed",
            "result": f"Buddy work failed: {str(error)}",
            "end_date": datetime.datetime.utcnow().isoformat()
        }
        logging.info("Updating Airtable record with failure status...")
        update_airtable_record(request.base_id, request.table_id, request.airtable_api_key, record_id, update_data)

if __name__ == "__main__":
    port = int(os.getenv("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port, http='h11', keep_alive_timeout=5)
