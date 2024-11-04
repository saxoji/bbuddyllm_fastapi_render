import uvicorn
from fastapi import FastAPI, HTTPException, BackgroundTasks
from pydantic import BaseModel
import httpx
import datetime
from typing import Optional, Dict, Any

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

async_client = httpx.AsyncClient()

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

class ApiResponse:
    @staticmethod
    def success(data: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "status": "success",
            "message": "Work assigned successfully",
            **data
        }

    @staticmethod
    def error(error_message: str, status_code: int = 500) -> Dict[str, Any]:
        return {
            "status": "error",
            "message": error_message,
            "code": status_code
        }

async def create_airtable_record(request: TTSRequest) -> str:
    """Airtable에 초기 레코드를 생성하고 record_id를 반환합니다."""
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

    url = f"https://api.airtable.com/v0/{request.base_id}/{request.table_id}"
    headers = {
        "Authorization": f"Bearer {request.airtable_api_key}",
        "Content-Type": "application/json"
    }

    try:
        response = await async_client.post(url, json=initial_record, headers=headers)
        response.raise_for_status()
        return response.json()['records'][0]['id']
    except httpx.HTTPStatusError as e:
        error_message = f"Airtable API error: {e.response.status_code} - {e.response.text}"
        raise HTTPException(status_code=e.response.status_code, detail=error_message)
    except httpx.RequestError as e:
        error_message = f"Network error while connecting to Airtable: {str(e)}"
        raise HTTPException(status_code=503, detail=error_message)
    except KeyError as e:
        error_message = "Invalid response format from Airtable"
        raise HTTPException(status_code=500, detail=error_message)
    except Exception as e:
        error_message = f"Unexpected error creating Airtable record: {str(e)}"
        raise HTTPException(status_code=500, detail=error_message)

async def update_airtable_record(base_id: str, table_id: str, api_key: str, record_id: str, update_data: dict):
    """Airtable 레코드를 업데이트합니다."""
    url = f"https://api.airtable.com/v0/{base_id}/{table_id}/{record_id}"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }
    
    try:
        response = await async_client.patch(url, json={"fields": update_data}, headers=headers)
        response.raise_for_status()
    except Exception as e:
        print(f"Failed to update record {record_id}: {str(e)}")

async def call_buddy_api(flowise_id: str, order: str) -> dict:
    """Buddy API를 호출하여 결과를 반환합니다."""
    api_url = f"https://ai.linkbricks.com/api/v1/prediction/{flowise_id}"
    
    try:
        response = await async_client.post(api_url, json={"question": order})
        response.raise_for_status()
        return response.json()
    except httpx.HTTPError as e:
        raise HTTPException(status_code=500, detail=f"Buddy API call failed: {str(e)}")

@app.post("/assign_buddy_work/")
async def assign_buddy_work(request: TTSRequest, background_tasks: BackgroundTasks):
    """
    작업을 할당하고 즉시 응답을 반환하는 메인 엔드포인트입니다.
    에러 발생 시 즉시 에러 응답을 반환합니다.
    """
    try:
        # 인증 키 확인
        if request.auth_key != REQUIRED_AUTH_KEY:
            return ApiResponse.error("Invalid authentication key", 403)

        # 필수 필드 검증
        if not all([request.base_id, request.table_id, request.airtable_api_key, request.flowise_id]):
            return ApiResponse.error("Missing required fields", 400)

        # Airtable에 초기 레코드 생성
        try:
            record_id = await create_airtable_record(request)
        except HTTPException as e:
            # Airtable 레코드 생성 실패 시 즉시 에러 반환
            return ApiResponse.error(f"Failed to create Airtable record: {e.detail}", e.status_code)

        # 백그라운드 작업 스케줄링
        try:
            background_tasks.add_task(
                process_buddy_work_background,
                request=request,
                record_id=record_id
            )
        except Exception as e:
            # 백그라운드 작업 스케줄링 실패 시 에러 반환
            return ApiResponse.error(f"Failed to schedule background task: {str(e)}", 500)

        # 성공 응답 반환
        return ApiResponse.success({
            "record_id": record_id
        })

    except Exception as e:
        # 예상치 못한 에러 발생 시 에러 반환
        return ApiResponse.error(f"Unexpected error: {str(e)}", 500)

async def process_buddy_work_background(request: TTSRequest, record_id: str):
    """백그라운드에서 실행될 실제 작업 처리 함수"""
    try:
        # Buddy API 호출
        buddy_result = await call_buddy_api(request.flowise_id, request.order)
        result_text = buddy_result.get("text", "No result text available")
        
        update_data = {
            "status": "finished",
            "result": result_text,
            "end_date": datetime.datetime.utcnow().isoformat()
        }
        
    except Exception as error:
        update_data = {
            "status": "failed",
            "result": f"Error: {str(error)}",
            "end_date": datetime.datetime.utcnow().isoformat()
        }
    
    finally:
        await update_airtable_record(
            request.base_id,
            request.table_id,
            request.airtable_api_key,
            record_id,
            update_data
        )

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
