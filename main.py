import os
import subprocess
import time
from typing import List, Optional

# from dotenv import load_dotenv
# load_dotenv()
packages = [
    'pandas',
    'numpy',
    'psycopg2-binary',
    'apscheduler',
    'boto3',
    'fastapi',
    'uvicorn',
    # 'redis',
]
flag_file = '/app/packages_installed.flag'
if not os.path.exists(flag_file):
    for package in packages:
        try:
            subprocess.check_call(['pip', 'install', package])
            print(f"Package {package} has been successfully installed.")
        except subprocess.CalledProcessError as e:
            print(f"An error occurred while installing {package}: {e}")
    os.makedirs('/app', exist_ok=True)
    with open(flag_file, 'w') as f:
        f.write('Packages installed successfully')
    print("All packages have been installed.")

import uvicorn
from fastapi import FastAPI
from pydantic import BaseModel
from generate_subject_response.index import handler as generate_subject_response_handler
from markpassive.index import handler as mark_passive_handler
from participant_phase.index import handler as subject_phase_handler
from upload_study.index import handler
from study_data_sync.index import handler as study_data_sync_handler
from apscheduler.schedulers.asyncio import AsyncIOScheduler


app = FastAPI()
scheduler = AsyncIOScheduler()


@app.get("/")
def read_root():
    return {"Hello": "World"}


class Item(BaseModel):
    fileName: str
    user: str


@app.post("/upload_participants")
def read_item(item: Item):
    print('--------------------------upload_participants_starting----------------------------------')
    study_list = handler({"user": item.user, "fileName": item.fileName}, None)
    print('study_id_list: ', study_list)
    print('-------------------------- upload_participants_end ----------------------------------')


class Item21(BaseModel):
    studyIds: Optional[List[int]] = None
    studyId: Optional[int] = None
    draftId: Optional[int] = None
    entityId: Optional[int] = None
    entityType: Optional[str] = None
    user: Optional[str] = None


@app.post("/generate_subject_response")
def read_item21(item: Item21):
    print('--------------------------generate_subject_response_starting----------------------------------')
    print(str({"studyIds": item.studyIds, "studyId": item.studyId, "draftId": item.draftId, "entityId": item.entityId,
               "entityType": item.entityType, "user": item.user}))
    body = {"studyIds": item.studyIds, "studyId": item.studyId, "draftId": item.draftId, "entityId": item.entityId,
            "entityType": item.entityType, "user": item.user}
    generate_subject_response_handler(body, None)
    print('--------------------------generate_subject_response_end----------------------------------')


class Item3(BaseModel):
    entityId: int
    entityType: str
    user: str


@app.post("/mark_passive")
def read_item4(item: Item3):
    print('--------------------------mark_passive_starting----------------------------------')
    mark_passive_handler({"entityId": item.entityId, "entityType": item.entityType, "user": item.user}, None)
    print('--------------------------mark_passive_end----------------------------------')


class Item4(BaseModel):
    studyId: Optional[int] = None
    user: Optional[str] = None
    type: Optional[str] = None


@app.post("/study_data_sync")
def read_item4(item: Item4):
    print('--------------------------study_data_sync_starting----------------------------------')
    req = {'studyId': item.studyId, 'user': item.user, 'type': item.type}
    print(str(req))
    time.sleep(6)
    study_data_sync_handler(req, None)
    print('--------------------------study_data_sync_end----------------------------------')


class Item5(BaseModel):
    subjectId: Optional[int] = None


@app.post("/subject_phase_status")
def read_item5(item: Item5):
    print('--------------------------subject_phase_status_starting----------------------------------')
    req = {'subjectId': item.subjectId}
    print(str(req))
    subject_phase_handler(req, None)
    print('--------------------------subject_phase_status_end----------------------------------')


@scheduler.scheduled_job('cron', minute=0)
async def cron_job():
    print('--------------------------job:subject_phase_status_starting----------------------------------')
    subject_phase_handler(None, None)
    print('--------------------------job:subject_phase_status_end----------------------------------')


@app.on_event("startup")
async def startup_event():
    print('--------------------------job:scheduler.startup----------------------------------')
    scheduler.start()


@app.on_event("shutdown")
async def shutdown_event():
    print('--------------------------job:scheduler.shutdown----------------------------------')
    scheduler.shutdown()


if __name__ == "__main__":
    config = uvicorn.Config("main:app", port=80, log_level="debug", host="0.0.0.0")
    server = uvicorn.Server(config)
    server.run()
