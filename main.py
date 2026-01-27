from fastapi import FastAPI, HTTPException, Body
from fastapi.middleware.cors import CORSMiddleware
from azure.storage.blob import BlobServiceClient
import pandas as pd
import requests
import os
from dotenv import load_dotenv
 
# --------------------------------------------------
# LOAD ENV
# --------------------------------------------------
load_dotenv()
 
POWERBI_API = "https://api.powerbi.com/v1.0/myorg"
 
TENANT_ID = os.getenv("TENANT_ID")
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
 
TEMPLATE_WORKSPACE_ID = os.getenv("TEMPLATE_WORKSPACE_ID")
TEMPLATE_REPORT_ID = os.getenv("TEMPLATE_REPORT_ID")
 
AZURE_STORAGE_CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
 
DATASET_NAME = "Migrated_Dataset"
 
# --------------------------------------------------
# APP
# --------------------------------------------------
app = FastAPI(title="Blob → Power BI Multi-Table Push")
 
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
 
# --------------------------------------------------
# AUTH TOKEN
# --------------------------------------------------
def get_token() -> str:
    r = requests.post(
        f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token",
        data={
            "grant_type": "client_credentials",
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET,
            "scope": "https://analysis.windows.net/powerbi/api/.default",
        },
        timeout=30,
    )
    r.raise_for_status()
    return r.json()["access_token"]
 
# --------------------------------------------------
# READ BLOB FILES → MULTIPLE TABLES
# --------------------------------------------------
def read_blob_tables(container_name: str, folder_name: str) -> dict:
    service = BlobServiceClient.from_connection_string(
        AZURE_STORAGE_CONNECTION_STRING
    )
    container = service.get_container_client(container_name)
 
    tables = {}
    prefix = f"{folder_name}/"
 
    for blob in container.list_blobs(name_starts_with=prefix):
        file_name = os.path.basename(blob.name)
        table_name = os.path.splitext(file_name)[0]
 
        blob_client = container.get_blob_client(blob.name)
        stream = blob_client.download_blob()
 
        if file_name.endswith(".csv"):
            df = pd.read_csv(stream)
        elif file_name.endswith((".xls", ".xlsx")):
            df = pd.read_excel(stream)
        else:
            continue
 
        tables[table_name] = df
 
    if not tables:
        raise HTTPException(status_code=404, detail="No valid files found")
 
    return tables
 
# --------------------------------------------------
# CREATE MULTI-TABLE PUSH DATASET
# --------------------------------------------------
def create_dataset(token: str, workspace_id: str, tables: dict) -> str:
    payload = {
        "name": DATASET_NAME,
        "defaultMode": "Push",
        "tables": []
    }
 
    for table_name, df in tables.items():
        payload["tables"].append({
            "name": table_name,
            "columns": [
                {"name": col, "dataType": "string"} for col in df.columns
            ],
        })
 
    r = requests.post(
        f"{POWERBI_API}/groups/{workspace_id}/datasets",
        headers={"Authorization": f"Bearer {token}"},
        json=payload,
        timeout=30,
    )
    r.raise_for_status()
    return r.json()["id"]
 
# --------------------------------------------------
# PUSH ROWS (TABLE-BY-TABLE) ✅ FIXED
# --------------------------------------------------
def push_rows(
    token: str,
    workspace_id: str,
    dataset_id: str,
    table_name: str,
    df: pd.DataFrame,
):
    # ✅ FIX: Replace NaN / None with empty string
    df = df.where(pd.notnull(df), "")
 
    rows = df.to_dict(orient="records")
 
    r = requests.post(
        f"{POWERBI_API}/groups/{workspace_id}/datasets/{dataset_id}/tables/{table_name}/rows",
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
        json={"rows": rows},
        timeout=30,
    )
    r.raise_for_status()
 
# --------------------------------------------------
# CLONE REPORT
# --------------------------------------------------
def clone_report(
    token: str,
    target_workspace_id: str,
    dataset_id: str,
    report_name: str,
) -> str:
    r = requests.post(
        f"{POWERBI_API}/groups/{TEMPLATE_WORKSPACE_ID}/reports/{TEMPLATE_REPORT_ID}/Clone",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "name": report_name,
            "targetWorkspaceId": target_workspace_id,
            "targetModelId": dataset_id,
        },
        timeout=30,
    )
    r.raise_for_status()
    return r.json()["id"]
 
# --------------------------------------------------
# MAIN API ENDPOINT
# --------------------------------------------------
@app.post("/generate")
def generate(payload: dict = Body(...)):
    container_name = payload.get("container_name")
    folder_name = payload.get("folder_name")
    report_name = payload.get("report_name")
    target_workspace_id = payload.get("target_workspace_id")
 
    if not all([container_name, folder_name, report_name, target_workspace_id]):
        raise HTTPException(
            status_code=400,
            detail="container_name, folder_name, report_name, target_workspace_id required",
        )
 
    token = get_token()
 
    tables = read_blob_tables(container_name, folder_name)
 
    dataset_id = create_dataset(token, target_workspace_id, tables)
 
    for table_name, df in tables.items():
        push_rows(
            token,
            target_workspace_id,
            dataset_id,
            table_name,
            df,
        )
 
    report_id = clone_report(
        token,
        target_workspace_id,
        dataset_id,
        report_name,
    )
 
    return {
        "datasetId": dataset_id,
        "reportId": report_id,
        "tables": list(tables.keys()),
        "workspaceId": target_workspace_id,
    }
 
# --------------------------------------------------
# HEALTH CHECK
# --------------------------------------------------
@app.get("/health")
def health():
    return {"status": "ok"}
 
 
