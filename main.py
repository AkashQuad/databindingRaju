from fastapi import FastAPI, HTTPException, Body
from azure.storage.blob import BlobServiceClient
import pandas as pd
import requests
import os
from dotenv import load_dotenv
 
load_dotenv()
 
POWERBI_API = "https://api.powerbi.com/v1.0/myorg"
 
# Azure AD
TENANT_ID = os.getenv("TENANT_ID")
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
 
# Power BI
TEMPLATE_WORKSPACE_ID = os.getenv("TEMPLATE_WORKSPACE_ID")
TEMPLATE_REPORT_ID = os.getenv("TEMPLATE_REPORT_ID")
TARGET_WORKSPACE_ID = os.getenv("TARGET_WORKSPACE_ID")
 
# Blob
AZURE_STORAGE_CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
 
DATASET_NAME = "Migrated_Dataset"
TABLE_NAME = "Migrated_Table"
 
app = FastAPI(title="Tableau to Power BI Migration")
 
# --------------------------------------------------
# TOKEN
# --------------------------------------------------
def get_token():
    r = requests.post(
        f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token",
        data={
            "grant_type": "client_credentials",
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET,
            "scope": "https://analysis.windows.net/powerbi/api/.default"
        }
    )
    r.raise_for_status()
    return r.json()["access_token"]
 
# --------------------------------------------------
# READ BLOB DATA (DYNAMIC)
# --------------------------------------------------
def read_blob_data(container_name: str, folder_name: str) -> pd.DataFrame:
    service = BlobServiceClient.from_connection_string(
        AZURE_STORAGE_CONNECTION_STRING
    )
    container = service.get_container_client(container_name)
 
    dfs = []
    prefix = f"{folder_name}/"
 
    for blob in container.list_blobs(name_starts_with=prefix):
        stream = container.get_blob_client(blob.name).download_blob()
        if blob.name.endswith(".csv"):
            dfs.append(pd.read_csv(stream))
        elif blob.name.endswith((".xls", ".xlsx")):
            dfs.append(pd.read_excel(stream))
 
    if not dfs:
        raise HTTPException(404, "No files found")
 
    return pd.concat(dfs, ignore_index=True)
 
# --------------------------------------------------
# DATASET
# --------------------------------------------------
def create_dataset(token, df):
    payload = {
        "name": DATASET_NAME,
        "defaultMode": "Push",
        "tables": [{
            "name": TABLE_NAME,
            "columns": [{"name": c, "dataType": "string"} for c in df.columns]
        }]
    }
 
    r = requests.post(
        f"{POWERBI_API}/groups/{TARGET_WORKSPACE_ID}/datasets",
        headers={"Authorization": f"Bearer {token}"},
        json=payload
    )
    r.raise_for_status()
    return r.json()["id"]
 
def push_rows(token, dataset_id, df):
    r = requests.post(
        f"{POWERBI_API}/groups/{TARGET_WORKSPACE_ID}/datasets/{dataset_id}/tables/{TABLE_NAME}/rows",
        headers={"Authorization": f"Bearer {token}"},
        json={"rows": df.astype(str).to_dict(orient="records")}
    )
    r.raise_for_status()
 
# --------------------------------------------------
# CLONE REPORT
# --------------------------------------------------
def clone_report(token, dataset_id, report_name):
    r = requests.post(
        f"{POWERBI_API}/groups/{TEMPLATE_WORKSPACE_ID}/reports/{TEMPLATE_REPORT_ID}/Clone",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "name": report_name,
            "targetWorkspaceId": TARGET_WORKSPACE_ID,
            "targetModelId": dataset_id
        }
    )
    r.raise_for_status()
    return r.json()["id"]
 
# --------------------------------------------------
# API ENDPOINT (DYNAMIC INPUT)
# --------------------------------------------------
@app.post("/generate")
def generate(payload: dict = Body(...)):
    container_name = payload.get("container_name")
    folder_name = payload.get("folder_name")
    report_name = payload.get("report_name")
 
    if not all([container_name, folder_name, report_name]):
        raise HTTPException(400, "container_name, folder_name, report_name required")
 
    token = get_token()
    df = read_blob_data(container_name, folder_name)
    dataset_id = create_dataset(token, df)
    push_rows(token, dataset_id, df)
    report_id = clone_report(token, dataset_id, report_name)
 
    return {
        "datasetId": dataset_id,
        "reportId": report_id,
        "reportName": report_name
    }
 
 
