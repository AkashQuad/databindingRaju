from fastapi import FastAPI, HTTPException, Body
from azure.storage.blob import BlobServiceClient
import pandas as pd
import requests
import os
from dotenv import load_dotenv
from fastapi.middleware.cors import CORSMiddleware

# --------------------------------------------------
# LOAD ENV
# --------------------------------------------------
load_dotenv()

POWERBI_API = "https://api.powerbi.com/v1.0/myorg"

# Azure AD (STATIC – ENV)
TENANT_ID = os.getenv("TENANT_ID")
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")

# Power BI Template (STATIC – ENV)
TEMPLATE_WORKSPACE_ID = os.getenv("TEMPLATE_WORKSPACE_ID")
TEMPLATE_REPORT_ID = os.getenv("TEMPLATE_REPORT_ID")

# Azure Blob (STATIC – ENV)
AZURE_STORAGE_CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")

# Constants
DATASET_NAME = "Migrated_Dataset"
TABLE_NAME = "MainTable"

# --------------------------------------------------
# APP
# --------------------------------------------------
app = FastAPI(title="Tableau to Power BI Migration")
# --------------------------------------------------
# CORS (ALLOW ALL)
# --------------------------------------------------
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],      # Allow all origins
    allow_credentials=True,
    allow_methods=["*"],      # Allow all HTTP methods
    allow_headers=["*"],      # Allow all headers
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
# READ BLOB DATA
# --------------------------------------------------
def read_blob_data(container_name: str, folder_name: str) -> pd.DataFrame:
    service = BlobServiceClient.from_connection_string(
        AZURE_STORAGE_CONNECTION_STRING
    )
    container = service.get_container_client(container_name)

    dfs = []
    prefix = f"{folder_name}/"

    for blob in container.list_blobs(name_starts_with=prefix):
        blob_client = container.get_blob_client(blob.name)
        stream = blob_client.download_blob()

        if blob.name.endswith(".csv"):
            dfs.append(pd.read_csv(stream))
        elif blob.name.endswith((".xls", ".xlsx")):
            dfs.append(pd.read_excel(stream))

    if not dfs:
        raise HTTPException(status_code=404, detail="No files found in blob folder")

    return pd.concat(dfs, ignore_index=True)

# --------------------------------------------------
# CREATE DATASET
# --------------------------------------------------
def create_dataset(token: str, target_workspace_id: str, df: pd.DataFrame) -> str:
    payload = {
        "name": DATASET_NAME,
        "defaultMode": "Push",
        "tables": [
            {
                "name": TABLE_NAME,
                "columns": [
                    {"name": col, "dataType": "string"} for col in df.columns
                ],
            }
        ],
    }

    r = requests.post(
        f"{POWERBI_API}/groups/{target_workspace_id}/datasets",
        headers={"Authorization": f"Bearer {token}"},
        json=payload,
        timeout=30,
    )
    r.raise_for_status()
    return r.json()["id"]

# --------------------------------------------------
# PUSH ROWS
# --------------------------------------------------
def push_rows(
    token: str, target_workspace_id: str, dataset_id: str, df: pd.DataFrame
):
    r = requests.post(
        f"{POWERBI_API}/groups/{target_workspace_id}/datasets/{dataset_id}/tables/{TABLE_NAME}/rows",
        headers={"Authorization": f"Bearer {token}"},
        json={"rows": df.astype(str).to_dict(orient="records")},
        timeout=30,
    )
    r.raise_for_status()

# --------------------------------------------------
# CLONE REPORT
# --------------------------------------------------
def clone_report(
    token: str, target_workspace_id: str, dataset_id: str, report_name: str
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
# API ENDPOINT
# --------------------------------------------------
@app.post("/generate")
def generate(payload: dict = Body(...)):
    container_name = payload.get("container_name")
    folder_name = payload.get("folder_name")
    report_name = payload.get("report_name")
    target_workspace_id = payload.get("target_workspace_id")

    if not all(
        [container_name, folder_name, report_name, target_workspace_id]
    ):
        raise HTTPException(
            status_code=400,
            detail="container_name, folder_name, report_name, target_workspace_id are required",
        )

    token = get_token()
    df = read_blob_data(container_name, folder_name)

    dataset_id = create_dataset(token, target_workspace_id, df)
    push_rows(token, target_workspace_id, dataset_id, df)

    report_id = clone_report(
        token,
        target_workspace_id,
        dataset_id,
        report_name,
    )

    return {
        "datasetId": dataset_id,
        "reportId": report_id,
        "reportName": report_name,
        "targetWorkspaceId": target_workspace_id,
    }

# --------------------------------------------------
# HEALTH CHECK (IMPORTANT FOR AZURE)
# --------------------------------------------------
@app.get("/health")
def health():
    return {"status": "ok"}
