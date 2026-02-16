from fastapi import FastAPI, UploadFile, File, HTTPException
import pandas as pd
import requests
import json
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
from dotenv import load_dotenv
import os

load_dotenv()

app = FastAPI()

# Database connection
DATABASE_URL = URL.create(
    "mysql+pymysql",
    username=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
    host=os.getenv("DB_HOST"),
    port=int(os.getenv("DB_PORT", "3306")),
    database=os.getenv("DB_NAME"),
)
engine = create_engine(DATABASE_URL)

# API Configuration
API_URL = os.getenv("API_URL")
TOKEN = os.getenv("DVARA_TOKEN")


def create_table_if_not_exists(table_name):
    """Create table if it doesn't exist"""
    try:
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            client_id VARCHAR(50) PRIMARY KEY,
            full_name VARCHAR(255),
            phone_no VARCHAR(20),
            client_amount FLOAT,
            total_land FLOAT,
            year INT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """

        with engine.connect() as conn:
            conn.execute(text(create_table_query))
            conn.commit()

        print(f"✅ Table '{table_name}' ready")
    except Exception as e:
        print(f"⚠️ Could not create table: {e}")


def get_database_fields(table_name):
    """Fetch actual field names from your database table"""
    try:
        create_table_if_not_exists(table_name)

        with engine.connect() as conn:
            result = conn.execute(text(f"DESCRIBE {table_name}"))
            fields = [row[0] for row in result if row[0] != 'created_at']
            return fields
    except Exception as e:
        print(f"⚠️ Could not fetch database fields: {e}")
        return [
            "client_id",
            "full_name",
            "phone_no",
            "client_amount",
            "total_land",
            "year"
        ]


def call_llm(excel_columns, database_fields):
    """Call the LLM API for field mapping using form-data"""

    task_data = {
        "excel_columns": excel_columns,
        "database_fields": database_fields
    }

    task_json_string = json.dumps(task_data)

    print("📤 Sending to LLM (form-data):")
    print(f"   task = {task_json_string}")

    form_data = {
        "task": task_json_string
    }

    headers_for_form = {
        "Authorization": f"Bearer {TOKEN}"
    }

    try:
        response = requests.post(
            API_URL,
            headers=headers_for_form,
            data=form_data,
            timeout=30
        )

        if response.status_code == 403:
            raise HTTPException(status_code=403, detail="Token expired. Update DVARA_TOKEN in .env")

        response.raise_for_status()

        result = response.json()
        print("📥 Full API Response:", json.dumps(result, indent=2))

        if result.get("status") != "completed":
            error_msg = result.get("error", "Unknown error")
            raise HTTPException(status_code=500, detail=f"Workflow failed: {error_msg}")

        workflow_error = result.get("error")
        if workflow_error:
            raise HTTPException(status_code=500, detail=f"Workflow returned an error: {workflow_error}")

        mapping_data = result.get("result", {}).get("result", {})

        if "is_valid" in mapping_data:
            del mapping_data["is_valid"]

        if not mapping_data:
            raise HTTPException(status_code=500, detail="Empty mapping returned from LLM")

        return mapping_data

    except requests.exceptions.RequestException as e:
        raise HTTPException(status_code=500, detail=f"LLM API failed: {str(e)}")
    except json.JSONDecodeError as e:
        raise HTTPException(status_code=500, detail=f"Invalid JSON response: {str(e)}")


@app.post("/upload/")
async def upload_excel(
    file: UploadFile = File(...),
    table_name: str = "llm_mapping",
    insert_to_db: bool = False
):
    """Upload Excel and map fields automatically"""

    try:
        df = pd.read_excel(file.file)
        excel_columns = df.columns.tolist()

        print(f" Excel Columns: {excel_columns}")
        print(f" Total Rows: {len(df)}")

        database_fields = get_database_fields(table_name)
        print(f"🗄️ Database Fields: {database_fields}")

        mapping = call_llm(excel_columns, database_fields)

        # 🔒 FIXED COLUMN MAPPING (PRIORITY OVER LLM)
        FIXED_COLUMN_MAPPING = {
            "loaner_id": "client_id",
            "name": "full_name",
            "phone_no": "phone_no",
            "loan_amount": "client_amount",
            "total_land": "total_land",
            "year": "year"
        }

        final_mapping = {}

        for excel_col in df.columns:
            if excel_col in FIXED_COLUMN_MAPPING:
                final_mapping[excel_col] = FIXED_COLUMN_MAPPING[excel_col]
            elif excel_col in mapping:
                final_mapping[excel_col] = mapping[excel_col]

        df.rename(columns=final_mapping, inplace=True)

        allowed_columns = list(FIXED_COLUMN_MAPPING.values())
        df = df[[col for col in df.columns if col in allowed_columns]]

        rows_inserted = 0
        if insert_to_db:
            df.to_sql(table_name, engine, if_exists="append", index=False)
            rows_inserted = len(df)

        return {
            "status": "success",
            "original_columns": excel_columns,
            "database_fields": database_fields,
            "mapping": final_mapping,
            "renamed_columns": df.columns.tolist(),
            "total_rows": len(df),
            "rows_inserted": rows_inserted,
            "preview": df.head(5).to_dict("records")
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/")
def root():
    return {
        "status": "Field Mapper API is running",
        "version": "2.0",
        "endpoints": {
            "upload": "/upload/?table_name=your_table&insert_to_db=false",
            "health": "/health",
            "docs": "/docs"
        }
    }


@app.get("/health")
def health_check():
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))

        return {
            "database": "connected",
            "status": "healthy",
            "token_set": bool(TOKEN)
        }
    except Exception as e:
        return {"status": "unhealthy", "error": str(e)}
