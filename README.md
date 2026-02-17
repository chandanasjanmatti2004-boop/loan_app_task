

#  Field Mapper API (FastAPI + LLM + MySQL)

##  Overview

This project is a **FastAPI-based backend service** that allows users to **upload Excel files**, automatically **map Excel columns to database fields using an LLM**, and optionally **insert the cleaned and mapped data into a MySQL database**.

The system supports:

* Automatic table creation
* Secure table name validation
* Intelligent column mapping using an external LLM API
* Duplicate prevention and data cleaning
* Safe database insertion

---

##  Features

*  Upload Excel files (`.xlsx`)
*  Automatic field mapping using LLM
*  Secure table name validation
*  MySQL integration via SQLAlchemy
*  Idempotent inserts (avoids duplicate primary keys)
*  Data preview before insertion
*  Health check endpoint

---

## Tech Stack

* **Backend Framework:** FastAPI
* **Database:** MySQL
* **ORM:** SQLAlchemy
* **Data Processing:** Pandas
* **LLM Integration:** External API via HTTP (Bearer token auth)
* **Environment Config:** python-dotenv

---

##  Database Schema

The API creates the table automatically (if it does not exist) with the following structure:

| Column Name   | Type         | Description    |
| ------------- | ------------ | -------------- |
| client_id     | VARCHAR(50)  | Primary Key    |
| full_name     | VARCHAR(255) | Client name    |
| phone_no      | VARCHAR(20)  | Phone number   |
| client_amount | FLOAT        | Loan/amount    |
| total_land    | FLOAT        | Land owned     |
| year          | INT          | Year           |
| created_at    | TIMESTAMP    | Auto timestamp |

---

##  Environment Variables

Create a `.env` file with the following keys:

```env
DB_USER=your_db_user
DB_PASSWORD=your_db_password
DB_HOST=localhost
DB_PORT=3306
DB_NAME=your_database_name

API_URL=your_llm_api_endpoint
DVARA_TOKEN=your_llm_api_token
```

---

##  API Endpoints

### 🔹 Root Endpoint

**GET /**
Returns API status and available routes.

---

### 🔹 Health Check

**GET /health**

Checks:

* Database connectivity
* Token availability

**Response Example**

```json
{
  "database": "connected",
  "status": "healthy",
  "token_set": true
}
```

---

### 🔹 Upload Excel & Map Fields

**POST /upload/**

#### Query Parameters

| Parameter    | Type    | Description            |
| ------------ | ------- | ---------------------- |
| table_name   | string  | MySQL table name       |
| insert_to_db | boolean | Whether to insert data |

#### Request

* Form-Data
* File key: `file`
* File type: Excel (`.xlsx`)

#### Processing Steps

1. Validates table name
2. Reads Excel file
3. Normalizes column names
4. Fetches DB schema
5. Calls LLM for field mapping
6. Applies fixed priority mappings
7. Cleans data (nulls, duplicates)
8. Optionally inserts into DB

---

##  Column Mapping Logic

### Fixed mappings (highest priority):

```python
{
  "loaner_id": "client_id",
  "name": "full_name",
  "phone_no": "phone_no",
  "loan_amount": "client_amount",
  "total_land": "total_land",
  "year": "year"
}
```

LLM mappings are used only when fixed mappings are not available.

---

##  Response Example

```json
{
  "status": "success",
  "original_columns": ["loaner_id", "name", "loan_amount"],
  "database_fields": ["client_id", "full_name", "client_amount"],
  "mapping": {
    "loaner_id": "client_id",
    "name": "full_name",
    "loan_amount": "client_amount"
  },
  "total_rows": 120,
  "rows_inserted": 115,
  "rows_skipped_existing": 3,
  "rows_dropped_invalid": 2,
  "preview": [
    {
      "client_id": "1001",
      "full_name": "Ravi Kumar",
      "client_amount": 50000
    }
  ]
}
```

---

##  Error Handling

* Invalid table names → `400 Bad Request`
* Missing client_id → `400 Bad Request`
* Token expiry → `403 Forbidden`
* Duplicate primary keys → safely skipped
* Database failures → `500 Internal Server Error`

---

## ▶ Running the Application

```bash
uvicorn main:app --reload
```

Open API docs:

```
http://127.0.0.1:8000/docs
```



