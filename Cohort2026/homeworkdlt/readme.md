# 🚕 Data Engineering Zoomcamp -- Workshop DLT

## dlt Pipeline: NYC Taxi Data → DuckDB

This homework builds a **dlt pipeline** that extracts NYC Yellow Taxi
trip data from a custom REST API, loads it into **DuckDB**, and answers
analytical questions using the loaded dataset.

------------------------------------------------------------------------

## 📌 The Challenge

Build an end-to-end **ELT pipeline** using `dlt` that:

-   Extracts NYC Taxi trip data from a custom API
-   Loads data into DuckDB
-   Performs analysis to answer provided questions

------------------------------------------------------------------------

## 📊 Data Source

  ---------------------------------------------------------------------------------------------------------------------------
  Property                            Value
  ----------------------------------- ---------------------------------------------------------------------------------------
  Base URL                            https://us-central1-dlthub-analytics.cloudfunctions.net/data_engineering_zoomcamp_api

  Format                              Paginated JSON

  Page Size                           1,000 records

  Pagination                          Stop when empty page returned
  ---------------------------------------------------------------------------------------------------------------------------

Dataset contains individual NYC Yellow Taxi trip records.

------------------------------------------------------------------------

## ⚙️ Project Setup

### 1️⃣ Create Project Folder

``` bash
mkdir taxi-pipeline
cd taxi-pipeline
```

------------------------------------------------------------------------

### 2️⃣ Set Up the dlt MCP Server

#### Cursor

Go to:

`Settings → Tools & MCP → New MCP Server`

Add configuration:

``` json
{
  "mcpServers": {
    "dlt": {
      "command": "uv",
      "args": [
        "run",
        "--with",
        "dlt[duckdb]",
        "--with",
        "dlt-mcp[search]",
        "python",
        "-m",
        "dlt_mcp"
      ]
    }
  }
}
```

#### VS Code (Copilot)

Create file:

    .vscode/mcp.json

Add:

``` json
{
  "servers": {
    "dlt": {
      "command": "uv",
      "args": [
        "run",
        "--with",
        "dlt[duckdb]",
        "--with",
        "dlt-mcp[search]",
        "python",
        "-m",
        "dlt_mcp"
      ]
    }
  }
}
```

#### Claude Code

``` bash
claude mcp add dlt -- uv run --with "dlt[duckdb]" --with "dlt-mcp[search]" python -m dlt_mcp
```

------------------------------------------------------------------------

### 3️⃣ Install dlt

``` bash
pip install "dlt[workspace]"
```

------------------------------------------------------------------------

### 4️⃣ Initialize the Project

``` bash
dlt init dlthub:taxi_pipeline duckdb
```

This command creates:

-   dlt project structure
-   configuration files
-   AI assistant rules

Since this API has no scaffold, API metadata must be defined manually.

------------------------------------------------------------------------

### 5️⃣ Build the Pipeline

Create file:

    taxi_pipeline.py

Pipeline name:

    taxi_pipeline

Example agent prompt:

Build a REST API source for NYC taxi data.

API details:

-   Base URL:
    https://us-central1-dlthub-analytics.cloudfunctions.net/data_engineering_zoomcamp_api
-   Data format: Paginated JSON (1,000 records per page)
-   Pagination: Stop when an empty page is returned

------------------------------------------------------------------------

### 6️⃣ Run and Debug

``` bash
python taxi_pipeline.py
```

Iterate until the pipeline runs successfully.

------------------------------------------------------------------------

## 🔎 Exploration Methods

### dlt Dashboard

``` bash
dlt pipeline taxi_pipeline show
```

### Alternative Methods

-   dlt MCP Server (ask AI questions)
-   Marimo Notebook for visualization and SQL queries

------------------------------------------------------------------------

## ❓ Questions

### Question 1

What is the start date and end date of the dataset?

-   2009-01-01 to 2009-01-31
-   2009-06-01 to 2009-07-01
-   2024-01-01 to 2024-02-01
-   2024-06-01 to 2024-07-01

**Answer**
-   2009-06-01 to 2009-07-01
**Query**

``` sql
-- add query here
```

------------------------------------------------------------------------

### Question 2

What proportion of trips are paid with credit card?

-   16.66%
-   26.66%
-   36.66%
-   46.66%

**Answer**
-   26.66%
**Query**

``` sql
-- add query here
```

------------------------------------------------------------------------

### Question 3

What is the total amount of money generated in tips?

-   \$4,063.41
-   \$6,063.41
-   \$8,063.41
-   \$10,063.41

**Answer**
-   \$6,063.41
**Query**

``` sql
-- add query here
```

------------------------------------------------------------------------

## 🧠 Learnings & Reflections

### What worked well

-   Fill your experience

### Challenges faced

-   Fill your challenges

### Key takeaways

-   Fill your insights

------------------------------------------------------------------------

## 📚 Resources

  -------------------------------------------------------------------------------------------------
  Resource                            Link
  ----------------------------------- -------------------------------------------------------------
  dlt Dashboard Docs                  https://dlthub.com/docs/general-usage/dashboard

  marimo + dlt Guide                  https://dlthub.com/docs/general-usage/dataset-access/marimo

  dlt Documentation                   https://dlthub.com/docs
  -------------------------------------------------------------------------------------------------

------------------------------------------------------------------------

## 🚀 Author

**Arsi Amallah**\
Data Engineering Zoomcamp 2026 Participant
