# 🚕 NYC Taxi dlt Pipeline — DE Zoomcamp 2026 Workshop

Homework submission for the **Data Ingestion Workshop** using `dlt` (Data Load Tool) from [DataTalksClub Data Engineering Zoomcamp 2026](https://github.com/DataTalksClub/data-engineering-zoomcamp).

---

## 📌 Overview

This project builds a data pipeline that:
- Fetches paginated NYC Yellow Taxi trip data from a custom REST API
- Loads it into a local **DuckDB** database using **dlt**
- Answers homework questions via SQL queries

---

## 🗂️ Project Structure

```
my-dlt-pipeline/
├── taxi_pipeline.py     # Pipeline definition & execution
├── query.py             # Data exploration queries
├── homework.py          # Homework answer queries
├── ny_taxi_pipeline.duckdb  # DuckDB database (generated)
└── README.md
```

---

## ⚙️ Setup

**Prerequisites:** Python 3.12+, [uv](https://docs.astral.sh/uv/)

```bash
# Install dependencies
uv add "dlt[duckdb]" numpy pandas
```

---

## 🔧 Pipeline Code

**`taxi_pipeline.py`**

```python
import dlt
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.paginators import PageNumberPaginator

@dlt.resource(name="rides", write_disposition="replace")
def ny_taxi():
    client = RESTClient(
        base_url="https://us-central1-dlthub-analytics.cloudfunctions.net",
        paginator=PageNumberPaginator(
            base_page=1,
            total_path=None  # stop when empty page is returned
        )
    )
    for page in client.paginate("data_engineering_zoomcamp_api"):
        yield page

pipeline = dlt.pipeline(
    pipeline_name="ny_taxi_pipeline",
    destination="duckdb",
    dataset_name="taxi_data"
)

if __name__ == "__main__":
    load_info = pipeline.run(ny_taxi)
    print(load_info)
```

**Run the pipeline:**

```bash
uv run taxi_pipeline.py
```

**Output:**
```
Pipeline ny_taxi_pipeline load step completed in 2.04 seconds
1 load package(s) were loaded to destination duckdb and into dataset taxi_data
The duckdb destination used duckdb:////home/.../ny_taxi_pipeline.duckdb location to store data
Load package ... is LOADED and contains no failed jobs
```

---

## 📊 Data Overview

| Property | Value |
|---|---|
| Source | REST API (paginated JSON) |
| Page size | 1,000 records/page |
| Total rows loaded | **10,000** |
| Destination | DuckDB |
| Dataset | `taxi_data` |
| Table | `rides` |

**Schema (key columns):**

| Column | Type |
|---|---|
| `trip_pickup_date_time` | TIMESTAMP WITH TIME ZONE |
| `trip_dropoff_date_time` | TIMESTAMP WITH TIME ZONE |
| `passenger_count` | BIGINT |
| `trip_distance` | DOUBLE |
| `fare_amt` | DOUBLE |
| `tip_amt` | DOUBLE |
| `total_amt` | DOUBLE |
| `payment_type` | VARCHAR |
| `vendor_name` | VARCHAR |

---

## ❓ Homework Questions & Answers

**`homework.py`**

```python
import duckdb

conn = duckdb.connect("ny_taxi_pipeline.duckdb")

# Q1: Date range
print(conn.execute("""
    SELECT 
        MIN(trip_pickup_date_time) AS start_date,
        MAX(trip_pickup_date_time) AS end_date
    FROM taxi_data.rides
""").df())

# Q2: Payment type proportion
print(conn.execute("""
    SELECT 
        payment_type,
        COUNT(*) AS count,
        ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS percentage
    FROM taxi_data.rides
    GROUP BY payment_type
    ORDER BY count DESC
""").df())

# Q3: Total tips
print(conn.execute("""
    SELECT ROUND(SUM(tip_amt), 2) AS total_tips
    FROM taxi_data.rides
""").df())
```

---

### Q1: What is the start date and end date of the dataset?

```
start_date: 2009-06-01 18:33:00+07:00
end_date:   2009-07-01 06:58:00+07:00
```

> ✅ **2009-06-01 to 2009-07-01**

---

### Q2: What proportion of trips are paid with credit card?

```
payment_type  count  percentage
CASH           7235       72.35
Credit         2666       26.66
Cash             97        0.97
No Charge         1        0.01
Dispute           1        0.01
```

> ✅ **26.66%**

---

### Q3: What is the total amount of money generated in tips?

```
total_tips: 6063.41
```

> ✅ **$6,063.41**

---

## 🛠️ Tools Used

| Tool | Purpose |
|---|---|
| [dlt](https://dlthub.com) | Data pipeline / ingestion |
| [DuckDB](https://duckdb.org) | Local analytical database |
| [uv](https://docs.astral.sh/uv/) | Python package manager |

---

## 📚 Resources

- [DE Zoomcamp 2026](https://github.com/DataTalksClub/data-engineering-zoomcamp)
- [dlt Documentation](https://dlthub.com/docs)
- [dlt REST API Source](https://dlthub.com/docs/dlt-ecosystem/verified-sources/rest_api)
- [dlt Dashboard Docs](https://dlthub.com/docs/general-usage/dashboard)
- [dlt Documentation](https://dlthub.com/docs)
- [dlt REST API Source](https://dlthub.com/docs/dlt-ecosystem/verified-sources/rest_api)
- [dlt Dashboard Docs](https://dlthub.com/docs/general-usage/dashboard)
