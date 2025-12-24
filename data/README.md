# Sample Data for PySpark Learning

This directory contains sample CSV data files for hands-on practice with PySpark, Pandas, SQL, ETL, and Data Warehouse concepts.

## Data Files

### Core Dimension Tables

| File | Description | Records | Key Fields |
|------|-------------|---------|------------|
| `customers.csv` | Customer master data | 15 | customer_id, name, segment |
| `products.csv` | Product catalog | 15 | product_id, category, brand |
| `employees.csv` | Employee hierarchy | 15 | employee_id, department, manager_id |
| `stores.csv` | Store locations | 5 | store_id, region, store_type |
| `dates.csv` | Date dimension | 31 | date_key, full_date, is_weekend |

### Fact Tables

| File | Description | Records | Key Fields |
|------|-------------|---------|------------|
| `sales.csv` | Sales transactions | 25 | transaction_id, customer_id, product_id |
| `orders.csv` | Order fulfillment | 20 | order_id, status, shipping_method |
| `inventory.csv` | Inventory snapshots | 25 | snapshot_date, product_id, warehouse_id |

## Data Model

```
                    dim_dates
                        |
                        |
    dim_products --- fact_sales --- dim_customers
                        |
                        |
                    dim_stores
```

## Usage Examples

### PySpark
```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Practice").getOrCreate()

# Read CSV files
customers = spark.read.csv("data/customers.csv", header=True, inferSchema=True)
sales = spark.read.csv("data/sales.csv", header=True, inferSchema=True)

# Join and analyze
sales.join(customers, "customer_id") \
    .groupBy("segment") \
    .sum("total_amount") \
    .show()
```

### Pandas
```python
import pandas as pd

customers = pd.read_csv("data/customers.csv")
sales = pd.read_csv("data/sales.csv")

# Merge and analyze
merged = sales.merge(customers, on="customer_id")
merged.groupby("segment")["total_amount"].sum()
```

### SQL (via PySpark)
```python
customers.createOrReplaceTempView("customers")
sales.createOrReplaceTempView("sales")

spark.sql("""
    SELECT c.segment, SUM(s.total_amount) as total_sales
    FROM sales s
    JOIN customers c ON s.customer_id = c.customer_id
    GROUP BY c.segment
""").show()
```

## Data Quality Notes

- All CSV files use UTF-8 encoding
- Date format: YYYY-MM-DD
- NULL values represented as empty strings or "NULL"
- Boolean values: true/false (lowercase)
- Decimal precision: 2 decimal places for currency

## Relationships

- `sales.customer_id` -> `customers.customer_id`
- `sales.product_id` -> `products.product_id`
- `sales.store_id` -> `stores.store_id`
- `orders.customer_id` -> `customers.customer_id`
- `inventory.product_id` -> `products.product_id`
- `employees.manager_id` -> `employees.employee_id`

## License

This sample data is provided for educational purposes as part of the PySpark Learning Hub.
