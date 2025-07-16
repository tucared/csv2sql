# CSV to SQL Converter

Convert CSV files to SQL SELECT statements. Works with Snowflake, PostgreSQL, MySQL, BigQuery, and most SQL databases. Perfect for Metabase and other SQL tools.

## Installation

```bash
# Install UV if needed
curl -LsSf https://astral.sh/uv/install.sh | sh

# Make executable
chmod +x csv_to_sql.py
```

## Usage

```bash
# Basic usage
./csv_to_sql.py data.csv output.sql

# For larger datasets (>500 rows)
./csv_to_sql.py data.csv output.sql --method cte

# Custom table name
./csv_to_sql.py data.csv output.sql --table-name my_data
```

## Output

**VALUES method (default):**
```sql
SELECT * FROM VALUES
  ('John', 25, 'Engineer'),
  ('Jane', 30, 'Manager')
AS csv_data(name, age, role);
```

**CTE method:**
```sql
WITH csv_data AS (
  SELECT 'John' AS name, 25 AS age, 'Engineer' AS role
  UNION ALL
  SELECT 'Jane', 30, 'Manager'
)
SELECT * FROM csv_data;
```

Automatically detects data types and handles SQL escaping. Use VALUES for small datasets, CTE for larger ones. Works with most SQL databases.