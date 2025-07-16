#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.8"
# dependencies = [
#     "pandas>=1.3.0",
#     "numpy>=1.21.0"
# ]
# ///

import argparse
import sys
from pathlib import Path
from typing import Any

import pandas as pd


def detect_data_type(series: pd.Series) -> str:
    """
    Detect the most appropriate Snowflake data type for a pandas Series.
    """
    # Handle completely null columns
    if series.isna().all():
        return "VARCHAR"
    
    # Drop nulls for type detection
    non_null_series = series.dropna()
    
    if len(non_null_series) == 0:
        return "VARCHAR"
    
    # Check if it's numeric
    if pd.api.types.is_numeric_dtype(series):
        if pd.api.types.is_integer_dtype(series):
            # Check the range to determine if it's BIGINT or INT
            max_val = series.max()
            min_val = series.min()
            if max_val <= 2147483647 and min_val >= -2147483648:
                return "INTEGER"
            else:
                return "BIGINT"
        else:
            return "FLOAT"
    
    # Check if it's datetime
    if pd.api.types.is_datetime64_any_dtype(series):
        return "TIMESTAMP"
    
    # Check if it's boolean
    if pd.api.types.is_bool_dtype(series):
        return "BOOLEAN"
    
    # Check if string values look like dates
    if series.dtype == 'object':
        sample_values = non_null_series.head(10).astype(str)
        date_patterns = [
            r'\d{4}-\d{2}-\d{2}',  # YYYY-MM-DD
            r'\d{2}/\d{2}/\d{4}',  # MM/DD/YYYY
            r'\d{2}-\d{2}-\d{4}',  # MM-DD-YYYY
        ]
        
        for pattern in date_patterns:
            if sample_values.str.match(pattern).any():
                return "DATE"
    
    # Default to VARCHAR for strings and everything else
    return "VARCHAR"


def escape_sql_value(value: Any, data_type: str) -> str:
    """
    Escape and format a value for SQL based on its data type.
    """
    if pd.isna(value) or value is None:
        return "NULL"
    
    if data_type in ["VARCHAR", "DATE"]:
        # Escape single quotes by doubling them
        escaped_value = str(value).replace("'", "''")
        return f"'{escaped_value}'"
    elif data_type == "BOOLEAN":
        return "TRUE" if value else "FALSE"
    elif data_type in ["INTEGER", "BIGINT", "FLOAT"]:
        return str(value)
    elif data_type == "TIMESTAMP":
        return f"'{value}'"
    else:
        # Default: treat as string
        escaped_value = str(value).replace("'", "''")
        return f"'{escaped_value}'"


def generate_values_clause_sql(df: pd.DataFrame, table_name: str = "csv_data") -> str:
    """
    Generate SQL using VALUES clause (good for smaller datasets).
    """
    if df.empty:
        return "-- Empty dataset\nSELECT NULL WHERE 1=0;"
    
    # Detect data types
    column_types = {}
    for col in df.columns:
        column_types[col] = detect_data_type(df[col])
    
    # Build column list with types (for reference)
    column_list = []
    for col in df.columns:
        safe_col = col.replace(" ", "_").replace("-", "_")
        column_list.append(f"{safe_col}")
    
    # Generate VALUES rows
    values_rows = []
    for _, row in df.iterrows():
        row_values = []
        for col in df.columns:
            value = row[col]
            data_type = column_types[col]
            escaped_value = escape_sql_value(value, data_type)
            row_values.append(escaped_value)
        
        values_rows.append(f"  ({', '.join(row_values)})")
    
    # Construct the final SQL
    sql_parts = [
        "-- Generated SQL from CSV",
        f"-- Columns: {', '.join(df.columns.tolist())}",
        f"-- Data types detected: {column_types}",
        "",
        "SELECT * FROM VALUES"
    ]
    
    sql_parts.extend(values_rows)
    sql_parts.append(f"AS {table_name}({', '.join(column_list)});")
    
    return ",\n".join(sql_parts[:5]) + "\n" + ",\n".join(sql_parts[5:-1]) + "\n" + sql_parts[-1]


def generate_cte_sql(df: pd.DataFrame, table_name: str = "csv_data") -> str:
    """
    Generate SQL using CTE with UNION ALL (more readable for complex data).
    """
    if df.empty:
        return "-- Empty dataset\nSELECT NULL WHERE 1=0;"
    
    # Detect data types
    column_types = {}
    for col in df.columns:
        column_types[col] = detect_data_type(df[col])
    
    # Build column list
    column_list = []
    for col in df.columns:
        safe_col = col.replace(" ", "_").replace("-", "_")
        column_list.append(safe_col)
    
    # Generate SELECT statements
    select_statements = []
    for i, (_, row) in enumerate(df.iterrows()):
        row_values = []
        for col in df.columns:
            value = row[col]
            data_type = column_types[col]
            escaped_value = escape_sql_value(value, data_type)
            
            safe_col = col.replace(" ", "_").replace("-", "_")
            row_values.append(f"{escaped_value} AS {safe_col}")
        
        if i == 0:
            select_statements.append(f"  SELECT {', '.join(row_values)}")
        else:
            select_statements.append(f"  UNION ALL\n  SELECT {', '.join(row_values)}")
    
    # Construct the final SQL
    sql_parts = [
        "-- Generated SQL from CSV",
        f"-- Columns: {', '.join(df.columns.tolist())}",
        f"-- Data types detected: {column_types}",
        "",
        f"WITH {table_name} AS ("
    ]
    
    sql_parts.extend(select_statements)
    sql_parts.extend([
        ")",
        f"SELECT * FROM {table_name};"
    ])
    
    return "\n".join(sql_parts)


def convert_csv_to_sql(csv_path: str, output_path: str, method: str = "values", table_name: str = "csv_data") -> None:
    """
    Convert CSV file to SQL SELECT statement.
    """
    try:
        # Read CSV file
        df = pd.read_csv(csv_path)
        
        if df.empty:
            print("Warning: CSV file is empty")
            sql_content = "-- Empty CSV file\nSELECT NULL WHERE 1=0;"
        else:
            print(f"Processing CSV with {len(df)} rows and {len(df.columns)} columns")
            print(f"Columns: {', '.join(df.columns.tolist())}")
            
            # Generate SQL based on method
            if method == "values":
                sql_content = generate_values_clause_sql(df, table_name)
            elif method == "cte":
                sql_content = generate_cte_sql(df, table_name)
            else:
                raise ValueError(f"Unknown method: {method}. Use 'values' or 'cte'")
        
        # Write SQL to output file
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(sql_content)
        
        print(f"SQL file generated successfully: {output_path}")
        
    except Exception as e:
        print(f"Error processing CSV: {e}")
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser(
        description="Convert CSV file to Snowflake SQL SELECT statement",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python csv_to_sql.py data.csv output.sql
  python csv_to_sql.py data.csv output.sql --method cte
  python csv_to_sql.py data.csv output.sql --table-name my_data
        """
    )
    
    parser.add_argument(
        "csv_path",
        help="Path to the input CSV file"
    )
    
    parser.add_argument(
        "output_path",
        help="Path to the output SQL file"
    )
    
    parser.add_argument(
        "--method",
        choices=["values", "cte"],
        default="values",
        help="SQL generation method: 'values' for VALUES clause (default), 'cte' for CTE with UNION ALL"
    )
    
    parser.add_argument(
        "--table-name",
        default="csv_data",
        help="Name for the table/CTE alias (default: csv_data)"
    )
    
    args = parser.parse_args()
    
    # Validate input file exists
    if not Path(args.csv_path).exists():
        print(f"Error: CSV file not found: {args.csv_path}")
        sys.exit(1)
    
    # Create output directory if it doesn't exist
    output_dir = Path(args.output_path).parent
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Convert CSV to SQL
    convert_csv_to_sql(args.csv_path, args.output_path, args.method, args.table_name)


if __name__ == "__main__":
    main()