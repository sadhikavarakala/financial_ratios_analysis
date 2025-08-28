# Financial Ratios PySpark Pipeline

This project is a PySpark-based data engineering pipeline that reads raw financial statement CSVs (Profit & Loss, Balance Sheet, Cash Flow) for a company from Google Cloud Storage, cleans and transforms the data, calculates standard financial ratios, and writes the results to a Google BigQuery table.

## Features

- Cleans and standardizes messy financial statement CSVs
- Converts data from wide to long format and back for easy analysis
- Calculates key financial ratios (profitability, leverage, returns, etc.)
- Modular Python code using classes for easy maintenance
- Loads results directly into BigQuery for reporting

## Example Output

| company | year | net_profit_margin | return_on_equity | ... |
|---------|------|-------------------|------------------|-----|
| GOOGLE  | 2024 |        35%        |       21%        | ... |
