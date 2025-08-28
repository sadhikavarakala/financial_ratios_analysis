# Financial Ratios PySpark Pipeline

This project is a PySpark-based data engineering pipeline that reads raw financial statement CSVs (Profit & Loss, Balance Sheet, Cash Flow) for a company from Google Cloud Storage, cleans and transforms the data, calculates standard financial ratios, and writes the results to a Google BigQuery table.

## Features

- Cleans and standardizes messy financial statement CSVs
- Converts data from wide to long format and back for easy analysis
- Calculates key financial ratios (profitability, leverage, returns, etc.)
- Modular Python code using classes for easy maintenance
- Loads results directly into BigQuery for reporting

## Example Output

| company | current_ratio | debt_to_equity | net_profit_margin | return_on_equity |
|---------|--------------|----------------|-------------------|------------------|
| INTEL   | 1.33         | 8.87           | -0.35             | -0.18            |
| GOOGLE  | 1.84         | 0.39           | 0.29              | 0.31             |
| NVIDIA  | 4.44         | 0.41           | 0.56              | 0.92             |
| AMD     | 2.62         |                | 0.06              | 0.03             |

