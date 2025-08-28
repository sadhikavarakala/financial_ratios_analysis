from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, trim, lower, when, lit, first
import re

COMPANY = "google"
GCS_PATH = f"gs://sadhika-1-central1/companies/{COMPANY}/"
TEMP_BUCKET = "sadhika-temp-bucket"
BQ_TABLE = "silken-forest-466023-a2.financial_ratios.ratios"

class FinancialStatementCleanerLoader:
    def __init__(self, spark, path, statement_type):
        self.spark = spark
        self.path = path
        self.statement_type = statement_type
        self.long_df = None

    def clean_financial_statement(self):
        try: 
            df = self.spark.read.option("header", True).csv(self.path)
        except Exception as e:
            print(f"Error reading file {self.path}: {e}")
            raise 
        
        try:
            # Renaming / filling first column to metric
            df = df.withColumnRenamed(df.columns[0], "metric")

            # Dropping junk rows and cleaning any white spaces
            df = df.withColumn("metric", trim(col("metric")))
            df = df.filter(col("metric") != "Data Model")
            df = df.filter(~col("metric").rlike(".*01-31.*"))

            # Cleaning column names - years
            clean_cols = []
            for c in df.columns:
                new_c = c.strip()
                if self.statement_type in ["pl", "cf"]:
                    new_c = re.sub(r"FY\s*", "", new_c)
                elif self.statement_type == "bs":
                    new_c = re.sub(r"Q4\s*", "", new_c)
                new_c = re.sub(r"[,&()\-\/\s]+", "_", new_c).lower().strip("_")
                clean_cols.append(new_c)
            df = df.toDF(*clean_cols)

            # Cleaning / standardising metric values - converting to lower case, removing special characters
            df = df.withColumn("metric", lower(trim(col("metric"))))
            df = df.withColumn("metric", regexp_replace(col("metric"), r"[,&()\-\/\s]+", "_"))
            df = df.withColumn("metric", regexp_replace(col("metric"), r"^_|_$", ""))

            # converting df from wide to long format
            year_cols = [c for c in df.columns if c != "metric"]
            long_df = df.selectExpr("metric", 
                            "stack({0}, {1}) as (year, value)".format(
                                len(year_cols),
                                ",".join([f"'{c}', `{c}`" for c in year_cols])
                            ))
    
            # Casting data types from str to int, double | creating new col - statement type
            long_df = long_df.withColumn("year", col("year").cast("int"))
            long_df = long_df.withColumn("value", col("value").cast("double"))
            long_df = long_df.withColumn("statement", when(col("metric").isNotNull(), self.statement_type))
            self.long_df = long_df
        except Exception as e:
            print(f"Error cleaning/transforming statement {self.statement_type}: {e}")
            raise
        return self
    
    def get_long_df(self):
        if self.long_df is None:
            raise ValueError("Error long_df is None, Did you run clean_financial_statement()?")
        return self.long_df
    
class FinancialStatementPivot:
    def __init__(self, df, prefix):
        self.df = df
        self.prefix = prefix

    def pivot(self):
        try:
            pivoted = (
                self.df.groupBy("year").pivot("metric").agg(first("value"))
            )
            for c in pivoted.columns:
                if c != "year":
                    pivoted = pivoted.withColumnRenamed(c, f"{self.prefix}_{c}")
            return pivoted
        except Exception as e:
            print(f"Error in pivoting for prefix {self.prefix}: {e}")
            raise

class FinancialRatiosCalculator:
    def __init__(self, company, pl_long, bs_long, cf_long):
        self.company = company
        self.pl_long = pl_long
        self.bs_long = bs_long
        self.cf_long = cf_long

    def calculate(self, year=2024):
        try:
            # filter year - 2024 only
            pl_2024 = self.pl_long.filter(col("year") == year)
            bs_2024 = self.bs_long.filter(col("year") == year)
            cf_2024 = self.cf_long.filter(col("year") == year)

            # Pivot each df
            pl_pivot = FinancialStatementPivot(pl_2024, "pl").pivot()
            bs_pivot = FinancialStatementPivot(bs_2024, "bs").pivot()
            cf_pivot = FinancialStatementPivot(cf_2024, "cf").pivot()

            # joining to calc ratios (on - year, join - inner)
            full_df = pl_pivot.join(bs_pivot, on="year", how="inner").join(cf_pivot, on="year", how="inner")
    
            # calculating and creating financial ratios
            financial_ratios_df = full_df.select(
                lit(self.company).alias("company"),

                # PROFITABILITY RATIOS
                (col("pl_gross_profit") / when(col("pl_revenue") == 0, None).otherwise(col("pl_revenue"))).alias("gross_profit_margin"),
                (col("pl_operating_income_loss") / when(col("pl_revenue") == 0, None).otherwise(col("pl_revenue"))).alias("operating_margin"),
                (col("pl_operating_income_loss") + col("pl_depreciation_amortization")).alias("ebitda"),
                (col("pl_net_income") / when(col("pl_revenue") == 0, None).otherwise(col("pl_revenue"))).alias("net_profit_margin"),

                # CASH FLOW 
                (col("cf_cash_from_operating_activities") - col("cf_acquisition_of_fixed_assets_intangibles")).alias("free_cash_flow"),
                ((col("cf_cash_from_operating_activities") - col("cf_acquisition_of_fixed_assets_intangibles")) / when(col("pl_net_income") == 0, None).otherwise(col("pl_net_income"))).alias("free_cash_flow_to_net_income"),

                # CAPITAL & RETURN RATIOS
                (col("pl_operating_income_loss") / when(
                    (col("bs_total_equity") + col("bs_long_term_debt") + col("bs_short_term_debt")) == 0, None
                ).otherwise(col("bs_total_equity") + col("bs_long_term_debt") + col("bs_short_term_debt"))).alias("cash_return_on_invested_capital"),

                # DIVIDENDS 
                (col("cf_dividends_paid") / when(col("pl_net_income") == 0, None).otherwise(col("pl_net_income"))).alias("dividend_payout_ratio"),

                # LIQUIDITY & LEVERAGE 
                (col("bs_total_current_assets") / when(col("bs_total_current_liabilities") == 0, None).otherwise(col("bs_total_current_liabilities"))).alias("current_ratio"),
                
                # Net Debt/EBITDA and Net Debt/EBIT 
                ((col("bs_short_term_debt") + col("bs_long_term_debt") - col("bs_cash_cash_equivalents")) /
                when((col("pl_operating_income_loss") + col("pl_depreciation_amortization")) == 0, None)
                .otherwise(col("pl_operating_income_loss") + col("pl_depreciation_amortization"))).alias("net_debt_ebitda"),
                ((col("bs_short_term_debt") + col("bs_long_term_debt") - col("bs_cash_cash_equivalents")) /
                when(col("pl_operating_income_loss") == 0, None).otherwise(col("pl_operating_income_loss"))).alias("net_debt_ebit"),

                (col("bs_total_liabilities") / when(col("bs_total_equity") == 0, None).otherwise(col("bs_total_equity"))).alias("debt_to_equity"),
                (col("bs_total_liabilities") / when(col("bs_total_assets") == 0, None).otherwise(col("bs_total_assets"))).alias("debt_ratio"),
                (col("bs_short_term_debt") + col("bs_long_term_debt")).alias("total_debt"),

                # RETURN RATIOS
                (col("pl_operating_income_loss") / when(
                    (col("bs_total_equity") + col("bs_long_term_debt") + col("bs_short_term_debt")) == 0, None
                ).otherwise(col("bs_total_equity") + col("bs_long_term_debt") + col("bs_short_term_debt"))).alias("return_on_invested_capital"),
                (col("pl_net_income") / when(col("bs_total_assets") == 0, None).otherwise(col("bs_total_assets"))).alias("return_on_assets"),
                (col("pl_net_income") / when(col("bs_total_equity") == 0, None).otherwise(col("bs_total_equity"))).alias("return_on_equity"),

                # ADJUSTED RATIOS (original unless 'adj' metrics) 
                col("pl_net_income").alias("net_income_adj"),
                (col("pl_net_income") / when(col("pl_revenue") == 0, None).otherwise(col("pl_revenue"))).alias("net_profit_margin_adj"),
                ((col("cf_cash_from_operating_activities") - col("cf_acquisition_of_fixed_assets_intangibles")) /
                when(col("pl_net_income") == 0, None).otherwise(col("pl_net_income"))).alias("fcf_to_net_income"),
                (col("pl_net_income") / when(col("bs_total_equity") == 0, None).otherwise(col("bs_total_equity"))).alias("return_on_equity_adj"),
                (col("pl_net_income") / when(col("bs_total_assets") == 0, None).otherwise(col("bs_total_assets"))).alias("return_on_assets_adj"),
                (col("pl_operating_income_loss") / when(
                    (col("bs_total_equity") + col("bs_long_term_debt") + col("bs_short_term_debt")) == 0, None
                ).otherwise(col("bs_total_equity") + col("bs_long_term_debt") + col("bs_short_term_debt"))).alias("return_on_invested_capital_adj")
            )
        
            return financial_ratios_df
        except Exception as e:
            print(f"Error in Ratio Calculation: {e}")
            raise

class BigQueryWriter:
    def __init__(self, df, bq_table, temp_bucket):
        self.df = df
        self.bq_table = bq_table
        self.temp_bucket = temp_bucket

    def write(self, mode="append"):
        try:
            (
                self.df.write
                .format("bigquery")
                .option("temporaryGcsBucket", self.temp_bucket)
                .mode(mode)
                .save(self.bq_table)
            )
            print(f"Data written to BigQuery table {self.bq_table} successfully.")
        except Exception as e:
            print(f"Error writing to BigQuery: {e}")
            raise

if __name__ == "__main__":
    try: 
        spark = SparkSession.builder.appName("FinancialRatios").getOrCreate()

        #paths with company variable
        bs_path = GCS_PATH + f"{COMPANY.upper()}_BS.csv"
        pl_path = GCS_PATH + f"{COMPANY.upper()}_PL.csv"
        cf_path = GCS_PATH + f"{COMPANY.upper()}_CF.csv"

        #Clean
        bs_cleaner = FinancialStatementCleanerLoader(spark, bs_path, "bs").clean_financial_statement()
        pl_cleaner = FinancialStatementCleanerLoader(spark, pl_path, "pl").clean_financial_statement()
        cf_cleaner = FinancialStatementCleanerLoader(spark, cf_path, "cf").clean_financial_statement()

        #get long form dataframes
        bs_long = bs_cleaner.get_long_df()
        pl_long = pl_cleaner.get_long_df()
        cf_long = cf_cleaner.get_long_df()

        # Calculate Ratios
        ratios_calculator = FinancialRatiosCalculator(COMPANY.upper(), pl_long, bs_long, cf_long)
        financial_ratios = ratios_calculator.calculate(year=2024)

        financial_ratios.show()

        #write to bigquery
        bq_writer = BigQueryWriter(financial_ratios, BQ_TABLE, TEMP_BUCKET)
        bq_writer.write(mode="append")

    except Exception as e:
        print(f"Error: Pipeline Failed: {e}")
