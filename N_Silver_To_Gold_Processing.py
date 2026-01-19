#!/usr/bin/env python
# coding: utf-8

# ## N_Silver_To_Gold_Processing
# 
# 
# 

# In[ ]:


from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Define paths
silver_base_path = "abfss://fintech@gdsdemo2026storage.dfs.core.windows.net/silver/fintech/"
output_base_path = "abfss://fintech@gdsdemo2026storage.dfs.core.windows.net/gold/fintech/"

# Enable Delta Lake optimizations
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")

def create_dimension_tables():
    """Create dimension tables following proper gold layer standards."""
    logger.info("Creating dimension tables for Gold layer")
    
    # Load data from the silver layer
    customers_df = spark.read.format("delta").load(f"{silver_base_path}Customers/")
    accounts_df = spark.read.format("delta").load(f"{silver_base_path}Accounts/")
    loans_df = spark.read.format("delta").load(f"{silver_base_path}Loans/")
    
    # Create DIM_CUSTOMERS - Master customer dimension table
    dim_customers = customers_df.select(
        col("CustomerID").alias("customer_key"),
        col("CustomerID").alias("customer_id"),
        col("FirstName").alias("first_name"),
        col("LastName").alias("last_name"),
        col("FullName").alias("full_name"),
        col("Email").alias("email"),
        col("MaskedEmail").alias("masked_email"),
        col("PhoneNumber").alias("phone_number"),
        col("Address").alias("address"),
        col("City").alias("city"),
        col("State").alias("state"),
        col("Country").alias("country"),
        col("ZipCode").alias("zip_code"),
        col("SignupDate").alias("signup_date"),
        col("CustomerAge").alias("customer_age"),
        col("CustomerSegment").alias("customer_segment"),
        col("CustomerTier").alias("customer_tier"),
        current_timestamp().alias("etl_timestamp"),
        lit("ACTIVE").alias("record_status")
    ).distinct()
    
    # Create DIM_ACCOUNTS - Master account dimension table
    dim_accounts = accounts_df.select(
        col("AccountID").alias("account_key"),
        col("AccountID").alias("account_id"),
        col("CustomerID").alias("customer_id"),
        col("AccountType").alias("account_type"),
        col("Balance").alias("current_balance"),
        col("OpenDate").alias("open_date"),
        col("AccountAgeYears").alias("account_age_years"),
        col("AccountStatus").alias("account_status"),
        col("AccountTier").alias("account_tier"),
        col("MonthlyInterest").alias("monthly_interest"),
        col("IsHighValue").alias("is_high_value"),
        current_timestamp().alias("etl_timestamp"),
        lit("ACTIVE").alias("record_status")
    ).distinct()
    
    # Create DIM_LOANS - Master loan dimension table
    dim_loans = loans_df.select(
        col("LoanID").alias("loan_key"),
        col("LoanID").alias("loan_id"),
        col("CustomerID").alias("customer_id"),
        col("LoanType").alias("loan_type"),
        col("LoanAmount").alias("loan_amount"),
        col("InterestRate").alias("interest_rate"),
        col("LoanStartDate").alias("loan_start_date"),
        col("LoanEndDate").alias("loan_end_date"),
        col("LoanDurationYears").alias("loan_duration_years"),
        col("TotalInterest").alias("total_interest"),
        col("MonthlyPayment").alias("monthly_payment"),
        col("LoanStatus").alias("loan_status"),
        col("RiskCategory").alias("risk_category"),
        col("LoanToValueRatio").alias("loan_to_value_ratio"),
        col("IsHighRisk").alias("is_high_risk"),
        current_timestamp().alias("etl_timestamp"),
        lit("ACTIVE").alias("record_status")
    ).distinct()
    
    # Write dimension tables
    dim_customers.write.format("delta").mode("overwrite").save(f"{output_base_path}dim_customers/")
    dim_accounts.write.format("delta").mode("overwrite").save(f"{output_base_path}dim_accounts/")
    dim_loans.write.format("delta").mode("overwrite").save(f"{output_base_path}dim_loans/")
    
    logger.info("Dimension tables created successfully")
    return dim_customers, dim_accounts, dim_loans

def create_fact_tables():
    """Create fact tables following proper gold layer standards."""
    logger.info("Creating fact tables for Gold layer")
    
    # Load data from silver layer
    customers_df = spark.read.format("delta").load(f"{silver_base_path}Customers/")
    accounts_df = spark.read.format("delta").load(f"{silver_base_path}Accounts/")
    loans_df = spark.read.format("delta").load(f"{silver_base_path}Loans/")
    transactions_df = spark.read.format("delta").load(f"{silver_base_path}Transactions/")
    payments_df = spark.read.format("delta").load(f"{silver_base_path}Payments/")
    
    # Create FACT_TRANSACTIONS - Transaction fact table with proper grain
    fact_transactions = transactions_df.join(
        accounts_df.select("AccountID", "CustomerID"), "AccountID"
    ).select(
        col("TransactionID").alias("transaction_key"),
        col("TransactionID").alias("transaction_id"),
        col("AccountID").alias("account_key"),
        col("CustomerID").alias("customer_key"),
        col("TransactionDate").alias("transaction_date"),
        col("Amount").alias("transaction_amount"),
        col("TransactionType").alias("transaction_type"),
        col("TransactionCategory").alias("transaction_category"),
        col("TransactionSize").alias("transaction_size"),
        col("Description").alias("transaction_description"),
        col("TransactionDayOfWeek").alias("transaction_day_of_week"),
        col("TransactionMonth").alias("transaction_month"),
        col("TransactionYear").alias("transaction_year"),
        col("IsWeekend").alias("is_weekend_transaction"),
        col("IsLargeTransaction").alias("is_large_transaction"),
        current_timestamp().alias("etl_timestamp")
    )
    
    # Create FACT_PAYMENTS - Payment fact table with proper grain
    fact_payments = payments_df.join(
        loans_df.select("LoanID", "CustomerID"), "LoanID"
    ).select(
        col("PaymentID").alias("payment_key"),
        col("PaymentID").alias("payment_id"),
        col("LoanID").alias("loan_key"),
        col("CustomerID").alias("customer_key"),
        col("PaymentDate").alias("payment_date"),
        col("PaymentAmount").alias("payment_amount"),
        col("PaymentMethod").alias("payment_method"),
        col("PaymentMethodCategory").alias("payment_method_category"),
        col("PaymentSize").alias("payment_size"),
        col("DaysSinceLastPayment").alias("days_since_last_payment"),
        col("IsLatePayment").alias("is_late_payment"),
        col("IsLargePayment").alias("is_large_payment"),
        current_timestamp().alias("etl_timestamp")
    )
    
    # Create FACT_CUSTOMER_ACCOUNTS - Bridge table for customer-account relationships
    fact_customer_accounts = accounts_df.select(
        col("AccountID").alias("account_key"),
        col("CustomerID").alias("customer_key"),
        col("AccountType").alias("account_type"),
        col("Balance").alias("account_balance"),
        col("AccountStatus").alias("account_status"),
        col("AccountTier").alias("account_tier"),
        col("IsHighValue").alias("is_high_value_account"),
        current_timestamp().alias("etl_timestamp")
    )
    
    # Write fact tables
    fact_transactions.write.format("delta").mode("overwrite").save(f"{output_base_path}fact_transactions/")
    fact_payments.write.format("delta").mode("overwrite").save(f"{output_base_path}fact_payments/")
    fact_customer_accounts.write.format("delta").mode("overwrite").save(f"{output_base_path}fact_customer_accounts/")
    
    logger.info("Fact tables created successfully")
    return fact_transactions, fact_payments, fact_customer_accounts

def create_aggregated_tables():
    """Create aggregated tables for analytics and reporting."""
    logger.info("Creating aggregated tables for Gold layer")
    
    # Load fact tables
    fact_transactions = spark.read.format("delta").load(f"{output_base_path}fact_transactions/")
    fact_payments = spark.read.format("delta").load(f"{output_base_path}fact_payments/")
    
    # Create AGG_CUSTOMER_SUMMARY - Customer-level aggregations
    agg_customer_summary = fact_transactions.groupBy("customer_key") \
        .agg(
            count("*").alias("total_transactions"),
            sum("transaction_amount").alias("total_transaction_amount"),
            avg("transaction_amount").alias("avg_transaction_amount"),
            max("transaction_amount").alias("max_transaction_amount"),
            min("transaction_amount").alias("min_transaction_amount"),
            countDistinct("transaction_type").alias("transaction_type_count"),
            sum(when(col("is_large_transaction"), 1).otherwise(0)).alias("large_transaction_count"),
            sum(when(col("is_weekend_transaction"), 1).otherwise(0)).alias("weekend_transaction_count")
        ) \
        .withColumn("etl_timestamp", current_timestamp())
    
    # Create AGG_ACCOUNT_SUMMARY - Account-level aggregations
    agg_account_summary = fact_transactions.groupBy("account_key") \
        .agg(
            count("*").alias("total_transactions"),
            sum("transaction_amount").alias("total_transaction_amount"),
            avg("transaction_amount").alias("avg_transaction_amount"),
            sum(when(col("transaction_category") == "Income", col("transaction_amount")).otherwise(0)).alias("total_deposits"),
            sum(when(col("transaction_category") == "Expense", col("transaction_amount")).otherwise(0)).alias("total_withdrawals"),
            count(when(col("transaction_category") == "Income", 1)).alias("deposit_count"),
            count(when(col("transaction_category") == "Expense", 1)).alias("withdrawal_count")
        ) \
        .withColumn("etl_timestamp", current_timestamp())
    
    # Create AGG_LOAN_SUMMARY - Loan-level aggregations
    agg_loan_summary = fact_payments.groupBy("loan_key") \
        .agg(
            count("*").alias("total_payments"),
            sum("payment_amount").alias("total_payment_amount"),
            avg("payment_amount").alias("avg_payment_amount"),
            max("payment_amount").alias("max_payment_amount"),
            min("payment_amount").alias("min_payment_amount"),
            countDistinct("payment_method").alias("payment_method_count"),
            sum(when(col("is_late_payment"), 1).otherwise(0)).alias("late_payment_count"),
            avg("days_since_last_payment").alias("avg_days_since_payment")
        ) \
        .withColumn("etl_timestamp", current_timestamp())
    
    # Write aggregated tables
    agg_customer_summary.write.format("delta").mode("overwrite").save(f"{output_base_path}agg_customer_summary/")
    agg_account_summary.write.format("delta").mode("overwrite").save(f"{output_base_path}agg_account_summary/")
    agg_loan_summary.write.format("delta").mode("overwrite").save(f"{output_base_path}agg_loan_summary/")
    
    logger.info("Aggregated tables created successfully")
    return agg_customer_summary, agg_account_summary, agg_loan_summary

def create_time_dimension():
    """Create time dimension table for temporal analysis."""
    logger.info("Creating time dimension table")
    
    # Generate date range for time dimension
    start_date = "2023-01-01"
    end_date = "2025-12-31"
    
    # Calculate number of days between start and end date
    num_days = spark.sql(f"SELECT DATEDIFF('{end_date}', '{start_date}') as days").collect()[0]["days"]
    
    # Create time dimension with proper type casting
    # Cast id to IntegerType() since date_add requires INT, SMALLINT, or TINYINT
    time_dim = spark.range(0, num_days + 1).select(
        col("id").cast(IntegerType()).alias("date_key"),
        date_add(lit(start_date), col("id").cast(IntegerType())).alias("date")
    ).select(
        col("date_key"),
        col("date"),
        year(col("date")).alias("year"),
        month(col("date")).alias("month"),
        dayofmonth(col("date")).alias("day"),
        dayofweek(col("date")).alias("day_of_week"),
        dayofyear(col("date")).alias("day_of_year"),
        quarter(col("date")).alias("quarter"),
        weekofyear(col("date")).alias("week_of_year"),
        date_format(col("date"), "EEEE").alias("day_name"),
        date_format(col("date"), "MMMM").alias("month_name"),
        when(dayofweek(col("date")).isin([1, 7]), "Weekend").otherwise("Weekday").alias("weekend_flag"),
        when(month(col("date")).isin([12, 1, 2]), "Winter")
        .when(month(col("date")).isin([3, 4, 5]), "Spring")
        .when(month(col("date")).isin([6, 7, 8]), "Summer")
        .otherwise("Fall").alias("season")
    ).filter(col("date") <= lit(end_date))
    
    time_dim.write.format("delta").mode("overwrite").save(f"{output_base_path}dim_time/")
    logger.info("Time dimension table created successfully")
    return time_dim

# Execute Gold layer creation
try:
    logger.info("Starting Silver to Gold layer processing")
    
    # Create dimension tables
    dim_customers, dim_accounts, dim_loans = create_dimension_tables()
    
    # Create fact tables
    fact_transactions, fact_payments, fact_customer_accounts = create_fact_tables()
    
    # Create aggregated tables
    agg_customer_summary, agg_account_summary, agg_loan_summary = create_aggregated_tables()
    
    # Create time dimension
    time_dim = create_time_dimension()
    
    print("Silver To Gold Processing Completed Successfully!")
    logger.info("Gold layer created with proper dimensional modeling standards")
    
except Exception as e:
    logger.error(f"Error in Silver to Gold processing: {str(e)}")
    print(f"Error: {str(e)}")
    raise


