#!/usr/bin/env python
# coding: utf-8

# ## N_Bronze_To_Silver_Data_Processing
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


# https://gdsdemo2026storage.blob.core.windows.net/fintech/bronze/fintech/Accounts/Accounts.parquet

# spark.read.parquet(f"{base_path}Customers/Customers.parquet")


base_path = "abfss://fintech@gdsdemo2026storage.dfs.core.windows.net/bronze/fintech/"
output_base_path = "abfss://fintech@gdsdemo2026storage.dfs.core.windows.net/silver/fintech/"

# Enable Delta Lake optimizations
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")

def validate_data_quality(df, table_name):
    """Validate data quality and log results."""
    total_records = df.count()
    null_records = df.filter(col("CustomerID").isNull()).count() if "CustomerID" in df.columns else 0
    
    quality_score = ((total_records - null_records) / total_records * 100) if total_records > 0 else 0
    
    logger.info(f"{table_name} - Total records: {total_records}, Quality score: {quality_score:.2f}%")
    
    if quality_score < 95:
        logger.warning(f"Data quality below threshold for {table_name}")
    
    return quality_score

def clean_and_validate_customers(df):
    """Clean and validate customer data with comprehensive transformations."""
    logger.info("Processing customers data")
    
    # Data validation
    validate_data_quality(df, "customers")
    
    # Data cleaning - standardize text fields
    cleaned_df = df.withColumn("FirstName", initcap(trim(col("FirstName")))) \
                 .withColumn("LastName", initcap(trim(col("LastName")))) \
                 .withColumn("Email", lower(trim(col("Email")))) \
                 .withColumn("City", initcap(trim(col("City")))) \
                 .withColumn("State", upper(trim(col("State")))) \
                 .withColumn("Country", initcap(trim(col("Country"))))
    
    # Create derived fields for Silver layer
    enriched_df = cleaned_df.withColumn("FullName", 
                                       concat_ws(" ", col("FirstName"), col("LastName"))) \
                           .withColumn("CustomerAge", 
                                       round(datediff(current_date(), col("SignupDate")) / 365.25, 0)) \
                           .withColumn("CustomerSegment", 
                                       when(col("SignupDate") >= "2024-01-01", "New")
                                       .when(col("SignupDate") >= "2023-01-01", "Recent")
                                       .otherwise("Established")) \
                           .withColumn("MaskedEmail", 
                                       concat(lit("***@"), substring_index(col("Email"), "@", -1))) \
                           .withColumn("CustomerTier", 
                                       when(col("SignupDate") >= "2024-01-01", "Premium")
                                       .when(col("SignupDate") >= "2023-06-01", "Gold")
                                       .otherwise("Standard"))
    
    # Data quality checks
    invalid_emails = enriched_df.filter(~col("Email").rlike(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$')).count()
    if invalid_emails > 0:
        logger.warning(f"Found {invalid_emails} invalid email addresses")
    
    return enriched_df

def clean_and_validate_accounts(df):
    """Clean and validate account data with comprehensive transformations."""
    logger.info("Processing accounts data")
    
    # Data validation
    validate_data_quality(df, "accounts")
    
    # Data cleaning - standardize account types and validate balances
    cleaned_df = df.withColumn("AccountType", 
                              when(col("AccountType").isin(["Savings", "Saving"]), "Savings")
                              .when(col("AccountType").isin(["Checking", "Check"]), "Checking")
                              .when(col("AccountType").isin(["Investment", "Invest"]), "Investment")
                              .when(col("AccountType").isin(["Business", "Biz"]), "Business")
                              .otherwise(col("AccountType"))) \
                  .withColumn("Balance", 
                              when(col("Balance") < 0, 0.0).otherwise(col("Balance")))
    
    # Create derived fields for Silver layer
    enriched_df = cleaned_df.withColumn("AccountAgeYears", 
                                       round(datediff(current_date(), col("OpenDate")) / 365.25, 2)) \
                           .withColumn("AccountStatus", 
                                       when(col("Balance") == 0, "Inactive")
                                       .when(col("Balance") < 1000, "Low Balance")
                                       .when(col("Balance") < 10000, "Standard")
                                       .otherwise("Premium")) \
                           .withColumn("AccountTier", 
                                       when(col("Balance") >= 50000, "Gold")
                                       .when(col("Balance") >= 25000, "Silver")
                                       .when(col("Balance") >= 10000, "Bronze")
                                       .otherwise("Basic")) \
                           .withColumn("MonthlyInterest", 
                                       (col("Balance") * 0.02 / 12).cast(DecimalType(18, 2))) \
                           .withColumn("IsHighValue", 
                                       col("Balance") >= 50000)
    
    # Data quality checks
    negative_balances = enriched_df.filter(col("Balance") < 0).count()
    if negative_balances > 0:
        logger.warning(f"Found {negative_balances} accounts with negative balances")
    
    return enriched_df

def clean_and_validate_loans(df):
    """Clean and validate loan data with comprehensive transformations."""
    logger.info("Processing loans data")
    
    # Data validation
    validate_data_quality(df, "loans")
    
    # Data cleaning - standardize loan types and validate amounts
    cleaned_df = df.withColumn("LoanType", 
                              when(col("LoanType").isin(["Personal", "Personal Loan"]), "Personal")
                              .when(col("LoanType").isin(["Home", "Home Loan", "Mortgage"]), "Home")
                              .when(col("LoanType").isin(["Car", "Auto", "Car Loan"]), "Car")
                              .when(col("LoanType").isin(["Education", "Student", "Education Loan"]), "Education")
                              .when(col("LoanType").isin(["Business", "Business Loan"]), "Business")
                              .otherwise(col("LoanType"))) \
                  .withColumn("LoanAmount", 
                              when(col("LoanAmount") <= 0, 1000.0).otherwise(col("LoanAmount"))) \
                  .withColumn("InterestRate", 
                              when(col("InterestRate") < 0, 0.0)
                              .when(col("InterestRate") > 30.0, 30.0)
                              .otherwise(col("InterestRate")))
    
    # Create derived fields for Silver layer
    enriched_df = cleaned_df.withColumn("LoanDurationYears", 
                                       round(datediff(col("LoanEndDate"), col("LoanStartDate")) / 365.25, 2)) \
                           .withColumn("TotalInterest", 
                                       (col("LoanAmount") * col("InterestRate") / 100).cast(DecimalType(18, 2))) \
                           .withColumn("MonthlyPayment", 
                                       ((col("LoanAmount") + col("TotalInterest")) / (col("LoanDurationYears") * 12)).cast(DecimalType(18, 2))) \
                           .withColumn("LoanStatus", 
                                       when(col("LoanEndDate") < current_date(), "Completed")
                                       .when(col("LoanStartDate") > current_date(), "Pending")
                                       .otherwise("Active")) \
                           .withColumn("RiskCategory", 
                                       when((col("LoanAmount") > 50000) & (col("InterestRate") > 10), "High Risk")
                                       .when((col("LoanAmount") > 25000) & (col("InterestRate") > 7), "Medium Risk")
                                       .otherwise("Low Risk")) \
                           .withColumn("LoanToValueRatio", 
                                       (col("LoanAmount") / 100000).cast(DecimalType(5, 2))) \
                           .withColumn("IsHighRisk", 
                                       (col("LoanAmount") > 50000) & (col("InterestRate") > 10))
    
    # Data quality checks
    invalid_dates = enriched_df.filter(col("LoanStartDate") >= col("LoanEndDate")).count()
    if invalid_dates > 0:
        logger.warning(f"Found {invalid_dates} loans with invalid date ranges")
    
    return enriched_df

def clean_and_validate_transactions(df):
    """Clean and validate transaction data with comprehensive transformations."""
    logger.info("Processing transactions data")
    
    # Data validation
    validate_data_quality(df, "transactions")
    
    # Data cleaning - standardize transaction types and validate amounts
    cleaned_df = df.withColumn("TransactionType", 
                              when(col("TransactionType").isin(["Deposit", "Credit"]), "Deposit")
                              .when(col("TransactionType").isin(["Withdrawal", "Debit"]), "Withdrawal")
                              .when(col("TransactionType").isin(["Transfer", "Transfer In", "Transfer Out"]), "Transfer")
                              .when(col("TransactionType").isin(["Fee", "Service Fee", "Maintenance Fee"]), "Fee")
                              .otherwise(col("TransactionType"))) \
                  .withColumn("Amount", 
                              when(col("Amount") <= 0, 0.01).otherwise(col("Amount"))) \
                  .withColumn("Description", trim(col("Description")))
    
    # Create derived fields for Silver layer
    enriched_df = cleaned_df.withColumn("TransactionCategory", 
                                       when(col("TransactionType") == "Deposit", "Income")
                                       .when(col("TransactionType") == "Withdrawal", "Expense")
                                       .when(col("TransactionType") == "Transfer", "Transfer")
                                       .when(col("TransactionType") == "Fee", "Fee")
                                       .otherwise("Other")) \
                           .withColumn("TransactionSize", 
                                       when(col("Amount") >= 10000, "Large")
                                       .when(col("Amount") >= 1000, "Medium")
                                       .when(col("Amount") >= 100, "Small")
                                       .otherwise("Micro")) \
                           .withColumn("TransactionDayOfWeek", 
                                       date_format(col("TransactionDate"), "EEEE")) \
                           .withColumn("TransactionMonth", 
                                       date_format(col("TransactionDate"), "MM")) \
                           .withColumn("TransactionYear", 
                                       date_format(col("TransactionDate"), "yyyy")) \
                           .withColumn("IsWeekend", 
                                       when(col("TransactionDayOfWeek").isin(["Saturday", "Sunday"]), True)
                                       .otherwise(False)) \
                           .withColumn("IsLargeTransaction", 
                                       col("Amount") >= 10000)
    
    return enriched_df

def clean_and_validate_payments(df):
    """Clean and validate payment data with comprehensive transformations."""
    logger.info("Processing payments data")
    
    # Data validation
    validate_data_quality(df, "payments")
    
    # Data cleaning - standardize payment methods and validate amounts
    cleaned_df = df.withColumn("PaymentMethod", 
                              when(col("PaymentMethod").isin(["Credit Card", "Credit"]), "Credit Card")
                              .when(col("PaymentMethod").isin(["Debit Card", "Debit"]), "Debit Card")
                              .when(col("PaymentMethod").isin(["Cash", "Cash Payment"]), "Cash")
                              .when(col("PaymentMethod").isin(["Bank Transfer", "Wire Transfer", "ACH"]), "Bank Transfer")
                              .when(col("PaymentMethod").isin(["Check", "Cheque"]), "Check")
                              .otherwise(col("PaymentMethod"))) \
                  .withColumn("PaymentAmount", 
                              when(col("PaymentAmount") <= 0, 0.01).otherwise(col("PaymentAmount")))
    
    # Create derived fields for Silver layer
    enriched_df = cleaned_df.withColumn("DaysSinceLastPayment", 
                                       datediff(current_date(), col("PaymentDate"))) \
                           .withColumn("PaymentSize", 
                                       when(col("PaymentAmount") >= 5000, "Large")
                                       .when(col("PaymentAmount") >= 1000, "Medium")
                                       .when(col("PaymentAmount") >= 100, "Small")
                                       .otherwise("Micro")) \
                           .withColumn("PaymentMethodCategory", 
                                       when(col("PaymentMethod").isin(["Credit Card", "Debit Card"]), "Card")
                                       .when(col("PaymentMethod") == "Cash", "Cash")
                                       .when(col("PaymentMethod") == "Bank Transfer", "Electronic")
                                       .when(col("PaymentMethod") == "Check", "Check")
                                       .otherwise("Other")) \
                           .withColumn("IsLatePayment", 
                                       col("DaysSinceLastPayment") > 30) \
                           .withColumn("IsLargePayment", 
                                       col("PaymentAmount") >= 5000)
    
    return enriched_df

# Process each table with improved transformations
try:
    # Process Customers
    customers_df = spark.read.parquet(f"{base_path}Customers/Customers.parquet")
    customers_processed = clean_and_validate_customers(customers_df)
    customers_processed.write.format("delta").mode("overwrite").save(f"{output_base_path}Customers/")
    logger.info("Customers processing completed")
    
    # Process Accounts
    accounts_df = spark.read.parquet(f"{base_path}Accounts/Accounts.parquet")
    accounts_processed = clean_and_validate_accounts(accounts_df)
    accounts_processed.write.format("delta").mode("overwrite").save(f"{output_base_path}Accounts/")
    logger.info("Accounts processing completed")
    
    # Process Loans
    loans_df = spark.read.parquet(f"{base_path}Loans/Loans.parquet")
    loans_processed = clean_and_validate_loans(loans_df)
    loans_processed.write.format("delta").mode("overwrite").save(f"{output_base_path}Loans/")
    logger.info("Loans processing completed")
    
    # Process Transactions
    transactions_df = spark.read.parquet(f"{base_path}Transactions/Transactions.parquet")
    transactions_processed = clean_and_validate_transactions(transactions_df)
    transactions_processed.write.format("delta").mode("overwrite").save(f"{output_base_path}Transactions/")
    logger.info("Transactions processing completed")
    
    # Process Payments
    payments_df = spark.read.parquet(f"{base_path}Payments/Payments.parquet")
    payments_processed = clean_and_validate_payments(payments_df)
    payments_processed.write.format("delta").mode("overwrite").save(f"{output_base_path}Payments/")
    logger.info("Payments processing completed")
    
    print("Bronze To Silver Processing Completed Successfully!")
    logger.info("All tables processed with comprehensive data validation and cleaning")
    
except Exception as e:
    logger.error(f"Error in Bronze to Silver processing: {str(e)}")
    print(f"Error: {str(e)}")
    raise

