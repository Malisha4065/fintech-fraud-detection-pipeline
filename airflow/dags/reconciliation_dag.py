"""
=============================================================================
FinTech Fraud Detection Pipeline - Batch Layer (Apache Airflow DAG)
=============================================================================
This DAG implements the batch processing layer of the Lambda Architecture.

Schedule: Every 6 hours
Tasks:
1. ETL: Read raw Parquet files, filter out already flagged fraud transactions
2. Warehouse: Save validated (non-fraud) data to warehouse location
3. Reconciliation Report: Compare Total Ingress vs Validated Amount
4. Analytic Report: Generate Fraud Attempts by Merchant Category report

=============================================================================
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import logging
import os
import pandas as pd
from pathlib import Path

# =============================================================================
# Configuration
# =============================================================================
logger = logging.getLogger(__name__)

# Paths
DATALAKE_RAW_PATH = "/opt/datalake/raw"
DATALAKE_VALIDATED_PATH = "/opt/datalake/validated_transactions"
REPORTS_PATH = "/opt/datalake/reports"

# PostgreSQL Configuration (for reading fraud alerts)
POSTGRES_HOST = os.environ.get('POSTGRES_HOST', 'postgres')
POSTGRES_PORT = os.environ.get('POSTGRES_PORT', '5432')
POSTGRES_DB = os.environ.get('POSTGRES_DB', 'fraud_db')
POSTGRES_USER = os.environ.get('POSTGRES_USER', 'fintech')
POSTGRES_PASSWORD = os.environ.get('POSTGRES_PASSWORD', 'fintech123')

# =============================================================================
# DAG Default Arguments
# =============================================================================
default_args = {
    'owner': 'fintech-team',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': days_ago(1),
}

# =============================================================================
# Task Functions
# =============================================================================

def get_postgres_connection():
    """Create a PostgreSQL connection using psycopg2."""
    import psycopg2
    return psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        database=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD
    )


def task_etl_raw_transactions(**context):
    """
    Task 1: ETL - Read raw Parquet files and filter out fraud transactions.
    
    This task:
    1. Reads all raw transaction Parquet files from the data lake
    2. Fetches fraud transaction IDs from PostgreSQL
    3. Filters out transactions that have been flagged as fraud
    4. Stores the clean data for the next task
    
    Returns:
        dict: Statistics about the ETL process
    """
    logger.info("=" * 60)
    logger.info("TASK 1: ETL - Processing Raw Transactions")
    logger.info("=" * 60)
    
    # Ensure paths exist
    Path(DATALAKE_VALIDATED_PATH).mkdir(parents=True, exist_ok=True)
    Path(REPORTS_PATH).mkdir(parents=True, exist_ok=True)
    
    # Check if raw data exists
    raw_path = Path(DATALAKE_RAW_PATH)
    if not raw_path.exists():
        logger.warning(f"Raw data path {DATALAKE_RAW_PATH} does not exist yet.")
        context['ti'].xcom_push(key='etl_stats', value={
            'total_raw_transactions': 0,
            'fraud_transactions': 0,
            'validated_transactions': 0,
            'total_raw_amount': 0.0,
            'validated_amount': 0.0
        })
        return "No raw data to process"
    
    # Find all Parquet files
    parquet_files = list(raw_path.rglob("*.parquet"))
    
    if not parquet_files:
        logger.warning("No Parquet files found in raw data path.")
        context['ti'].xcom_push(key='etl_stats', value={
            'total_raw_transactions': 0,
            'fraud_transactions': 0,
            'validated_transactions': 0,
            'total_raw_amount': 0.0,
            'validated_amount': 0.0
        })
        return "No Parquet files to process"
    
    logger.info(f"Found {len(parquet_files)} Parquet files to process")
    
    # Read all raw transactions
    try:
        raw_df = pd.concat([
            pd.read_parquet(f) for f in parquet_files
        ], ignore_index=True)
    except Exception as e:
        logger.error(f"Error reading Parquet files: {e}")
        raise
    
    total_raw_transactions = len(raw_df)
    total_raw_amount = raw_df['amount'].sum() if 'amount' in raw_df.columns else 0.0
    
    logger.info(f"Total raw transactions: {total_raw_transactions}")
    logger.info(f"Total raw amount: ${total_raw_amount:,.2f}")
    
    # Get fraud transaction IDs from PostgreSQL
    fraud_transaction_ids = set()
    try:
        conn = get_postgres_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT DISTINCT transaction_id FROM fraud_alerts WHERE transaction_id IS NOT NULL")
        fraud_transaction_ids = {row[0] for row in cursor.fetchall()}
        cursor.close()
        conn.close()
        logger.info(f"Found {len(fraud_transaction_ids)} fraud transaction IDs in database")
    except Exception as e:
        logger.warning(f"Could not fetch fraud IDs from PostgreSQL: {e}")
        logger.warning("Proceeding without fraud filtering from database")
    
    # Also filter high-value transactions (>$5000) as they are fraud
    if 'amount' in raw_df.columns:
        high_value_mask = raw_df['amount'] > 5000
        high_value_ids = set(raw_df.loc[high_value_mask, 'transaction_id'].tolist())
        fraud_transaction_ids.update(high_value_ids)
        logger.info(f"Added {len(high_value_ids)} high-value transaction IDs to fraud set")
    
    # Filter out fraud transactions
    if 'transaction_id' in raw_df.columns:
        validated_df = raw_df[~raw_df['transaction_id'].isin(fraud_transaction_ids)]
    else:
        validated_df = raw_df
    
    validated_transactions = len(validated_df)
    validated_amount = validated_df['amount'].sum() if 'amount' in validated_df.columns else 0.0
    fraud_count = total_raw_transactions - validated_transactions
    
    logger.info(f"Fraud transactions filtered: {fraud_count}")
    logger.info(f"Validated transactions: {validated_transactions}")
    logger.info(f"Validated amount: ${validated_amount:,.2f}")
    
    # Store statistics for downstream tasks
    etl_stats = {
        'total_raw_transactions': int(total_raw_transactions),
        'fraud_transactions': int(fraud_count),
        'validated_transactions': int(validated_transactions),
        'total_raw_amount': float(total_raw_amount),
        'validated_amount': float(validated_amount),
        'processing_time': datetime.utcnow().isoformat()
    }
    
    # Save validated data for next task
    context['ti'].xcom_push(key='etl_stats', value=etl_stats)
    context['ti'].xcom_push(key='validated_data_path', value=f"{REPORTS_PATH}/validated_temp.parquet")
    
    # Save temporary validated data
    validated_df.to_parquet(f"{REPORTS_PATH}/validated_temp.parquet", index=False)
    
    # Also keep raw_df info for analytics
    if 'merchant_category' in raw_df.columns:
        merchant_fraud_stats = raw_df[raw_df['transaction_id'].isin(fraud_transaction_ids)].groupby('merchant_category').agg({
            'transaction_id': 'count',
            'amount': 'sum'
        }).reset_index()
        merchant_fraud_stats.columns = ['merchant_category', 'fraud_count', 'fraud_amount']
        merchant_fraud_stats.to_parquet(f"{REPORTS_PATH}/merchant_fraud_temp.parquet", index=False)
        context['ti'].xcom_push(key='merchant_fraud_path', value=f"{REPORTS_PATH}/merchant_fraud_temp.parquet")
    
    logger.info("ETL task completed successfully")
    return etl_stats


def task_save_to_warehouse(**context):
    """
    Task 2: Warehouse - Save validated transactions to the data warehouse.
    
    This task:
    1. Reads the validated transactions from the previous task
    2. Saves them to the warehouse location in Parquet format
    3. Partitions by date for efficient querying
    """
    logger.info("=" * 60)
    logger.info("TASK 2: Saving Validated Data to Warehouse")
    logger.info("=" * 60)
    
    # Get validated data path from previous task
    validated_path = context['ti'].xcom_pull(key='validated_data_path', task_ids='etl_raw_transactions')
    
    if not validated_path or not Path(validated_path).exists():
        logger.warning("No validated data to save to warehouse")
        return "No data to save"
    
    # Read validated data
    validated_df = pd.read_parquet(validated_path)
    
    if validated_df.empty:
        logger.warning("Validated dataframe is empty")
        return "Empty dataframe"
    
    # Ensure warehouse path exists
    Path(DATALAKE_VALIDATED_PATH).mkdir(parents=True, exist_ok=True)
    
    # Add processing metadata
    validated_df['batch_processed_at'] = datetime.utcnow().isoformat()
    validated_df['batch_id'] = context['run_id']
    
    # Save to warehouse with timestamp-based partitioning
    timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
    output_path = f"{DATALAKE_VALIDATED_PATH}/batch_{timestamp}.parquet"
    
    validated_df.to_parquet(output_path, index=False)
    
    logger.info(f"Saved {len(validated_df)} validated transactions to: {output_path}")
    
    context['ti'].xcom_push(key='warehouse_path', value=output_path)
    
    return f"Saved {len(validated_df)} records to {output_path}"


def task_generate_reconciliation_report(**context):
    """
    Task 3: Generate Reconciliation Report.
    
    This task compares:
    - Total Ingress Amount (all raw transactions)
    - Validated Amount (non-fraud transactions)
    - Difference (fraudulent amount blocked)
    """
    logger.info("=" * 60)
    logger.info("TASK 3: Generating Reconciliation Report")
    logger.info("=" * 60)
    
    # Get ETL statistics from previous task
    etl_stats = context['ti'].xcom_pull(key='etl_stats', task_ids='etl_raw_transactions')
    
    if not etl_stats:
        logger.warning("No ETL stats available for reconciliation report")
        return "No data for reconciliation"
    
    # Calculate reconciliation metrics
    total_volume = etl_stats.get('total_raw_transactions', 0)
    validated_volume = etl_stats.get('validated_transactions', 0)
    fraud_volume = etl_stats.get('fraud_transactions', 0)
    
    total_amount = etl_stats.get('total_raw_amount', 0.0)
    validated_amount = etl_stats.get('validated_amount', 0.0)
    blocked_amount = total_amount - validated_amount
    
    fraud_rate = (fraud_volume / total_volume * 100) if total_volume > 0 else 0.0
    blocked_rate = (blocked_amount / total_amount * 100) if total_amount > 0 else 0.0
    
    # Generate report
    report = f"""
================================================================================
                    RECONCILIATION REPORT
                    {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}
================================================================================

VOLUME ANALYSIS
---------------
Total Transactions Processed:     {total_volume:,}
Validated Transactions:           {validated_volume:,}
Fraud Transactions Blocked:       {fraud_volume:,}
Fraud Rate:                       {fraud_rate:.2f}%

AMOUNT ANALYSIS
---------------
Total Ingress Amount:            ${total_amount:,.2f}
Validated Amount:                ${validated_amount:,.2f}
Fraudulent Amount Blocked:       ${blocked_amount:,.2f}
Block Rate:                      {blocked_rate:.2f}%

RECONCILIATION STATUS
---------------------
Status: {'✅ BALANCED' if total_volume == (validated_volume + fraud_volume) else '⚠️ DISCREPANCY DETECTED'}
Total = Validated + Fraud: {total_volume} = {validated_volume} + {fraud_volume}

================================================================================
                    END OF RECONCILIATION REPORT
================================================================================
"""
    
    # Log the report
    logger.info(report)
    
    # Save report to file
    Path(REPORTS_PATH).mkdir(parents=True, exist_ok=True)
    timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
    report_path = f"{REPORTS_PATH}/reconciliation_report_{timestamp}.txt"
    
    with open(report_path, 'w') as f:
        f.write(report)
    
    logger.info(f"Reconciliation report saved to: {report_path}")
    
    # Also save as CSV for programmatic access
    csv_data = {
        'metric': [
            'total_transactions', 'validated_transactions', 'fraud_transactions',
            'total_amount', 'validated_amount', 'blocked_amount',
            'fraud_rate_pct', 'blocked_rate_pct'
        ],
        'value': [
            total_volume, validated_volume, fraud_volume,
            total_amount, validated_amount, blocked_amount,
            fraud_rate, blocked_rate
        ]
    }
    
    csv_df = pd.DataFrame(csv_data)
    csv_path = f"{REPORTS_PATH}/reconciliation_report_{timestamp}.csv"
    csv_df.to_csv(csv_path, index=False)
    
    return report


def task_generate_analytic_report(**context):
    """
    Task 4: Generate Analytic Report - Fraud Attempts by Merchant Category.
    
    This task analyzes fraud patterns by merchant category to help
    identify high-risk business segments.
    """
    logger.info("=" * 60)
    logger.info("TASK 4: Generating Analytic Report - Fraud by Merchant Category")
    logger.info("=" * 60)
    
    # Get merchant fraud stats from ETL task
    merchant_fraud_path = context['ti'].xcom_pull(key='merchant_fraud_path', task_ids='etl_raw_transactions')
    
    if not merchant_fraud_path or not Path(merchant_fraud_path).exists():
        # Try to generate from raw data directly
        raw_path = Path(DATALAKE_RAW_PATH)
        if not raw_path.exists():
            logger.warning("No data available for analytic report")
            return "No data for analytics"
        
        parquet_files = list(raw_path.rglob("*.parquet"))
        if not parquet_files:
            logger.warning("No Parquet files found for analytics")
            return "No data for analytics"
        
        # Read and analyze
        raw_df = pd.concat([pd.read_parquet(f) for f in parquet_files], ignore_index=True)
        
        # Identify fraud (high value > $5000)
        if 'amount' in raw_df.columns and 'merchant_category' in raw_df.columns:
            fraud_df = raw_df[raw_df['amount'] > 5000]
            merchant_stats = fraud_df.groupby('merchant_category').agg({
                'transaction_id': 'count',
                'amount': 'sum'
            }).reset_index()
            merchant_stats.columns = ['merchant_category', 'fraud_count', 'fraud_amount']
        else:
            logger.warning("Required columns not found in data")
            return "Missing required columns"
    else:
        merchant_stats = pd.read_parquet(merchant_fraud_path)
    
    if merchant_stats.empty:
        logger.info("No fraud transactions found for analysis")
        return "No fraud data to analyze"
    
    # Sort by fraud count
    merchant_stats = merchant_stats.sort_values('fraud_count', ascending=False)
    
    # Calculate percentages
    total_fraud = merchant_stats['fraud_count'].sum()
    total_fraud_amount = merchant_stats['fraud_amount'].sum()
    
    merchant_stats['fraud_pct'] = (merchant_stats['fraud_count'] / total_fraud * 100).round(2)
    merchant_stats['amount_pct'] = (merchant_stats['fraud_amount'] / total_fraud_amount * 100).round(2)
    
    # Generate report
    report = f"""
================================================================================
                    ANALYTIC REPORT: Fraud Attempts by Merchant Category
                    {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}
================================================================================

SUMMARY
-------
Total Fraud Attempts:    {total_fraud:,}
Total Fraud Amount:      ${total_fraud_amount:,.2f}

BREAKDOWN BY MERCHANT CATEGORY
------------------------------
"""
    
    for _, row in merchant_stats.iterrows():
        report += f"""
{row['merchant_category'].upper()}
  - Fraud Count:    {int(row['fraud_count']):,} ({row['fraud_pct']:.1f}%)
  - Fraud Amount:   ${row['fraud_amount']:,.2f} ({row['amount_pct']:.1f}%)
"""
    
    report += f"""
================================================================================
TOP 3 HIGH-RISK CATEGORIES
================================================================================
"""
    
    for i, (_, row) in enumerate(merchant_stats.head(3).iterrows(), 1):
        report += f"""
{i}. {row['merchant_category'].upper()}: {int(row['fraud_count']):,} attempts (${row['fraud_amount']:,.2f})
"""
    
    report += """
================================================================================
                    END OF ANALYTIC REPORT
================================================================================
"""
    
    # Log the report
    logger.info(report)
    
    # Save report to file
    Path(REPORTS_PATH).mkdir(parents=True, exist_ok=True)
    timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
    report_path = f"{REPORTS_PATH}/analytic_report_{timestamp}.txt"
    
    with open(report_path, 'w') as f:
        f.write(report)
    
    # Save as CSV
    csv_path = f"{REPORTS_PATH}/fraud_by_merchant_{timestamp}.csv"
    merchant_stats.to_csv(csv_path, index=False)
    
    logger.info(f"Analytic report saved to: {report_path}")
    logger.info(f"CSV data saved to: {csv_path}")
    
    return report


# =============================================================================
# DAG Definition
# =============================================================================
with DAG(
    dag_id='fintech_fraud_reconciliation',
    default_args=default_args,
    description='FinTech Fraud Detection Pipeline - Batch Layer (Reconciliation)',
    schedule_interval='0 */6 * * *',  # Every 6 hours
    catchup=False,
    max_active_runs=1,
    tags=['fintech', 'fraud', 'reconciliation', 'batch'],
) as dag:
    
    # Task 1: ETL - Read and filter raw transactions
    etl_task = PythonOperator(
        task_id='etl_raw_transactions',
        python_callable=task_etl_raw_transactions,
        provide_context=True,
    )
    
    # Task 2: Save validated data to warehouse
    warehouse_task = PythonOperator(
        task_id='save_to_warehouse',
        python_callable=task_save_to_warehouse,
        provide_context=True,
    )
    
    # Task 3: Generate reconciliation report
    reconciliation_task = PythonOperator(
        task_id='generate_reconciliation_report',
        python_callable=task_generate_reconciliation_report,
        provide_context=True,
    )
    
    # Task 4: Generate analytic report
    analytic_task = PythonOperator(
        task_id='generate_analytic_report',
        python_callable=task_generate_analytic_report,
        provide_context=True,
    )
    
    # Define task dependencies
    # ETL must run first, then warehouse and reports can run in parallel
    etl_task >> warehouse_task
    etl_task >> reconciliation_task
    etl_task >> analytic_task
