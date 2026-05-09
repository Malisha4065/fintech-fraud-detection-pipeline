"""
FinTech Fraud Detection Dashboard
Serving Layer Visualization for Lambda Architecture
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import duckdb
from datetime import datetime
import psycopg2
import os
import glob
import time
from pathlib import Path

# =============================================================================
# Configuration
# =============================================================================
st.set_page_config(
    page_title="FinTech Fraud Detection Dashboard",
    page_icon="🔒",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Database configuration
DB_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "postgres"),
    "port": os.getenv("POSTGRES_PORT", "5432"),
    "database": os.getenv("POSTGRES_DB", "fraud_db"),
    "user": os.getenv("POSTGRES_USER", "fintech"),
    "password": os.getenv("POSTGRES_PASSWORD", "fintech123")
}

# Datalake paths
DATALAKE_RAW = "/opt/datalake/raw"
DATALAKE_VALIDATED = "/opt/datalake/validated_transactions"
DATALAKE_REPORTS = "/opt/datalake/reports"

# =============================================================================
# Database Connection
# =============================================================================
@st.cache_resource
def get_db_connection():
    """Create a database connection."""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        return conn
    except Exception as e:
        st.error(f"Database connection failed: {e}")
        return None

def execute_query(query, params=None):
    """Execute a query and return results as DataFrame."""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        df = pd.read_sql_query(query, conn, params=params)
        conn.close()
        return df
    except Exception as e:
        st.error(f"Query failed: {e}")
        return pd.DataFrame()

# =============================================================================
# Data Loading Functions
# =============================================================================
def load_fraud_alerts(limit=10):
    """Load latest fraud alerts from PostgreSQL."""
    query = """
        SELECT 
            transaction_id,
            user_id,
            fraud_type,
            fraud_reason,
            amount,
            country,
            merchant_category,
            detected_at
        FROM fraud_alerts 
        ORDER BY detected_at DESC 
        LIMIT %s
    """
    return execute_query(query, (limit,))

def get_fraud_kpis():
    """Get KPI metrics from fraud alerts."""
    query = """
        SELECT 
            COUNT(*) as total_alerts,
            COALESCE(SUM(amount), 0) as total_value,
            COUNT(DISTINCT user_id) as unique_users,
            COUNT(CASE WHEN fraud_type = 'HIGH_VALUE' THEN 1 END) as high_value_count,
            COUNT(CASE WHEN fraud_type = 'IMPOSSIBLE_TRAVEL' THEN 1 END) as impossible_travel_count
        FROM fraud_alerts
    """
    df = execute_query(query)
    if not df.empty:
        return df.iloc[0].to_dict()
    return {
        "total_alerts": 0, 
        "total_value": 0, 
        "unique_users": 0,
        "high_value_count": 0,
        "impossible_travel_count": 0
    }

def get_alerts_by_type():
    """Get fraud alerts grouped by type."""
    query = """
        SELECT 
            fraud_type,
            COUNT(*) as count,
            COALESCE(SUM(amount), 0) as total_amount
        FROM fraud_alerts
        GROUP BY fraud_type
        ORDER BY count DESC
    """
    return execute_query(query)

def get_alerts_timeline():
    """Get fraud alerts over time."""
    query = """
        SELECT 
            DATE_TRUNC('minute', detected_at) as time_bucket,
            COUNT(*) as alert_count,
            COALESCE(SUM(amount), 0) as total_amount
        FROM fraud_alerts
        WHERE detected_at > NOW() - INTERVAL '1 hour'
        GROUP BY DATE_TRUNC('minute', detected_at)
        ORDER BY time_bucket
    """
    return execute_query(query)

def load_parquet_data():
    """Load data from Parquet files."""
    # Try validated transactions first, then raw
    for path in [DATALAKE_VALIDATED, DATALAKE_RAW]:
        if os.path.exists(path):
            parquet_files = glob.glob(os.path.join(path, "**/*.parquet"), recursive=True)
            if parquet_files:
                try:
                    dfs = []
                    for f in parquet_files[:50]:  # Limit files to avoid memory issues
                        dfs.append(pd.read_parquet(f))
                    if dfs:
                        return pd.concat(dfs, ignore_index=True)
                except Exception as e:
                    st.warning(f"Error reading parquet files: {e}")
    return None

def get_fraud_alert_count():
    """Return total fraud alert rows for paginated browsing."""
    df = execute_query("SELECT COUNT(*) AS total FROM fraud_alerts")
    if df.empty:
        return 0
    return int(df.iloc[0]["total"])

def load_paginated_fraud_alerts(limit, offset):
    """Load one page of speed-layer fraud alerts from PostgreSQL."""
    query = """
        SELECT
            id,
            transaction_id,
            user_id,
            fraud_type,
            amount,
            country,
            location,
            merchant_category,
            detected_at,
            created_at,
            fraud_reason
        FROM fraud_alerts
        ORDER BY created_at DESC, id DESC
        LIMIT %s OFFSET %s
    """
    return execute_query(query, (limit, offset))

def get_parquet_files(path):
    """Return parquet files below a data lake path."""
    if not os.path.exists(path):
        return []
    return glob.glob(os.path.join(path, "**/*.parquet"), recursive=True)

def parquet_count(path):
    """Count rows in Parquet files without loading them into pandas."""
    parquet_files = get_parquet_files(path)
    if not parquet_files:
        return 0

    pattern = os.path.join(path, "**/*.parquet").replace("'", "''")
    query = f"SELECT COUNT(*) AS total FROM read_parquet('{pattern}', hive_partitioning=true)"
    try:
        return int(duckdb.sql(query).fetchone()[0])
    except Exception as e:
        st.error(f"Could not count Parquet rows: {e}")
        return 0

def load_paginated_parquet(path, limit, offset):
    """Load one page from Parquet files using DuckDB LIMIT/OFFSET."""
    parquet_files = get_parquet_files(path)
    if not parquet_files:
        return pd.DataFrame()

    pattern = os.path.join(path, "**/*.parquet").replace("'", "''")
    query = f"""
        SELECT *
        FROM read_parquet('{pattern}', hive_partitioning=true)
        LIMIT {int(limit)} OFFSET {int(offset)}
    """
    try:
        return duckdb.sql(query).df()
    except Exception as e:
        st.error(f"Could not load Parquet page: {e}")
        return pd.DataFrame()

def get_report_files():
    """Return generated Airflow report files."""
    if not os.path.exists(DATALAKE_REPORTS):
        return []
    return sorted(
        [p for p in Path(DATALAKE_REPORTS).glob("*") if p.is_file()],
        key=lambda p: p.stat().st_mtime,
        reverse=True
    )

def render_pagination(total_rows, key_prefix):
    """Render reusable pagination controls and return limit/offset."""
    if total_rows <= 0:
        return 25, 0

    page_key = f"{key_prefix}_page"
    if page_key not in st.session_state:
        st.session_state[page_key] = 1

    page_size = st.selectbox(
        "Rows per page",
        options=[10, 25, 50, 100, 250],
        index=1,
        key=f"{key_prefix}_page_size"
    )
    total_pages = max((total_rows + page_size - 1) // page_size, 1)
    st.session_state[page_key] = min(max(int(st.session_state[page_key]), 1), total_pages)

    # Do not set max_value here. For live sources, total_pages changes while
    # records arrive, and Streamlit resets widgets when bounds change.
    page = st.number_input(
        "Page",
        min_value=1,
        step=1,
        key=page_key
    )
    if page > total_pages:
        page = total_pages
        st.session_state[page_key] = total_pages

    offset = (page - 1) * page_size
    st.caption(f"Showing page {page:,} of {total_pages:,} | Total rows: {total_rows:,}")
    return page_size, offset

# =============================================================================
# UI Components
# =============================================================================
def render_header():
    """Render the dashboard header."""
    st.title("🔒 FinTech Fraud Detection Dashboard")
    st.markdown("**Lambda Architecture Serving Layer** - Real-time and Batch Analytics")
    st.divider()

def render_realtime_tab():
    """Render the Real-Time Alerts tab."""
    st.header("⚡ Real-Time Fraud Alerts")
    st.caption("Auto-refreshes every 5 seconds | Data from PostgreSQL (Speed Layer)")
    
    # KPIs Row
    kpis = get_fraud_kpis()
    
    col1, col2, col3, col4, col5 = st.columns(5)
    
    with col1:
        st.metric(
            label="🚨 Total Alerts",
            value=f"{kpis['total_alerts']:,}"
        )
    
    with col2:
        st.metric(
            label="💰 Total Fraud Value",
            value=f"${kpis['total_value']:,.2f}"
        )
    
    with col3:
        st.metric(
            label="👥 Unique Users",
            value=f"{kpis['unique_users']:,}"
        )
    
    with col4:
        st.metric(
            label="💵 High Value",
            value=f"{kpis['high_value_count']:,}"
        )
    
    with col5:
        st.metric(
            label="✈️ Impossible Travel",
            value=f"{kpis['impossible_travel_count']:,}"
        )
    
    st.divider()
    
    # Charts Row
    col_chart1, col_chart2 = st.columns(2)
    
    with col_chart1:
        st.subheader("📊 Alerts by Fraud Type")
        alerts_by_type = get_alerts_by_type()
        if not alerts_by_type.empty:
            fig = px.pie(
                alerts_by_type, 
                values='count', 
                names='fraud_type',
                color_discrete_sequence=px.colors.qualitative.Set2,
                hole=0.4
            )
            fig.update_layout(margin=dict(t=20, b=20, l=20, r=20))
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No fraud alerts yet")
    
    with col_chart2:
        st.subheader("📈 Alerts Timeline (Last Hour)")
        timeline = get_alerts_timeline()
        if not timeline.empty:
            fig = px.line(
                timeline, 
                x='time_bucket', 
                y='alert_count',
                markers=True,
                color_discrete_sequence=['#FF6B6B']
            )
            fig.update_layout(
                xaxis_title="Time",
                yaxis_title="Alert Count",
                margin=dict(t=20, b=20, l=20, r=20)
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No alerts in the last hour")
    
    st.divider()
    
    # Latest Alerts Table
    st.subheader("🔍 Latest 10 Fraud Alerts")
    alerts_df = load_fraud_alerts(limit=10)
    
    if not alerts_df.empty:
        # Format the dataframe for display
        display_df = alerts_df.copy()
        if 'amount' in display_df.columns:
            display_df['amount'] = display_df['amount'].apply(lambda x: f"${x:,.2f}" if pd.notna(x) else "N/A")
        if 'detected_at' in display_df.columns:
            display_df['detected_at'] = pd.to_datetime(display_df['detected_at']).dt.strftime('%Y-%m-%d %H:%M:%S')
        
        st.dataframe(
            display_df,
            use_container_width=True,
            hide_index=True,
            column_config={
                "transaction_id": st.column_config.TextColumn("Transaction ID", width="medium"),
                "user_id": st.column_config.TextColumn("User ID", width="small"),
                "fraud_type": st.column_config.TextColumn("Fraud Type", width="small"),
                "fraud_reason": st.column_config.TextColumn("Reason", width="large"),
                "amount": st.column_config.TextColumn("Amount", width="small"),
                "country": st.column_config.TextColumn("Country", width="medium"),
                "merchant_category": st.column_config.TextColumn("Merchant", width="small"),
                "detected_at": st.column_config.TextColumn("Detected At", width="medium"),
            }
        )
    else:
        st.info("🔍 No fraud alerts detected yet. The system is monitoring transactions...")
    
    # Last update timestamp
    st.caption(f"Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

def render_historical_tab():
    """Render the Historical Analysis tab."""
    st.header("📚 Historical Analysis")
    st.caption("Batch Layer Analytics from Parquet Files")
    
    # Load parquet data
    df = load_parquet_data()
    
    if df is None or df.empty:
        st.warning("⏳ No historical data available yet.")
        st.info("""
        **Why is this empty?**
        - The Spark Streaming job archives raw transactions to Parquet files every batch.
        - It may take a few minutes for the first files to be written.
        - Check that the `spark-job` container is running.
        
        **Expected paths:**
        - `/opt/datalake/raw` - Raw transaction archives
        - `/opt/datalake/validated_transactions` - Validated clean data
        """)
        return
    
    # Data info
    st.success(f"✅ Loaded {len(df):,} historical transactions")
    
    # KPIs Row
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("📊 Total Transactions", f"{len(df):,}")
    
    with col2:
        if 'amount' in df.columns:
            st.metric("💵 Total Volume", f"${df['amount'].sum():,.2f}")
        else:
            st.metric("💵 Total Volume", "N/A")
    
    with col3:
        if 'user_id' in df.columns:
            st.metric("👥 Unique Users", f"{df['user_id'].nunique():,}")
        else:
            st.metric("👥 Unique Users", "N/A")
    
    with col4:
        if 'country' in df.columns:
            st.metric("🌍 Countries", f"{df['country'].nunique():,}")
        else:
            st.metric("🌍 Countries", "N/A")
    
    st.divider()
    
    # Charts
    col_chart1, col_chart2 = st.columns(2)
    
    with col_chart1:
        st.subheader("🌍 Transactions per Country")
        if 'country' in df.columns:
            country_counts = df['country'].value_counts().head(10).reset_index()
            country_counts.columns = ['country', 'count']
            
            fig = px.bar(
                country_counts,
                x='country',
                y='count',
                color='count',
                color_continuous_scale='Blues'
            )
            fig.update_layout(
                xaxis_title="Country",
                yaxis_title="Transaction Count",
                margin=dict(t=20, b=20, l=20, r=20),
                showlegend=False
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Country data not available")
    
    with col_chart2:
        st.subheader("📈 Transaction Volume Over Time")
        if 'timestamp' in df.columns or 'event_time' in df.columns:
            time_col = 'timestamp' if 'timestamp' in df.columns else 'event_time'
            df_time = df.copy()
            df_time[time_col] = pd.to_datetime(df_time[time_col])
            df_time['hour'] = df_time[time_col].dt.floor('H')
            
            hourly = df_time.groupby('hour').size().reset_index(name='count')
            
            fig = px.line(
                hourly,
                x='hour',
                y='count',
                markers=True,
                color_discrete_sequence=['#4CAF50']
            )
            fig.update_layout(
                xaxis_title="Time",
                yaxis_title="Transaction Count",
                margin=dict(t=20, b=20, l=20, r=20)
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Time data not available")
    
    st.divider()
    
    # Additional Analysis
    col_extra1, col_extra2 = st.columns(2)
    
    with col_extra1:
        st.subheader("🏪 Top Merchant Categories")
        if 'merchant_category' in df.columns:
            merchant_counts = df['merchant_category'].value_counts().head(10).reset_index()
            merchant_counts.columns = ['category', 'count']
            
            fig = px.bar(
                merchant_counts,
                x='count',
                y='category',
                orientation='h',
                color='count',
                color_continuous_scale='Greens'
            )
            fig.update_layout(
                xaxis_title="Count",
                yaxis_title="",
                margin=dict(t=20, b=20, l=20, r=20),
                showlegend=False
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Merchant category data not available")
    
    with col_extra2:
        st.subheader("💰 Amount Distribution")
        if 'amount' in df.columns:
            fig = px.histogram(
                df,
                x='amount',
                nbins=50,
                color_discrete_sequence=['#9C27B0']
            )
            fig.update_layout(
                xaxis_title="Transaction Amount ($)",
                yaxis_title="Count",
                margin=dict(t=20, b=20, l=20, r=20)
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Amount data not available")
    
    # Sample Data Preview
    st.divider()
    st.subheader("📋 Sample Data Preview")
    st.dataframe(df.head(20), use_container_width=True, hide_index=True)

def render_data_explorer_tab():
    """Render paginated browsing for speed and batch outputs."""
    st.header("🗂️ Data Explorer")
    st.caption("Paginated browsing for speed-layer outputs and batch/data lake outputs")

    source = st.selectbox(
        "Choose data source",
        [
            "Speed Output: PostgreSQL fraud_alerts",
            "Raw Data Lake: datalake/raw Parquet",
            "Batch Output: datalake/validated_transactions Parquet",
            "Batch Reports: datalake/reports"
        ],
        key="data_explorer_source"
    )

    st.divider()

    if source == "Speed Output: PostgreSQL fraud_alerts":
        st.subheader("Speed Output: PostgreSQL fraud_alerts")
        total_rows = get_fraud_alert_count()
        if total_rows == 0:
            st.info("No fraud alerts found yet.")
            return

        page_size, offset = render_pagination(total_rows, "fraud_alerts")
        df = load_paginated_fraud_alerts(page_size, offset)
        st.dataframe(df, use_container_width=True, hide_index=True)
        return

    if source == "Raw Data Lake: datalake/raw Parquet":
        st.subheader("Raw Data Lake: datalake/raw")
        st.caption("This is the raw Parquet archive written continuously by Spark Streaming.")
        total_rows = parquet_count(DATALAKE_RAW)
        if total_rows == 0:
            st.info("No raw Parquet rows found yet.")
            return

        page_size, offset = render_pagination(total_rows, "raw_parquet")
        df = load_paginated_parquet(DATALAKE_RAW, page_size, offset)
        st.dataframe(df, use_container_width=True, hide_index=True)
        return

    if source == "Batch Output: datalake/validated_transactions Parquet":
        st.subheader("Batch Output: datalake/validated_transactions")
        st.caption("This is the validated non-fraud warehouse output produced by the Airflow DAG.")
        total_rows = parquet_count(DATALAKE_VALIDATED)
        if total_rows == 0:
            st.info("No validated Parquet rows found yet. Trigger the Airflow DAG to create this output.")
            return

        page_size, offset = render_pagination(total_rows, "validated_parquet")
        df = load_paginated_parquet(DATALAKE_VALIDATED, page_size, offset)
        st.dataframe(df, use_container_width=True, hide_index=True)
        return

    st.subheader("Batch Reports: datalake/reports")
    report_files = get_report_files()
    if not report_files:
        st.info("No reports found yet. Trigger the Airflow DAG to generate reconciliation and analytic reports.")
        return

    total_rows = len(report_files)
    page_size, offset = render_pagination(total_rows, "report_files")
    page_files = report_files[offset:offset + page_size]
    report_df = pd.DataFrame([
        {
            "file_name": p.name,
            "size_bytes": p.stat().st_size,
            "modified_at": datetime.fromtimestamp(p.stat().st_mtime).strftime("%Y-%m-%d %H:%M:%S")
        }
        for p in page_files
    ])
    st.dataframe(report_df, use_container_width=True, hide_index=True)

    selected_name = st.selectbox(
        "Preview report file",
        [p.name for p in page_files],
        key="report_preview_file"
    )
    selected_file = next((p for p in page_files if p.name == selected_name), None)
    if selected_file is None:
        return

    if selected_file.suffix.lower() == ".txt":
        st.text(selected_file.read_text(errors="replace"))
    elif selected_file.suffix.lower() == ".csv":
        st.dataframe(pd.read_csv(selected_file), use_container_width=True, hide_index=True)
    elif selected_file.suffix.lower() == ".parquet":
        st.dataframe(pd.read_parquet(selected_file).head(100), use_container_width=True, hide_index=True)
    else:
        st.info("Preview is available for .txt, .csv, and .parquet report files.")

def render_sidebar():
    """Render the sidebar."""
    with st.sidebar:
        st.image("https://img.icons8.com/color/96/000000/fraud.png", width=80)
        st.title("Navigation")
        
        st.divider()
        
        auto_refresh = st.checkbox(
            "Auto-refresh every 5 seconds",
            value=True,
            help="Turn this off while paging through the Data Explorer."
        )
        
        st.divider()
        
        st.markdown("### 🏗️ Architecture")
        st.markdown("""
        - **Speed Layer**: Spark Streaming → PostgreSQL
        - **Batch Layer**: Parquet Files
        - **Serving Layer**: This Dashboard
        """)
        
        st.divider()
        
        st.markdown("### 📊 Data Sources")
        st.markdown("""
        - **Real-Time**: `fraud_alerts` table
        - **Historical**: `/opt/datalake/`
        """)
        
        st.divider()
        
        st.markdown("### 🔗 Quick Links")
        st.markdown("""
        - [Spark UI](http://localhost:8080)
        - [Airflow](http://localhost:8082)
        - [Kafka UI](http://localhost:8084)
        """)
        
        st.divider()
        
        # System Status
        st.markdown("### 🖥️ System Status")
        try:
            conn = psycopg2.connect(**DB_CONFIG)
            conn.close()
            st.success("✅ PostgreSQL Connected")
        except:
            st.error("❌ PostgreSQL Disconnected")
        
        if os.path.exists(DATALAKE_RAW) or os.path.exists(DATALAKE_VALIDATED):
            st.success("✅ Datalake Mounted")
        else:
            st.warning("⚠️ Datalake Not Found")
        
        return auto_refresh

# =============================================================================
# Main Application
# =============================================================================
def main():
    auto_refresh = render_sidebar()
    render_header()
    
    # Create tabs
    tab1, tab2, tab3 = st.tabs(["⚡ Real-Time Alerts", "📚 Historical Analysis", "🗂️ Data Explorer"])
    
    with tab1:
        render_realtime_tab()
    
    with tab2:
        render_historical_tab()

    with tab3:
        render_data_explorer_tab()
    
    return auto_refresh

if __name__ == "__main__":
    auto_refresh = main()
    
    # Auto-refresh every 5 seconds
    if auto_refresh:
        time.sleep(5)
        st.rerun()
