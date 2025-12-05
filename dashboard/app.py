"""
FinTech Fraud Detection Dashboard
Serving Layer Visualization for Lambda Architecture
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import psycopg2
from psycopg2.extras import RealDictCursor
import os
import glob
import time

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

def render_sidebar():
    """Render the sidebar."""
    with st.sidebar:
        st.image("https://img.icons8.com/color/96/000000/fraud.png", width=80)
        st.title("Navigation")
        
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

# =============================================================================
# Main Application
# =============================================================================
def main():
    render_sidebar()
    render_header()
    
    # Create tabs
    tab1, tab2 = st.tabs(["⚡ Real-Time Alerts", "📚 Historical Analysis"])
    
    with tab1:
        render_realtime_tab()
    
    with tab2:
        render_historical_tab()

if __name__ == "__main__":
    # Auto-refresh for real-time tab
    # Using st.rerun() with time-based refresh
    main()
    
    # Auto-refresh every 5 seconds
    time.sleep(5)
    st.rerun()
