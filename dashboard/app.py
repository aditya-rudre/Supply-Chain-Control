import streamlit as st
import pandas as pd
import sqlite3
import plotly.express as px
import os
import time

# ---------------------------------------------------------
# 1. Page Configuration
# ---------------------------------------------------------
st.set_page_config(
    page_title="Supply Chain Control Dashboard",
    page_icon="🚚",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for KPI cards
st.markdown("""
<style>
    .metric-card {
        background-color: #f9f9f9;
        border-left: 5px solid #FF4B4B;
        padding: 15px;
        border-radius: 5px;
        margin-bottom: 10px;
    }
</style>
""", unsafe_allow_html=True)

# ---------------------------------------------------------
# 2. Data Loading (Robust & Cached)
# ---------------------------------------------------------
DB_PATH = 'database/supply_chain_dw.db'

@st.cache_data(ttl=3600)  # Cache data for 1 hour
def load_data():
    """Loads analytical view from Data Warehouse."""
    if not os.path.exists(DB_PATH):
        return None
    
    try:
        conn = sqlite3.connect(DB_PATH)
        # Optimized query with region grouping
        query = """
        SELECT 
            f.order_id, 
            f.days_real, 
            f.days_scheduled, 
            f.late_delivery_risk, 
            f.sales_amount, 
            f.shipping_mode,
            f.order_status,
            d.market, 
            d.order_region
        FROM fact_orders f
        JOIN dim_location d ON f.location_id = d.location_id
        WHERE f.order_status = 'COMPLETE'
        """
        df = pd.read_sql(query, conn)
        conn.close()
        return df
    except Exception as e:
        st.error(f"Database Error: {e}")
        return None

# ---------------------------------------------------------
# 3. Main Dashboard Logic
# ---------------------------------------------------------

st.title("🚚 Predictive Supply Chain Control Dashboard")
st.markdown("Real-time monitoring of shipment delays and risk predictions.")

# Load Data
df = load_data()

if df is None:
    st.error("⚠️ **Database Not Found!**")
    st.info("Please run the ETL pipeline first to generate the Data Warehouse.")
    st.code("python run_pipeline.py", language="bash")
    st.stop()

# --- SIDEBAR FILTERS ---
st.sidebar.header("Filter Settings")
selected_market = st.sidebar.multiselect("Select Market", df['market'].unique(), default=df['market'].unique()[:3])
selected_mode = st.sidebar.selectbox("Shipping Mode", ["All"] + list(df['shipping_mode'].unique()))

# Apply Filters
df_filtered = df[df['market'].isin(selected_market)]
if selected_mode != "All":
    df_filtered = df_filtered[df_filtered['shipping_mode'] == selected_mode]

# --- KEY METRICS (KPIs) ---
total_orders = len(df_filtered)
late_orders = df_filtered[df_filtered['late_delivery_risk'] == 1].shape[0]
on_time_rate = 100 - (late_orders / total_orders * 100) if total_orders > 0 else 0
avg_sales = df_filtered['sales_amount'].mean()

col1, col2, col3, col4 = st.columns(4)
col1.metric("📦 Total Orders", f"{total_orders:,}")
col2.metric("⏱️ On-Time Rate", f"{on_time_rate:.1f}%", delta=f"{on_time_rate-85:.1f}% vs Target")
col3.metric("⚠️ Late Risk Count", f"{late_orders:,}", delta_color="inverse")
col4.metric("💰 Avg Sales per Order", f"${avg_sales:.2f}")

st.markdown("---")

# --- TABS FOR ANALYSIS ---
tab1, tab2 = st.tabs(["📊 Performance Analytics", "🌍 Regional Heatmap"])

with tab1:
    c1, c2 = st.columns(2)
    
    with c1:
        st.subheader("Shipping Mode Performance")
        # Comparison of Scheduled vs Real Days
        avg_days = df_filtered.groupby('shipping_mode')[['days_scheduled', 'days_real']].mean().reset_index()
        avg_days = avg_days.melt(id_vars='shipping_mode', var_name='Metric', value_name='Days')
        
        fig_days = px.bar(avg_days, x='shipping_mode', y='Days', color='Metric', barmode='group',
                          color_discrete_sequence=['#3b8ed0', '#e04f5f'])
        st.plotly_chart(fig_days, use_container_width=True)
    
    with c2:
        st.subheader("Late Delivery Risk by Region")
        risk_by_region = df_filtered[df_filtered['late_delivery_risk'] == 1].groupby('order_region').size().reset_index(name='Late Count')
        risk_by_region = risk_by_region.sort_values('Late Count', ascending=False).head(10)
        
        fig_risk = px.bar(risk_by_region, x='Late Count', y='order_region', orientation='h', 
                          color='Late Count', color_continuous_scale='Reds')
        st.plotly_chart(fig_risk, use_container_width=True)

with tab2:
    st.subheader("Global Delay Hotspots")
    # Simple Treemap for Market Analysis
    fig_tree = px.treemap(df_filtered, path=['market', 'order_region'], values='sales_amount',
                          color='late_delivery_risk', color_continuous_scale='RdYlGn_r',
                          title="Market Sales sized by Volume, Colored by Risk")
    st.plotly_chart(fig_tree, use_container_width=True)

# Footer
st.markdown("---")
st.caption(f"Data refreshed: {time.strftime('%Y-%m-%d %H:%M:%S')}")