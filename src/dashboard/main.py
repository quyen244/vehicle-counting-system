# app.py
import streamlit as st
import plotly.express as px
from streamlit_autorefresh import st_autorefresh
import psycopg2
import pandas as pd 


class DBConfig:
    DB_NAME = "traffic_db"
    USER = "postgres"
    PASSWORD = "1234"
    HOST = "localhost"
    PORT = 5432

    @classmethod
    def get_conn_str(cls):
        return (
            f"dbname={cls.DB_NAME} "
            f"user={cls.USER} "
            f"password={cls.PASSWORD} "
            f"host={cls.HOST} "
            f"port={cls.PORT}"
        )

class Database:
    def __init__(self):
        self.conn_str = DBConfig.get_conn_str()

    def fetch_vehicle_data(self, limit=500):
        query = f"""
            SELECT 
                cam_id,
                vehicle_count,
                created_at
            FROM vehicle_counts
            ORDER BY created_at DESC
            LIMIT {limit}
        """
        with psycopg2.connect(self.conn_str) as conn:
            df = pd.read_sql(query, conn)
        return df



st.set_page_config(
    page_title="Vehicle Dashboard",
    layout="wide"
)

st.title("🚦 Real-Time Vehicle Counting Dashboard")

# Auto refresh mỗi 2 giây (chuẩn hơn while True)
st_autorefresh(interval=2000, key="datarefresh")

db = Database()
df = db.fetch_vehicle_data()

if df.empty:
    st.warning("No data available")
    st.stop()

# ======================
# KPIs
# ======================
kpi1, kpi2, kpi3 = st.columns(3)

kpi1.metric(
    "🚗 Total Vehicles",
    int(df["vehicle_count"].sum())
)

kpi2.metric(
    "📷 Active Cameras",
    df["cam_id"].nunique()
)

kpi3.metric(
    "📊 Avg Vehicles / Camera",
    round(df["vehicle_count"].mean(), 2)
)

st.divider()

# ======================
# Biểu đồ hàng 1
# ======================
col1, col2 = st.columns(2)

with col1:
    fig_line = px.line(
        df,
        x="created_at",
        y="vehicle_count",
        color="cam_id",
        title="📈 Vehicle Count Over Time"
    )
    st.plotly_chart(fig_line, use_container_width=True)

with col2:
    cam_sum = df.groupby("cam_id")["vehicle_count"].sum().reset_index()
    fig_bar = px.bar(
        cam_sum,
        x="cam_id",
        y="vehicle_count",
        title="📊 Total Vehicles per Camera"
    )
    st.plotly_chart(fig_bar, use_container_width=True)

# ======================
# Biểu đồ hàng 2
# ======================
col3, col4 = st.columns(2)

with col3:
    fig_pie = px.pie(
        cam_sum,
        names="cam_id",
        values="vehicle_count",
        title="📷 Traffic Distribution by Camera"
    )
    st.plotly_chart(fig_pie, use_container_width=True)

with col4:
    df["hour"] = df["created_at"].dt.hour
    heatmap_data = df.groupby(["cam_id", "hour"])["vehicle_count"].mean().reset_index()

    fig_heat = px.density_heatmap(
        heatmap_data,
        x="hour",
        y="cam_id",
        z="vehicle_count",
        title="🔥 Traffic Heatmap (Camera vs Hour)",
        color_continuous_scale="Turbo"
    )
    st.plotly_chart(fig_heat, use_container_width=True)
