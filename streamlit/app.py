import streamlit as st
import pandas as pd
import psycopg2
import plotly.express as px
import pydeck as pdk
from datetime import datetime

st.set_page_config(page_title="NYC Taxi Full Analytics", layout="wide")

def get_connection():
    return psycopg2.connect(
        host="localhost",
        database="nyc_taxi", 
        user="nyc_taxi",         
        password="nyc_taxi" 
    )

# --- HÀM TRUY VẤN DỮ LIỆU ---
@st.cache_data
def run_query(query):
    try:
        with get_connection() as conn:
            return pd.read_sql(query, conn)
    except Exception as e:
        st.error(f"Lỗi SQL: {e}")
        return pd.DataFrame()

st.sidebar.header("🕹️ Bộ lọc Dashboard")
service_mapping = {"Yellow": 1, "Green": 2}
date_range = st.sidebar.date_input(
    "Khoảng thời gian",
    [datetime(2025, 1, 1), datetime(2025, 12, 31)]
)
service_type_names = st.sidebar.multiselect(
    "Loại hình dịch vụ",
    options=list(service_mapping.keys()),
    default=list(service_mapping.keys())
)

# Chặn lỗi khi chưa chọn đủ filter
if len(date_range) < 2 or not service_type_names:
    st.info("Vui lòng chọn đầy đủ ngày và loại xe ở thanh bên.")
    st.stop()

start_date = date_range[0].strftime('%Y-%m-%d')
end_date = date_range[1].strftime('%Y-%m-%d')
selected_ids = [service_mapping[name] for name in service_type_names]
services_sql = f"({', '.join(map(str, selected_ids))})"

# --- GIAO DIỆN CHÍNH ---
st.title("🚕 NYC Taxi Dashboard")

# 1. TỔNG QUAN (METRICS)
metric_query = f"""
    SELECT count(*) as total_trips, sum(total_amount) as total_revenue, 
           avg(tip_amount) as avg_tip, avg(trip_distance) as avg_dist
    FROM production.fact_trip
    WHERE pickup_datetime >= '{start_date}' AND pickup_datetime <= '{end_date}'
    AND service_type_id IN {services_sql}
"""
df_m = run_query(metric_query)
if not df_m.empty:
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Tổng chuyến đi", f"{df_m['total_trips'][0]:,.0f}")
    c2.metric("Doanh thu", f"${df_m['total_revenue'][0]:,.0f}")
    c3.metric("Tip TB", f"${df_m['avg_tip'][0]:.2f}")
    c4.metric("Quãng đường TB", f"{df_m['avg_dist'][0]:.2f} mi")

st.divider()

# TẠO TABS ĐỂ PHÂN CHIA DỮ LIỆU
tab1, tab2, tab3 = st.tabs(["📈 Tổng quan xu hướng", "🔍 Phân tích chuyên sâu", "🗺️ Địa lý & Phân bổ"])

with tab1:
    # XU HƯỚNG THEO THỜI GIAN
    st.subheader("Xu hướng nhu cầu hàng ngày")
    df_time = run_query(f"""
        SELECT date_trunc('day', pickup_datetime) as date, count(*) as trips
        FROM production.fact_trip
        WHERE pickup_datetime >= '{start_date}' AND pickup_datetime <= '{end_date}'
        AND service_type_id IN {services_sql}
        GROUP BY 1 ORDER BY 1
    """)
    if not df_time.empty:
        st.plotly_chart(px.line(df_time, x='date', y='trips', title="Số lượng chuyến đi"), use_container_width=True)

    # HÌNH THỨC THANH TOÁN & PHÂN BỔ GIÁ VÉ
    col_l, col_r = st.columns(2)
    with col_l:
        df_pay = run_query(f"""
            SELECT p.payment_description, count(*) as count FROM production.fact_trip f
            JOIN production.dim_payment p ON f.payment_type_key = p.payment_type_key
            WHERE f.service_type_id IN {services_sql} GROUP BY 1
        """)
        st.plotly_chart(px.pie(df_pay, values='count', names='payment_description', title="Hình thức thanh toán", hole=.4), use_container_width=True)
    with col_r:
        df_fare = run_query(f"SELECT fare_amount FROM production.fact_trip WHERE service_type_id IN {services_sql} LIMIT 5000")
        st.plotly_chart(px.histogram(df_fare, x="fare_amount", nbins=50, title="Phân bổ giá vé"), use_container_width=True)

with tab2:
    # GIỜ CAO ĐIỂM (HEATMAP)
    st.subheader("Ma trận giờ cao điểm trong tuần")
    df_heat = run_query(f"""
        SELECT EXTRACT(DOW FROM pickup_datetime) as dow, EXTRACT(HOUR FROM pickup_datetime) as hour, COUNT(*) as trips
        FROM production.fact_trip WHERE service_type_id IN {services_sql} GROUP BY 1, 2
    """)
    if not df_heat.empty:
        df_heat['day'] = df_heat['dow'].map({0:'Sun', 1:'Mon', 2:'Tue', 3:'Wed', 4:'Thu', 5:'Fri', 6:'Sat'})
        st.plotly_chart(px.density_heatmap(df_heat, x='hour', y='day', z='trips', color_continuous_scale='Viridis',
                                           category_orders={"day": ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]}), use_container_width=True)

    # QUÃNG ĐƯỜNG VS GIÁ VÉ
    st.subheader("Tương quan Quãng đường & Giá vé")
    df_scat = run_query(f"SELECT trip_distance, total_amount FROM production.fact_trip WHERE trip_distance > 0 LIMIT 2000")
    st.plotly_chart(px.scatter(df_scat, x="trip_distance", y="total_amount", trendline="ols", opacity=0.5), use_container_width=True)

with tab3:
    col_3a, col_3b = st.columns(2)
    with col_3a:
        st.subheader("💰 1. Hiệu suất doanh thu trên mỗi dặm (Revenue per Mile)")
        # Phân tích xem khung giờ nào kiếm được nhiều tiền nhất trên 1 đơn vị khoảng cách
        rev_mile_query = f"""
            SELECT 
                EXTRACT(HOUR FROM pickup_datetime) as hour_of_day,
                SUM(total_amount) / NULLIF(SUM(trip_distance), 0) as rev_per_mile
            FROM production.fact_trip
            WHERE pickup_datetime >= '{start_date}' AND pickup_datetime <= '{end_date}'
            AND service_type_id IN {services_sql}
            AND trip_distance > 0 -- Chỉ lấy các chuyến có quãng đường hợp lệ
            GROUP BY 1 
            ORDER BY 1
        """
        df_rev_mile = run_query(rev_mile_query)
        
        if not df_rev_mile.empty:
            fig_rev = px.area(
                df_rev_mile, 
                x='hour_of_day', 
                y='rev_per_mile',
                title="Số tiền kiếm được ($) trên mỗi dặm theo khung giờ",
                labels={'hour_of_day': 'Giờ trong ngày (0-23h)', 'rev_per_mile': 'USD / Dặm'},
                color_discrete_sequence=['#2E8B57'] # Màu xanh lục đậm (SeaGreen)
            )
            # Làm mượt biểu đồ và thêm ký hiệu $
            fig_rev.update_layout(yaxis_tickprefix='$', hovermode="x unified")
            st.plotly_chart(fig_rev, use_container_width=True)
        else:
            st.info("Không có dữ liệu quãng đường để tính hiệu suất.")
    
    with col_3b:
        st.subheader("Tỷ lệ Chuyến đi Sân bay")
        df_air = run_query(f"""
            SELECT CASE WHEN pickup_location_id IN (132, 138) THEN 'Airport' ELSE 'City' END as type, COUNT(*) as count
            FROM production.fact_trip GROUP BY 1
        """)
        st.plotly_chart(px.pie(df_air, values='count', names='type', hole=.5), use_container_width=True)