import streamlit as st
import pandas as pd
import psycopg2
import plotly.express as px
from datetime import datetime, timedelta
from pathlib import Path

# --- CẤU HÌNH TRANG ---
st.set_page_config(page_title="NYC Taxi Full Analytics", layout="wide")

ZONE_LOOKUP_PATH = Path(__file__).resolve().parents[1] / "scripts_pipeline" / "data" / "taxi_zone_lookup.csv"

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

@st.cache_data
def load_zone_lookup():
    try:
        df = pd.read_csv(ZONE_LOOKUP_PATH)
        return df.rename(columns={"LocationID": "pickup_location_id"})
    except Exception as e:
        st.warning(f"Không thể tải taxi zone lookup: {e}")
        return pd.DataFrame(columns=["pickup_location_id", "Borough", "Zone", "service_zone"])

# --- LẤY PHẠM VI NGÀY THỰC TẾ TỪ DATABASE ---
@st.cache_data
def get_db_date_range():
    query = "SELECT MIN(pickup_datetime) as min_d, MAX(pickup_datetime) as max_d FROM production.fact_trip"
    df = run_query(query)
    if not df.empty and df['max_d'].iloc[0] is not None:
        return pd.to_datetime(df['min_d'].iloc[0]), pd.to_datetime(df['max_d'].iloc[0])
    # Trả về giá trị mặc định nếu DB trống
    return datetime(2025, 1, 1), datetime.now()

@st.cache_data
def get_staging_date_range():
    query = "SELECT MIN(pickup_datetime) as min_d, MAX(pickup_datetime) as max_d FROM staging.nyc_taxi"
    df = run_query(query)
    if not df.empty and df['max_d'].iloc[0] is not None:
        return pd.to_datetime(df['min_d'].iloc[0]), pd.to_datetime(df['max_d'].iloc[0])
    return None, None

def add_simulated_zone_coordinates(df):
    if df.empty:
        return df

    borough_centers = {
        "Manhattan": (40.7831, -73.9712),
        "Brooklyn": (40.6600, -73.9500),
        "Queens": (40.7350, -73.8200),
        "Bronx": (40.8600, -73.8850),
        "Staten Island": (40.5795, -74.1502),
        "EWR": (40.6895, -74.1745),
        "Unknown": (40.7300, -73.9350),
    }

    df = df.copy()
    df["Borough"] = df["Borough"].fillna("Unknown")
    df = df.sort_values(["Borough", "pickup_location_id"]).reset_index(drop=True)
    df["borough_rank"] = df.groupby("Borough").cumcount()
    df["grid_col"] = df["borough_rank"] % 6
    df["grid_row"] = df["borough_rank"] // 6

    def compute_lat(row):
        base_lat = borough_centers.get(row["Borough"], borough_centers["Unknown"])[0]
        return base_lat + (row["grid_row"] * 0.015) + ((row["grid_col"] % 2) * 0.0045)

    def compute_lon(row):
        base_lon = borough_centers.get(row["Borough"], borough_centers["Unknown"])[1]
        return base_lon + ((row["grid_col"] - 2.5) * 0.018)

    df["sim_latitude"] = df.apply(compute_lat, axis=1)
    df["sim_longitude"] = df.apply(compute_lon, axis=1)
    return df.drop(columns=["borough_rank", "grid_col", "grid_row"])

# Lấy ngày biên từ DB
min_db_date, max_db_date = get_db_date_range()
min_staging_date, max_staging_date = get_staging_date_range()
zone_lookup_df = load_zone_lookup()

# --- SIDEBAR (BỘ LỌC) ---
st.sidebar.header("🕹️ Bộ lọc Dashboard")
service_mapping = {"Yellow": 1, "Green": 2}

default_start = min_db_date 
default_end = max_db_date

date_range = st.sidebar.date_input(
    "Khoảng thời gian",
    [default_start.date(), max_db_date.date()],
    min_value=min_db_date.date(),
    max_value=max_db_date.date()
)

service_type_names = st.sidebar.multiselect(
    "Loại hình dịch vụ",
    options=list(service_mapping.keys()),
    default=list(service_mapping.keys())
)

# Chặn lỗi khi chưa chọn đủ filter
if len(date_range) < 2 or not service_type_names:
    st.info("💡 Vui lòng chọn đầy đủ ngày (Bắt đầu - Kết thúc) và loại xe ở thanh bên.")
    st.stop()

# Chuẩn bị biến cho SQL
start_date = date_range[0].strftime('%Y-%m-%d')
end_date = date_range[1].strftime('%Y-%m-%d')
selected_ids = [service_mapping[name] for name in service_type_names]

# Xử lý SQL IN clause an toàn (tránh lỗi cú pháp khi chọn 1 hoặc nhiều xe)
if len(selected_ids) == 1:
    services_sql = f"({selected_ids[0]})"
else:
    services_sql = tuple(selected_ids)

# --- GIAO DIỆN CHÍNH ---
st.title("🚕 NYC Taxi Dashboard")
st.caption(f"Dữ liệu cập nhật từ **{min_db_date.strftime('%d/%m/%Y')}** đến **{max_db_date.strftime('%d/%m/%Y')}**")

# 1. TỔNG QUAN (METRICS)
# Sử dụng COALESCE để biến NULL thành 0, tránh lỗi TypeError khi format chuỗi
metric_query = f"""
    SELECT 
        COUNT(*) as total_trips, 
        COALESCE(SUM(total_amount), 0) as total_revenue, 
        COALESCE(AVG(tip_amount), 0) as avg_tip, 
        COALESCE(AVG(trip_distance), 0) as avg_dist
    FROM production.fact_trip
    WHERE pickup_datetime >= '{start_date} 00:00:00' 
      AND pickup_datetime <= '{end_date} 23:59:59'
      AND service_type_id IN {services_sql}
"""
df_m = run_query(metric_query)

if not df_m.empty:
    c1, c2, c3, c4 = st.columns(4)
    # Dùng .iloc[0] để truy cập giá trị an toàn
    c1.metric("Tổng chuyến đi", f"{df_m['total_trips'].iloc[0]:,.0f}")
    c2.metric("Doanh thu", f"${df_m['total_revenue'].iloc[0]:,.0f}")
    c3.metric("Tip TB", f"${df_m['avg_tip'].iloc[0]:.2f}")
    c4.metric("Quãng đường TB", f"{df_m['avg_dist'].iloc[0]:.2f} mi")

st.subheader("Chỉ số chuyên sâu")
special_metric_query = f"""
    SELECT
        COALESCE(SUM(total_amount) / NULLIF(COUNT(*), 0), 0) AS revenue_per_trip,
        COALESCE(100.0 * SUM(tip_amount) / NULLIF(SUM(fare_amount), 0), 0) AS tip_rate_pct,
        COALESCE(AVG(
            CASE
                WHEN dropoff_datetime > pickup_datetime
                THEN EXTRACT(EPOCH FROM (dropoff_datetime - pickup_datetime)) / 60.0
            END
        ), 0) AS avg_duration_min,
        COALESCE(100.0 * AVG(CASE WHEN total_amount >= 50 THEN 1 ELSE 0 END), 0) AS high_value_trip_pct,
        COALESCE(100.0 * AVG(CASE WHEN pickup_location_id IN (1, 132, 138) THEN 1 ELSE 0 END), 0) AS airport_pickup_pct
    FROM production.fact_trip
    WHERE pickup_datetime >= '{start_date} 00:00:00'
      AND pickup_datetime <= '{end_date} 23:59:59'
      AND service_type_id IN {services_sql}
"""
df_special = run_query(special_metric_query)

if not df_special.empty:
    s1, s2, s3, s4, s5 = st.columns(5)
    s1.metric("Doanh thu / chuyến", f"${df_special['revenue_per_trip'].iloc[0]:.2f}")
    s2.metric("Tip Rate", f"{df_special['tip_rate_pct'].iloc[0]:.1f}%")
    s3.metric("Thời lượng TB", f"{df_special['avg_duration_min'].iloc[0]:.1f} phút")
    s4.metric("Chuyến giá trị cao", f"{df_special['high_value_trip_pct'].iloc[0]:.1f}%")
    s5.metric("Pickup tại sân bay", f"{df_special['airport_pickup_pct'].iloc[0]:.1f}%")

st.divider()

# TẠO TABS
tab1, tab2, tab3, tab4 = st.tabs([
    "📈 Tổng quan xu hướng",
    "🔍 Phân tích chuyên sâu",
    "🗺️ Địa lý & Phân bổ",
    "🧭 Bản đồ mô phỏng"
])

with tab1:
    st.subheader("Xu hướng nhu cầu hàng ngày")
    df_time = run_query(f"""
        SELECT date_trunc('day', pickup_datetime) as date, count(*) as trips
        FROM production.fact_trip
        WHERE pickup_datetime >= '{start_date}' AND pickup_datetime <= '{end_date} 23:59:59'
        AND service_type_id IN {services_sql}
        GROUP BY 1 ORDER BY 1
    """)
    if not df_time.empty:
        st.plotly_chart(px.line(df_time, x='date', y='trips', title="Số lượng chuyến đi"), use_container_width=True)

    col_l, col_r = st.columns(2)
    with col_l:
        df_pay = run_query(f"""
            SELECT p.payment_description, count(*) as count 
            FROM production.fact_trip f
            JOIN production.dim_payment p ON f.payment_type_key = p.payment_type_key
            WHERE f.pickup_datetime >= '{start_date}' AND f.pickup_datetime <= '{end_date} 23:59:59'
            AND f.service_type_id IN {services_sql} 
            GROUP BY 1
        """)
        if not df_pay.empty:
            st.plotly_chart(px.pie(df_pay, values='count', names='payment_description', title="Hình thức thanh toán", hole=.4), use_container_width=True)
    
    with col_r:
        df_fare = run_query(f"""
            SELECT fare_amount FROM production.fact_trip 
            WHERE pickup_datetime >= '{start_date}' AND pickup_datetime <= '{end_date} 23:59:59'
            AND service_type_id IN {services_sql} LIMIT 5000
        """)
        if not df_fare.empty:
            st.plotly_chart(px.histogram(df_fare, x="fare_amount", nbins=50, title="Phân bổ giá vé"), use_container_width=True)

with tab2:
    st.subheader("Ma trận giờ cao điểm trong tuần")
    df_heat = run_query(f"""
        SELECT EXTRACT(DOW FROM pickup_datetime) as dow, EXTRACT(HOUR FROM pickup_datetime) as hour, COUNT(*) as trips
        FROM production.fact_trip 
        WHERE pickup_datetime >= '{start_date}' AND pickup_datetime <= '{end_date} 23:59:59'
        AND service_type_id IN {services_sql} 
        GROUP BY 1, 2
    """)
    if not df_heat.empty:
        df_heat['day'] = df_heat['dow'].map({0:'Sun', 1:'Mon', 2:'Tue', 3:'Wed', 4:'Thu', 5:'Fri', 6:'Sat'})
        st.plotly_chart(px.density_heatmap(df_heat, x='hour', y='day', z='trips', color_continuous_scale='Viridis',
                                           category_orders={"day": ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]}), use_container_width=True)

    st.subheader("Tương quan Quãng đường & Giá vé")
    df_scat = run_query(f"""
        SELECT trip_distance, total_amount FROM production.fact_trip 
        WHERE trip_distance > 0 
        AND pickup_datetime >= '{start_date}' AND pickup_datetime <= '{end_date} 23:59:59'
        AND service_type_id IN {services_sql} LIMIT 2000
    """)
    if not df_scat.empty:
        st.plotly_chart(px.scatter(df_scat, x="trip_distance", y="total_amount", trendline="ols", opacity=0.5), use_container_width=True)

with tab3:
    col_3a, col_3b = st.columns(2)
    with col_3a:
        st.subheader("💰 Hiệu suất doanh thu / dặm")
        rev_mile_query = f"""
            SELECT 
                EXTRACT(HOUR FROM pickup_datetime) as hour_of_day,
                SUM(total_amount) / NULLIF(SUM(trip_distance), 0) as rev_per_mile
            FROM production.fact_trip
            WHERE pickup_datetime >= '{start_date}' AND pickup_datetime <= '{end_date} 23:59:59'
            AND service_type_id IN {services_sql}
            AND trip_distance > 0
            GROUP BY 1 ORDER BY 1
        """
        df_rev_mile = run_query(rev_mile_query)
        if not df_rev_mile.empty:
            fig_rev = px.area(df_rev_mile, x='hour_of_day', y='rev_per_mile', title="USD/Dặm theo khung giờ", color_discrete_sequence=['#2E8B57'])
            fig_rev.update_layout(yaxis_tickprefix='$', hovermode="x unified")
            st.plotly_chart(fig_rev, use_container_width=True)
    
    with col_3b:
        st.subheader("Tỷ lệ Chuyến đi Sân bay")
        df_air = run_query(f"""
            SELECT CASE WHEN pickup_location_id IN (132, 138) THEN 'Airport' ELSE 'City' END as type, COUNT(*) as count
            FROM production.fact_trip 
            WHERE pickup_datetime >= '{start_date}' AND pickup_datetime <= '{end_date} 23:59:59'
            AND service_type_id IN {services_sql}
            GROUP BY 1
        """)
        if not df_air.empty:
            st.plotly_chart(px.pie(df_air, values='count', names='type', hole=.5), use_container_width=True)

with tab4:
    st.subheader("Bản đồ hotspot pickup")
    st.caption("Bản đồ mô phỏng theo zone")

    map_query = f"""
        SELECT
            pickup_location_id,
            COUNT(*) AS trips,
            AVG(total_amount) AS avg_total_amount
        FROM production.fact_trip
        WHERE pickup_datetime >= '{start_date} 00:00:00'
          AND pickup_datetime <= '{end_date} 23:59:59'
          AND service_type_id IN {services_sql}
        GROUP BY 1
        HAVING COUNT(*) >= 5
        ORDER BY trips DESC
        LIMIT 80
    """
    df_map = run_query(map_query)

    if not df_map.empty:
        if not zone_lookup_df.empty:
            df_map = df_map.merge(zone_lookup_df, on="pickup_location_id", how="left")
        else:
            df_map["Zone"] = "Unknown"
            df_map["Borough"] = "Unknown"

        df_map = add_simulated_zone_coordinates(df_map)
        df_map["zone_label"] = df_map["Zone"].fillna("Unknown Zone")
        df_map["borough_label"] = df_map["Borough"].fillna("Unknown Borough")
        df_map["avg_total_label"] = df_map["avg_total_amount"].map(lambda x: f"${x:,.2f}")

        borough_palette = {
            "Manhattan": "#1f77b4",
            "Brooklyn": "#ff7f0e",
            "Queens": "#2ca02c",
            "Bronx": "#d62728",
            "Staten Island": "#9467bd",
            "EWR": "#8c564b",
            "Unknown": "#7f7f7f",
        }

        borough_summary = (
            df_map.groupby("borough_label", dropna=False)
            .agg(
                zones=("pickup_location_id", "nunique"),
                trips=("trips", "sum"),
                avg_revenue=("avg_total_amount", "mean"),
            )
            .reset_index()
            .sort_values("trips", ascending=False)
        )

        b1, b2, b3 = st.columns(3)
        b1.metric("Zone hiển thị", f"{df_map['pickup_location_id'].nunique():,}")
        b2.metric("Borough active", f"{borough_summary['borough_label'].nunique():,}")
        b3.metric("Top zone doanh thu TB", df_map.sort_values("avg_total_amount", ascending=False).iloc[0]["zone_label"])

        fig_map = px.scatter_mapbox(
            df_map,
            lat="sim_latitude",
            lon="sim_longitude",
            size="trips",
            color="borough_label",
            hover_name="zone_label",
            hover_data={
                "borough_label": True,
                "pickup_location_id": True,
                "trips": ":,",
                "avg_total_label": True,
                "sim_latitude": False,
                "sim_longitude": False,
                "avg_total_amount": False,
            },
            color_discrete_map=borough_palette,
            size_max=25,
            zoom=9.5,
            center={"lat": 40.73, "lon": -73.94},
            title="Hotspot pickup theo zone "
        )
        fig_map.update_traces(
            marker=dict(opacity=0.85)
        )
        fig_map.update_layout(
            # "carto-darkmatter" giúp các nốt màu nổi bật hơn "carto-positron"
            mapbox_style="carto-darkmatter", 
            margin=dict(l=0, r=0, t=50, b=0),
            legend_title_text="Borough",
            hoverlabel=dict(bgcolor="white", font_size=13) # Làm tooltip dễ đọc hơn
        )

        top_zone_df = (
            df_map.sort_values(["trips", "avg_total_amount"], ascending=[False, False])
            .head(10)[["zone_label", "borough_label", "trips", "avg_total_amount"]]
            .rename(columns={
                "zone_label": "Zone",
                "borough_label": "Borough",
                "trips": "Trips",
                "avg_total_amount": "Avg total ($)"
            })
        )

        left_col, right_col = st.columns([1.8, 1])
        with left_col:
            st.plotly_chart(fig_map, use_container_width=True)
        with right_col:
            fig_borough = px.bar(
                borough_summary,
                x="trips",
                y="borough_label",
                orientation="h",
                color="borough_label",
                color_discrete_map=borough_palette,
                title="Tổng chuyến theo borough",
            )
            fig_borough.update_layout(
                showlegend=False,
                margin=dict(l=0, r=0, t=50, b=0),
                xaxis_title="Trips",
                yaxis_title="",
                height=400
            )
            fig_borough.update_yaxes(categoryorder="total ascending")
            st.plotly_chart(fig_borough, use_container_width=True)

        fig_top_zone = px.bar(
            top_zone_df.sort_values("Trips", ascending=True),
            x="Trips",
            y="Zone",
            orientation="h",
            color="Borough",
            color_discrete_map=borough_palette,
            title="Top 10 zone pickup nổi bật",
            hover_data={"Avg total ($)": ":.2f"}
        )
        fig_top_zone.update_layout(
            margin=dict(l=0, r=0, t=50, b=0),
            xaxis_title="Trips",
            yaxis_title=""
        )
        st.plotly_chart(fig_top_zone, use_container_width=True)
        st.dataframe(top_zone_df, use_container_width=True, hide_index=True)
    else:
        st.info("Không có đủ dữ liệu `pickup_location_id` để dựng bản đồ giả lập cho bộ lọc hiện tại.")
