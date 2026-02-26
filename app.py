import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import json
import time
import random

# ─── Page Config ───
st.set_page_config(
    page_title="Azure Databricks Pipeline Demo",
    page_icon="🔷",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ─── Custom CSS ───
st.markdown("""
<style>
    .main-header {
        font-size: 2.2rem; font-weight: 700; color: #0078D4;
        border-bottom: 3px solid #0078D4; padding-bottom: 10px; margin-bottom: 20px;
    }
    .metric-card {
        background: linear-gradient(135deg, #0078D4 0%, #106EBE 100%);
        padding: 20px; border-radius: 12px; color: white; text-align: center;
    }
    .metric-value { font-size: 2rem; font-weight: 700; }
    .metric-label { font-size: 0.85rem; opacity: 0.9; }
    .pipeline-stage {
        background: #E8F4FD; border-left: 4px solid #0078D4;
        padding: 12px 16px; border-radius: 0 8px 8px 0; margin: 8px 0;
    }
    .success-badge { background: #107C10; color: white; padding: 4px 12px; border-radius: 20px; font-size: 0.8rem; }
    .warning-badge { background: #FF8C00; color: white; padding: 4px 12px; border-radius: 20px; font-size: 0.8rem; }
    .error-badge { background: #D13438; color: white; padding: 4px 12px; border-radius: 20px; font-size: 0.8rem; }
    div[data-testid="stMetric"] {
        background-color: #F3F6FC; border: 1px solid #D2D2D7;
        padding: 15px; border-radius: 10px;
    }
</style>
""", unsafe_allow_html=True)


# ─── Data Generation ───
@st.cache_data
def generate_iot_data(n=5000):
    np.random.seed(42)
    dates = pd.date_range("2024-01-01", periods=n, freq="5min")
    devices = [f"sensor_{i:03d}" for i in range(1, 51)]
    data = {
        "timestamp": dates,
        "device_id": np.random.choice(devices, n),
        "temperature": np.round(np.random.normal(72, 8, n), 2),
        "humidity": np.round(np.clip(np.random.normal(45, 15, n), 0, 100), 2),
        "pressure": np.round(np.random.normal(1013, 5, n), 2),
        "voltage": np.round(np.clip(np.random.normal(3.7, 0.3, n), 2.8, 4.2), 3),
        "error_code": np.random.choice([None, None, None, None, "E001", "E002", "E003", "W001"], n),
        "location": np.random.choice(["Building_A", "Building_B", "Building_C", "Warehouse"], n),
    }
    df = pd.DataFrame(data)
    # inject some nulls and duplicates
    mask = np.random.random(n) < 0.03
    df.loc[mask, "temperature"] = None
    mask2 = np.random.random(n) < 0.02
    df.loc[mask2, "humidity"] = None
    dupes = df.sample(n=int(n * 0.02), random_state=42)
    df = pd.concat([df, dupes]).sort_values("timestamp").reset_index(drop=True)
    return df


@st.cache_data
def generate_pipeline_runs(n=50):
    runs = []
    base = datetime(2024, 1, 15, 6, 0, 0)
    for i in range(n):
        start = base + timedelta(hours=i * 6, minutes=random.randint(0, 15))
        duration = random.randint(120, 900)
        status = random.choices(["Success", "Success", "Success", "Success", "Failed", "Warning"], k=1)[0]
        records = random.randint(8000, 50000)
        runs.append({
            "run_id": f"run_{1000 + i}",
            "start_time": start,
            "end_time": start + timedelta(seconds=duration),
            "duration_sec": duration,
            "status": status,
            "records_processed": records if status != "Failed" else 0,
            "pipeline": random.choice(["ingest_iot", "transform_hourly", "aggregate_daily", "quality_check"]),
        })
    return pd.DataFrame(runs)


# ─── Sidebar ───
with st.sidebar:
    st.image("https://upload.wikimedia.org/wikipedia/commons/thumb/f/fa/Microsoft_Azure.svg/150px-Microsoft_Azure.svg.png", width=50)
    st.markdown("## Azure Databricks Pipeline")
    st.markdown("---")
    page = st.radio("Navigation", [
        "🏗️ Pipeline Architecture",
        "📥 Data Ingestion",
        "⚡ PySpark Transformations",
        "🗄️ Delta Lake Operations",
        "📊 Pipeline Monitoring",
        "✅ Data Quality Checks",
    ], label_visibility="collapsed")
    st.markdown("---")
    st.markdown("**Tech Stack**")
    st.markdown("• Azure Data Factory\n• Azure Databricks\n• Delta Lake\n• PySpark\n• Azure SQL Database")

raw_df = generate_iot_data()
pipeline_df = generate_pipeline_runs()


# ════════════════════════════════════════════════════════════════
# PAGE: Pipeline Architecture
# ════════════════════════════════════════════════════════════════
if page == "🏗️ Pipeline Architecture":
    st.markdown('<div class="main-header">Pipeline Architecture — Azure Data Factory + Databricks</div>', unsafe_allow_html=True)

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Data Sources", "4", help="IoT sensors, APIs, files, databases")
    with col2:
        st.metric("Pipelines Active", "6", delta="2 new this week")
    with col3:
        st.metric("Avg Latency", "3.2 min", delta="-0.8 min")
    with col4:
        st.metric("Daily Records", "1.2M", delta="120K")

    st.markdown("### End-to-End Pipeline Flow")

    # Architecture as a Sankey diagram
    labels = [
        "IoT Sensors", "REST APIs", "CSV Files", "Azure SQL",  # sources 0-3
        "Azure Data Factory", "Event Hub",  # orchestration 4-5
        "Databricks Bronze", "Databricks Silver", "Databricks Gold",  # layers 6-8
        "Delta Lake", "Power BI", "Azure Synapse",  # targets 9-11
    ]
    source = [0, 1, 2, 3, 0, 4, 4, 5, 6, 7, 8, 8, 8]
    target = [4, 4, 4, 4, 5, 6, 6, 6, 7, 8, 9, 10, 11]
    value = [30, 20, 15, 10, 25, 40, 35, 25, 95, 90, 50, 25, 20]
    colors = ["#0078D4"] * len(source)

    fig = go.Figure(go.Sankey(
        node=dict(
            pad=20, thickness=25,
            label=labels,
            color=["#50E6FF", "#50E6FF", "#50E6FF", "#50E6FF",
                   "#0078D4", "#50E6FF",
                   "#CD7F32", "#C0C0C0", "#FFD700",
                   "#107C10", "#FF8C00", "#5C2D91"],
        ),
        link=dict(source=source, target=target, value=value, color="rgba(0,120,212,0.2)"),
    ))
    fig.update_layout(title_text="Data Flow: Sources → ADF → Databricks → Targets", height=450, font_size=12)
    st.plotly_chart(fig, use_container_width=True)

    # Pipeline stages
    st.markdown("### Pipeline Stages Detail")
    stages = [
        ("1. Ingestion (Bronze)", "Raw data lands via ADF Copy Activity + Event Hub streaming. Stored as-is in Delta format.", "Azure Data Factory, Event Hubs"),
        ("2. Cleansing (Silver)", "Deduplication, null handling, type casting, schema enforcement. PySpark notebooks in Databricks.", "PySpark, Delta Lake, Databricks Notebooks"),
        ("3. Aggregation (Gold)", "Business-level aggregations, KPI calculations, join with dimension tables.", "PySpark SQL, Delta Lake MERGE"),
        ("4. Serving", "Optimized tables served to Power BI via Direct Query and Azure Synapse Analytics.", "Azure Synapse, Power BI, Delta Sharing"),
    ]
    for title, desc, tech in stages:
        with st.expander(title, expanded=False):
            st.write(desc)
            st.code(f"Technologies: {tech}", language="text")


# ════════════════════════════════════════════════════════════════
# PAGE: Data Ingestion
# ════════════════════════════════════════════════════════════════
elif page == "📥 Data Ingestion":
    st.markdown('<div class="main-header">Data Ingestion — Bronze Layer</div>', unsafe_allow_html=True)

    tab1, tab2 = st.tabs(["📡 Sample IoT Data", "📤 Upload Your Data"])

    with tab1:
        st.markdown("### IoT Sensor Data Preview")
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Total Records", f"{len(raw_df):,}")
        col2.metric("Devices", raw_df["device_id"].nunique())
        col3.metric("Null Values", f"{raw_df.isnull().sum().sum():,}")
        col4.metric("Duplicates", f"{raw_df.duplicated().sum():,}")

        st.dataframe(raw_df.head(100), use_container_width=True, height=300)

        st.markdown("### Data Profiling")
        col1, col2 = st.columns(2)
        with col1:
            fig = px.histogram(raw_df, x="temperature", nbins=50, title="Temperature Distribution",
                               color_discrete_sequence=["#0078D4"])
            fig.update_layout(height=350)
            st.plotly_chart(fig, use_container_width=True)
        with col2:
            fig = px.histogram(raw_df, x="humidity", nbins=50, title="Humidity Distribution",
                               color_discrete_sequence=["#50E6FF"])
            fig.update_layout(height=350)
            st.plotly_chart(fig, use_container_width=True)

        # Null analysis
        st.markdown("### Null Value Analysis")
        null_counts = raw_df.isnull().sum()
        null_pct = (null_counts / len(raw_df) * 100).round(2)
        null_df = pd.DataFrame({"Column": null_counts.index, "Null Count": null_counts.values, "Null %": null_pct.values})
        null_df = null_df[null_df["Null Count"] > 0]
        fig = px.bar(null_df, x="Column", y="Null %", title="Missing Data by Column",
                     color="Null %", color_continuous_scale="Blues", text="Null Count")
        fig.update_layout(height=350)
        st.plotly_chart(fig, use_container_width=True)

    with tab2:
        uploaded = st.file_uploader("Upload a CSV file to ingest", type=["csv"])
        if uploaded:
            user_df = pd.read_csv(uploaded)
            st.success(f"Loaded {len(user_df):,} rows x {len(user_df.columns)} columns")
            st.dataframe(user_df.head(50), use_container_width=True)
            st.markdown("#### Column Types")
            st.json({col: str(dtype) for col, dtype in user_df.dtypes.items()})


# ════════════════════════════════════════════════════════════════
# PAGE: PySpark Transformations
# ════════════════════════════════════════════════════════════════
elif page == "⚡ PySpark Transformations":
    st.markdown('<div class="main-header">PySpark Transformations — Silver Layer</div>', unsafe_allow_html=True)

    st.markdown("### Transformation Pipeline")
    steps = st.multiselect("Select transformations to apply:", [
        "Drop Duplicates", "Handle Nulls (median fill)", "Cast Types",
        "Remove Outliers (3σ)", "Standardize Device IDs", "Add Derived Columns"
    ], default=["Drop Duplicates", "Handle Nulls (median fill)", "Remove Outliers (3σ)", "Add Derived Columns"])

    transformed = raw_df.copy()
    log = []

    if "Drop Duplicates" in steps:
        before = len(transformed)
        transformed = transformed.drop_duplicates()
        after = len(transformed)
        log.append(f"✅ Dropped {before - after:,} duplicate rows")

    if "Handle Nulls (median fill)" in steps:
        for col in ["temperature", "humidity"]:
            nulls = transformed[col].isnull().sum()
            if nulls > 0:
                median_val = transformed[col].median()
                transformed[col] = transformed[col].fillna(median_val)
                log.append(f"✅ Filled {nulls} nulls in '{col}' with median ({median_val:.1f})")

    if "Cast Types" in steps:
        transformed["timestamp"] = pd.to_datetime(transformed["timestamp"])
        transformed["device_id"] = transformed["device_id"].astype(str)
        log.append("✅ Cast timestamp → datetime64, device_id → string")

    if "Remove Outliers (3σ)" in steps:
        before = len(transformed)
        for col in ["temperature", "humidity", "pressure"]:
            mean, std = transformed[col].mean(), transformed[col].std()
            transformed = transformed[(transformed[col] >= mean - 3 * std) & (transformed[col] <= mean + 3 * std)]
        log.append(f"✅ Removed {before - len(transformed):,} outlier rows (3σ rule)")

    if "Standardize Device IDs" in steps:
        transformed["device_id"] = transformed["device_id"].str.upper().str.strip()
        log.append("✅ Standardized device IDs to uppercase")

    if "Add Derived Columns" in steps:
        transformed["hour"] = pd.to_datetime(transformed["timestamp"]).dt.hour
        transformed["date"] = pd.to_datetime(transformed["timestamp"]).dt.date
        transformed["temp_status"] = pd.cut(transformed["temperature"], bins=[-999, 60, 80, 999], labels=["Cold", "Normal", "Hot"])
        log.append("✅ Added: hour, date, temp_status columns")

    # Show transformation log
    col1, col2 = st.columns([1, 1])
    with col1:
        st.markdown("### Transformation Log")
        for entry in log:
            st.markdown(f"<div class='pipeline-stage'>{entry}</div>", unsafe_allow_html=True)
    with col2:
        st.markdown("### Result Summary")
        st.metric("Output Records", f"{len(transformed):,}")
        st.metric("Output Columns", len(transformed.columns))
        st.metric("Transformations Applied", len(steps))

    # Before/After
    st.markdown("### Before / After Comparison")
    col1, col2 = st.columns(2)
    with col1:
        st.markdown("**Before (Bronze)**")
        st.dataframe(raw_df.head(10), use_container_width=True)
    with col2:
        st.markdown("**After (Silver)**")
        st.dataframe(transformed.head(10), use_container_width=True)

    # PySpark Code
    st.markdown("### Equivalent PySpark Code")
    st.code('''
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, mean, stddev, upper, trim, hour, to_date

spark = SparkSession.builder.appName("IoT_Silver_Transform").getOrCreate()

# Read Bronze Delta table
bronze_df = spark.read.format("delta").load("/mnt/delta/iot_bronze")

# Drop duplicates
silver_df = bronze_df.dropDuplicates()

# Handle nulls with median fill
for c in ["temperature", "humidity"]:
    median_val = silver_df.approxQuantile(c, [0.5], 0.01)[0]
    silver_df = silver_df.fillna({c: median_val})

# Remove outliers (3σ)
for c in ["temperature", "humidity", "pressure"]:
    stats = silver_df.select(mean(col(c)), stddev(col(c))).first()
    mu, sigma = stats[0], stats[1]
    silver_df = silver_df.filter(
        (col(c) >= mu - 3 * sigma) & (col(c) <= mu + 3 * sigma)
    )

# Add derived columns
silver_df = silver_df.withColumn("hour", hour("timestamp")) \\
    .withColumn("date", to_date("timestamp")) \\
    .withColumn("temp_status",
        when(col("temperature") < 60, "Cold")
        .when(col("temperature") < 80, "Normal")
        .otherwise("Hot"))

# Write to Silver Delta table with MERGE
silver_df.write.format("delta").mode("overwrite") \\
    .option("overwriteSchema", "true") \\
    .save("/mnt/delta/iot_silver")
    ''', language="python")


# ════════════════════════════════════════════════════════════════
# PAGE: Delta Lake Operations
# ════════════════════════════════════════════════════════════════
elif page == "🗄️ Delta Lake Operations":
    st.markdown('<div class="main-header">Delta Lake Operations — Gold Layer</div>', unsafe_allow_html=True)

    tab1, tab2, tab3 = st.tabs(["MERGE/UPSERT", "Time Travel", "Partitioning"])

    with tab1:
        st.markdown("### Delta Lake MERGE Operation")
        st.markdown("Simulating an upsert: new records are inserted, existing records are updated.")

        col1, col2 = st.columns(2)
        with col1:
            st.markdown("**Existing Gold Table (target)**")
            existing = pd.DataFrame({
                "device_id": ["SENSOR_001", "SENSOR_002", "SENSOR_003", "SENSOR_004"],
                "avg_temp": [71.5, 73.2, 69.8, 75.1],
                "avg_humidity": [42.3, 48.1, 39.7, 51.2],
                "record_count": [1200, 980, 1150, 870],
                "last_updated": ["2024-01-14", "2024-01-14", "2024-01-14", "2024-01-14"],
            })
            st.dataframe(existing, use_container_width=True)

        with col2:
            st.markdown("**Incoming Batch (source)**")
            incoming = pd.DataFrame({
                "device_id": ["SENSOR_002", "SENSOR_004", "SENSOR_050"],
                "avg_temp": [74.0, 76.3, 68.5],
                "avg_humidity": [47.5, 52.0, 44.1],
                "record_count": [150, 200, 300],
                "last_updated": ["2024-01-15", "2024-01-15", "2024-01-15"],
            })
            st.dataframe(incoming, use_container_width=True)

        if st.button("🔄 Execute MERGE", type="primary"):
            with st.spinner("Running Delta MERGE..."):
                time.sleep(1.5)
            merged = existing.copy()
            for _, row in incoming.iterrows():
                mask = merged["device_id"] == row["device_id"]
                if mask.any():
                    merged.loc[mask, "avg_temp"] = row["avg_temp"]
                    merged.loc[mask, "avg_humidity"] = row["avg_humidity"]
                    merged.loc[mask, "record_count"] = merged.loc[mask, "record_count"].values[0] + row["record_count"]
                    merged.loc[mask, "last_updated"] = row["last_updated"]
                else:
                    merged = pd.concat([merged, pd.DataFrame([row])], ignore_index=True)

            st.success("MERGE complete: 2 updated, 1 inserted, 2 unchanged")
            st.markdown("**Result (Gold Table after MERGE)**")
            st.dataframe(merged, use_container_width=True)

            st.code('''
MERGE INTO gold.device_aggregates AS target
USING staging.daily_batch AS source
ON target.device_id = source.device_id
WHEN MATCHED THEN UPDATE SET
    target.avg_temp = source.avg_temp,
    target.avg_humidity = source.avg_humidity,
    target.record_count = target.record_count + source.record_count,
    target.last_updated = source.last_updated
WHEN NOT MATCHED THEN INSERT *
            ''', language="sql")

    with tab2:
        st.markdown("### Delta Lake Time Travel")
        versions = pd.DataFrame({
            "Version": [0, 1, 2, 3, 4],
            "Timestamp": pd.date_range("2024-01-10", periods=5, freq="D"),
            "Operation": ["CREATE TABLE", "INSERT (initial load)", "MERGE (daily batch)", "UPDATE (corrections)", "MERGE (daily batch)"],
            "Records Changed": [0, 5000, 320, 45, 285],
            "Files Added": [0, 12, 4, 2, 3],
            "Files Removed": [0, 0, 2, 1, 2],
        })
        st.dataframe(versions, use_container_width=True)

        selected_version = st.slider("Select version to query:", 0, 4, 4)
        st.code(f"""
-- Query specific version
SELECT * FROM gold.device_aggregates VERSION AS OF {selected_version};

-- Or by timestamp
SELECT * FROM gold.device_aggregates TIMESTAMP AS OF '2024-01-1{selected_version}';
        """, language="sql")
        st.info(f"📌 Querying Version {selected_version} — {versions.iloc[selected_version]['Operation']} ({versions.iloc[selected_version]['Records Changed']} records changed)")

    with tab3:
        st.markdown("### Partition Strategy")
        st.markdown("Data partitioned by `date` and `location` for optimal query performance.")
        partition_stats = pd.DataFrame({
            "Partition": [f"date=2024-01-{d:02d}/location={loc}" for d in range(10, 16) for loc in ["Building_A", "Building_B", "Building_C", "Warehouse"]],
            "Files": np.random.randint(2, 8, 24),
            "Size (MB)": np.round(np.random.uniform(50, 300, 24), 1),
            "Records": np.random.randint(5000, 25000, 24),
        })
        fig = px.treemap(partition_stats, path=["Partition"], values="Records",
                         color="Size (MB)", color_continuous_scale="Blues",
                         title="Partition Layout (records per partition)")
        fig.update_layout(height=500)
        st.plotly_chart(fig, use_container_width=True)


# ════════════════════════════════════════════════════════════════
# PAGE: Pipeline Monitoring
# ════════════════════════════════════════════════════════════════
elif page == "📊 Pipeline Monitoring":
    st.markdown('<div class="main-header">Pipeline Monitoring Dashboard</div>', unsafe_allow_html=True)

    # KPIs
    col1, col2, col3, col4, col5 = st.columns(5)
    success = len(pipeline_df[pipeline_df["status"] == "Success"])
    failed = len(pipeline_df[pipeline_df["status"] == "Failed"])
    col1.metric("Total Runs", len(pipeline_df))
    col2.metric("Success Rate", f"{success / len(pipeline_df) * 100:.1f}%", delta="2.1%")
    col3.metric("Avg Duration", f"{pipeline_df['duration_sec'].mean():.0f}s")
    col4.metric("Records/Day", f"{pipeline_df['records_processed'].sum() / 12:.0f}")
    col5.metric("Failed Runs", failed, delta=f"-{failed}" if failed > 0 else "0")

    # Timeline chart
    st.markdown("### Pipeline Execution Timeline")
    color_map = {"Success": "#107C10", "Failed": "#D13438", "Warning": "#FF8C00"}
    fig = px.scatter(pipeline_df, x="start_time", y="pipeline", color="status",
                     size="records_processed", hover_data=["run_id", "duration_sec"],
                     color_discrete_map=color_map, title="Pipeline Runs Over Time")
    fig.update_layout(height=350)
    st.plotly_chart(fig, use_container_width=True)

    col1, col2 = st.columns(2)
    with col1:
        st.markdown("### Throughput Over Time")
        throughput = pipeline_df.copy()
        throughput["date"] = throughput["start_time"].dt.date
        daily = throughput.groupby("date")["records_processed"].sum().reset_index()
        fig = px.area(daily, x="date", y="records_processed", title="Records Processed per Day",
                      color_discrete_sequence=["#0078D4"])
        fig.update_layout(height=350)
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.markdown("### Duration by Pipeline")
        fig = px.box(pipeline_df, x="pipeline", y="duration_sec", color="pipeline",
                     title="Execution Duration Distribution",
                     color_discrete_sequence=px.colors.qualitative.Set2)
        fig.update_layout(height=350, showlegend=False)
        st.plotly_chart(fig, use_container_width=True)

    # Activity Log
    st.markdown("### Recent Activity Log")
    recent = pipeline_df.sort_values("start_time", ascending=False).head(15)
    for _, row in recent.iterrows():
        badge = "success-badge" if row["status"] == "Success" else ("error-badge" if row["status"] == "Failed" else "warning-badge")
        st.markdown(
            f'<div class="pipeline-stage">'
            f'<span class="{badge}">{row["status"]}</span> '
            f'<strong>{row["run_id"]}</strong> — {row["pipeline"]} — '
            f'{row["start_time"].strftime("%Y-%m-%d %H:%M")} — '
            f'{row["duration_sec"]}s — {row["records_processed"]:,} records'
            f'</div>',
            unsafe_allow_html=True,
        )


# ════════════════════════════════════════════════════════════════
# PAGE: Data Quality
# ════════════════════════════════════════════════════════════════
elif page == "✅ Data Quality Checks":
    st.markdown('<div class="main-header">Data Quality Checks</div>', unsafe_allow_html=True)

    checks = [
        {"Rule": "Null check — temperature", "Column": "temperature", "Type": "Completeness", "Threshold": "< 5%", "Actual": f"{raw_df['temperature'].isnull().mean()*100:.2f}%", "Status": "PASS" if raw_df["temperature"].isnull().mean() < 0.05 else "FAIL"},
        {"Rule": "Null check — humidity", "Column": "humidity", "Type": "Completeness", "Threshold": "< 5%", "Actual": f"{raw_df['humidity'].isnull().mean()*100:.2f}%", "Status": "PASS" if raw_df["humidity"].isnull().mean() < 0.05 else "FAIL"},
        {"Rule": "Temperature range", "Column": "temperature", "Type": "Validity", "Threshold": "[30, 120]°F", "Actual": f"[{raw_df['temperature'].min():.1f}, {raw_df['temperature'].max():.1f}]", "Status": "PASS"},
        {"Rule": "Humidity range", "Column": "humidity", "Type": "Validity", "Threshold": "[0, 100]%", "Actual": f"[{raw_df['humidity'].min():.1f}, {raw_df['humidity'].max():.1f}]", "Status": "PASS"},
        {"Rule": "Duplicate check", "Column": "all", "Type": "Uniqueness", "Threshold": "< 3%", "Actual": f"{raw_df.duplicated().mean()*100:.2f}%", "Status": "PASS" if raw_df.duplicated().mean() < 0.03 else "FAIL"},
        {"Rule": "Device ID format", "Column": "device_id", "Type": "Consistency", "Threshold": "sensor_XXX", "Actual": "All match pattern", "Status": "PASS"},
        {"Rule": "Timestamp freshness", "Column": "timestamp", "Type": "Timeliness", "Threshold": "< 24h old", "Actual": "17.4 days old", "Status": "FAIL"},
        {"Rule": "Voltage range", "Column": "voltage", "Type": "Validity", "Threshold": "[2.5, 4.5]V", "Actual": f"[{raw_df['voltage'].min():.2f}, {raw_df['voltage'].max():.2f}]", "Status": "PASS"},
    ]
    checks_df = pd.DataFrame(checks)

    # Quality Score
    passed = len(checks_df[checks_df["Status"] == "PASS"])
    total = len(checks_df)
    score = passed / total * 100

    col1, col2, col3 = st.columns(3)
    col1.metric("Quality Score", f"{score:.0f}%")
    col2.metric("Checks Passed", f"{passed}/{total}")
    col3.metric("Checks Failed", f"{total - passed}", delta=f"-{total - passed}")

    # Gauge chart
    fig = go.Figure(go.Indicator(
        mode="gauge+number+delta",
        value=score,
        title={"text": "Overall Data Quality Score"},
        delta={"reference": 95},
        gauge={
            "axis": {"range": [0, 100]},
            "bar": {"color": "#0078D4"},
            "steps": [
                {"range": [0, 60], "color": "#FDE7E7"},
                {"range": [60, 80], "color": "#FFF4CE"},
                {"range": [80, 100], "color": "#DFF6DD"},
            ],
            "threshold": {"line": {"color": "#D13438", "width": 3}, "thickness": 0.8, "value": 95},
        },
    ))
    fig.update_layout(height=300)
    st.plotly_chart(fig, use_container_width=True)

    # Detailed results table
    st.markdown("### Validation Results")

    def color_status(val):
        if val == "PASS":
            return "background-color: #DFF6DD; color: #107C10; font-weight: bold"
        return "background-color: #FDE7E7; color: #D13438; font-weight: bold"

    styled = checks_df.style.applymap(color_status, subset=["Status"])
    st.dataframe(styled, use_container_width=True, height=350)

    # Quality by column
    st.markdown("### Quality Score by Column")
    col_scores = []
    for col_name in ["temperature", "humidity", "pressure", "voltage", "device_id", "timestamp"]:
        if col_name in raw_df.columns:
            completeness = (1 - raw_df[col_name].isnull().mean()) * 100
            col_scores.append({"Column": col_name, "Completeness %": round(completeness, 2)})
    col_scores_df = pd.DataFrame(col_scores)
    fig = px.bar(col_scores_df, x="Column", y="Completeness %", title="Column Completeness",
                 color="Completeness %", color_continuous_scale="RdYlGn", range_y=[90, 100],
                 text="Completeness %")
    fig.update_layout(height=350)
    st.plotly_chart(fig, use_container_width=True)

    # Delta Live Tables expectations code
    st.markdown("### Delta Live Tables — Quality Expectations Code")
    st.code('''
import dlt
from pyspark.sql.functions import col

@dlt.table(comment="Silver IoT data with quality checks")
@dlt.expect("valid_temperature", "temperature BETWEEN 30 AND 120")
@dlt.expect("valid_humidity", "humidity BETWEEN 0 AND 100")
@dlt.expect_or_drop("not_null_temp", "temperature IS NOT NULL")
@dlt.expect_or_fail("valid_device", "device_id RLIKE '^sensor_[0-9]{3}$'")
def iot_silver():
    return (
        dlt.read("iot_bronze")
        .dropDuplicates(["timestamp", "device_id"])
        .withColumn("temp_status",
            when(col("temperature") < 60, "Cold")
            .when(col("temperature") < 80, "Normal")
            .otherwise("Hot"))
    )
    ''', language="python")
