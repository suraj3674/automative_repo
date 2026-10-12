import streamlit as st
import pandas as pd
import snowflake.connector


# ============================================================
# PAGE CONFIGURATION
# ============================================================

st.set_page_config(
    page_title="Volkswagen Automotive Analytics",
    page_icon="🚗",
    layout="wide"
)


# ============================================================
# TITLE
# ============================================================

st.title("🚗 Volkswagen Automotive Analytics")

st.caption(
    "AWS Automotive Data Platform | "
    "Snowflake MART | dbt KPI Reporting"
)


# ============================================================
# SNOWFLAKE CONNECTION
# ============================================================

@st.cache_resource
def create_connection(password):

    return snowflake.connector.connect(
        account="QDWTYKX-BU40830",
        user="SURAJ6219",
        password=password,
        warehouse="AUTOMOTIVE_WH",
        database="AUTOMOTIVE_DBT_DB",
        schema="MART",
        role="ACCOUNTADMIN"
    )


# ============================================================
# SIDEBAR
# ============================================================

st.sidebar.header("Snowflake Connection")

password = st.sidebar.text_input(
    "Snowflake Password",
    type="password"
)

if not password:

    st.info(
        "Enter your Snowflake password in the sidebar."
    )

    st.stop()


# ============================================================
# CONNECT TO SNOWFLAKE
# ============================================================

try:

    conn = create_connection(password)

    st.sidebar.success(
        "✅ Snowflake Connected"
    )

except Exception as e:

    st.sidebar.error(
        "❌ Snowflake Connection Failed"
    )

    st.error(
        f"Connection error: {e}"
    )

    st.stop()


# ============================================================
# LOAD KPI DATA
# ============================================================

@st.cache_data(ttl=300)
def load_data(_conn):

    sales_query = """
        SELECT *
        FROM AUTOMOTIVE_DBT_DB.MART.KPI_SALES
    """

    dealer_query = """
        SELECT *
        FROM AUTOMOTIVE_DBT_DB.MART.KPI_DEALER
    """

    vehicle_query = """
        SELECT *
        FROM AUTOMOTIVE_DBT_DB.MART.KPI_VEHICLE
    """

    sales_df = pd.read_sql(
        sales_query,
        _conn
    )

    dealer_df = pd.read_sql(
        dealer_query,
        _conn
    )

    vehicle_df = pd.read_sql(
        vehicle_query,
        _conn
    )

    return (
        sales_df,
        dealer_df,
        vehicle_df
    )


# ============================================================
# READ DATA
# ============================================================

try:

    sales_df, dealer_df, vehicle_df = load_data(conn)

except Exception as e:

    st.error(
        f"Error loading KPI tables: {e}"
    )

    st.stop()


# ============================================================
# DATE CONVERSION
# ============================================================

for df in [
    sales_df,
    dealer_df,
    vehicle_df
]:

    if "SALE_DATE" in df.columns:

        df["SALE_DATE"] = pd.to_datetime(
            df["SALE_DATE"]
        )


# ============================================================
# FILTERS
# ============================================================

st.sidebar.divider()

st.sidebar.header(
    "Dashboard Filters"
)


# ============================================================
# REGION FILTER
# ============================================================

if "REGION_NAME" in sales_df.columns:

    regions = sorted(
        sales_df["REGION_NAME"]
        .dropna()
        .unique()
        .tolist()
    )

    selected_regions = st.sidebar.multiselect(
        "Region",
        regions,
        default=regions
    )

else:

    selected_regions = []


# ============================================================
# CURRENCY FILTER
# ============================================================

if "CURRENCY" in sales_df.columns:

    currencies = sorted(
        sales_df["CURRENCY"]
        .dropna()
        .unique()
        .tolist()
    )

    selected_currencies = st.sidebar.multiselect(
        "Currency",
        currencies,
        default=currencies
    )

else:

    selected_currencies = []


# ============================================================
# APPLY FILTERS
# ============================================================

filtered_sales = sales_df.copy()


if (
    selected_regions
    and "REGION_NAME" in filtered_sales.columns
):

    filtered_sales = filtered_sales[
        filtered_sales["REGION_NAME"].isin(
            selected_regions
        )
    ]


if (
    selected_currencies
    and "CURRENCY" in filtered_sales.columns
):

    filtered_sales = filtered_sales[
        filtered_sales["CURRENCY"].isin(
            selected_currencies
        )
    ]


# ============================================================
# KPI CALCULATIONS
# ============================================================

total_vehicles = filtered_sales[
    "TOTAL_VEHICLES_SOLD"
].sum()


total_sales = filtered_sales[
    "TOTAL_SALES_AMOUNT"
].sum()


average_price = filtered_sales[
    "AVERAGE_SELLING_PRICE"
].mean()


high_value_sales = filtered_sales[
    "HIGH_VALUE_SALES"
].sum()


# ============================================================
# KPI CARDS
# ============================================================

st.subheader(
    "📊 Executive KPIs"
)

col1, col2, col3, col4 = st.columns(4)


with col1:

    st.metric(
        "🚗 Vehicles Sold",
        f"{int(total_vehicles):,}"
    )


with col2:

    st.metric(
        "💰 Total Sales",
        f"{total_sales:,.2f}"
    )


with col3:

    st.metric(
        "💵 Average Selling Price",
        f"{average_price:,.2f}"
    )


with col4:

    st.metric(
        "⭐ High Value Sales",
        f"{int(high_value_sales):,}"
    )


st.divider()


# ============================================================
# MONTHLY SALES
# ============================================================

st.subheader(
    "📈 Monthly Sales Trend"
)


if not filtered_sales.empty:

    monthly_sales = (
        filtered_sales
        .groupby(
            "SALES_MONTH",
            as_index=False
        )["TOTAL_SALES_AMOUNT"]
        .sum()
    )

    monthly_sales["SALES_MONTH"] = pd.to_datetime(
        monthly_sales["SALES_MONTH"]
    )

    monthly_sales = monthly_sales.sort_values(
        "SALES_MONTH"
    )

    st.line_chart(
        monthly_sales.set_index(
            "SALES_MONTH"
        )
    )

else:

    st.warning(
        "No sales data available."
    )


# ============================================================
# REGION SALES
# ============================================================

st.subheader(
    "🌎 Sales by Region"
)


if not filtered_sales.empty:

    region_sales = (
        filtered_sales
        .groupby(
            "REGION_NAME",
            as_index=False
        )["TOTAL_SALES_AMOUNT"]
        .sum()
        .sort_values(
            "TOTAL_SALES_AMOUNT",
            ascending=False
        )
    )

    st.bar_chart(
        region_sales.set_index(
            "REGION_NAME"
        )
    )

else:

    st.warning(
        "No regional data available."
    )


# ============================================================
# DEALER PERFORMANCE
# ============================================================

st.subheader(
    "🏢 Dealer Performance"
)


dealer_display = dealer_df.copy()


if (
    selected_regions
    and "REGION_NAME" in dealer_display.columns
):

    dealer_display = dealer_display[
        dealer_display["REGION_NAME"].isin(
            selected_regions
        )
    ]


if (
    selected_currencies
    and "CURRENCY" in dealer_display.columns
):

    dealer_display = dealer_display[
        dealer_display["CURRENCY"].isin(
            selected_currencies
        )
    ]


if not dealer_display.empty:

    dealer_summary = (
        dealer_display
        .groupby(
            [
                "DEALER_ID",
                "DEALER_NAME"
            ],
            as_index=False
        )
        .agg(
            VEHICLES_SOLD=(
                "VEHICLES_SOLD",
                "sum"
            ),
            TOTAL_SALES_AMOUNT=(
                "TOTAL_SALES_AMOUNT",
                "sum"
            )
        )
        .sort_values(
            "TOTAL_SALES_AMOUNT",
            ascending=False
        )
    )

    st.dataframe(
        dealer_summary,
        use_container_width=True
    )

else:

    st.warning(
        "No dealer data available."
    )


# ============================================================
# VEHICLE / MODEL PERFORMANCE
# ============================================================

st.subheader(
    "🚘 Vehicle / Model Performance"
)


vehicle_display = vehicle_df.copy()


if (
    selected_currencies
    and "CURRENCY" in vehicle_display.columns
):

    vehicle_display = vehicle_display[
        vehicle_display["CURRENCY"].isin(
            selected_currencies
        )
    ]


if not vehicle_display.empty:

    model_summary = (
        vehicle_display
        .groupby(
            "MODEL",
            as_index=False
        )
        .agg(
            VEHICLES_SOLD=(
                "VEHICLES_SOLD",
                "sum"
            ),
            TOTAL_SALES_AMOUNT=(
                "TOTAL_SALES_AMOUNT",
                "sum"
            ),
            AVERAGE_SELLING_PRICE=(
                "AVERAGE_SELLING_PRICE",
                "mean"
            )
        )
        .sort_values(
            "TOTAL_SALES_AMOUNT",
            ascending=False
        )
    )

    st.dataframe(
        model_summary,
        use_container_width=True
    )

else:

    st.warning(
        "No vehicle data available."
    )


# ============================================================
# RAW KPI DATA
# ============================================================

with st.expander(
    "🔎 View KPI Sales Data"
):

    st.dataframe(
        filtered_sales,
        use_container_width=True
    )


with st.expander(
    "🔎 View KPI Dealer Data"
):

    st.dataframe(
        dealer_df,
        use_container_width=True
    )


with st.expander(
    "🔎 View KPI Vehicle Data"
):

    st.dataframe(
        vehicle_df,
        use_container_width=True
    )


# ============================================================
# FOOTER
# ============================================================

st.divider()

st.caption(
    "Volkswagen Automotive Analytics | "
    "Snowflake + dbt + Streamlit"
)