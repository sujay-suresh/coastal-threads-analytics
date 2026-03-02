"""
Coastal Threads Analytics Dashboard
Three tabs: Executive KPIs, RFM Segments, Channel Attribution
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from sqlalchemy import create_engine

# --- Config ---
DB_URL = "postgresql://portfolio:portfolio_dev@localhost:5432/portfolio"
SCHEMA = "coastal_threads"

st.set_page_config(
    page_title="Coastal Threads Analytics",
    page_icon="🧵",
    layout="wide",
)


@st.cache_resource
def get_engine():
    return create_engine(DB_URL)


@st.cache_data(ttl=300)
def run_query(query):
    engine = get_engine()
    return pd.read_sql(query, engine)


# --- Header ---
st.title("Coastal Threads Analytics")
st.caption("E-commerce retention analytics for a DTC fashion retailer")

tab1, tab2, tab3 = st.tabs(["Executive KPIs", "RFM Segments", "Channel Attribution"])


# ============================================================
# TAB 1: Executive KPIs
# ============================================================
with tab1:
    # KPI cards
    kpi_data = run_query(f"""
        select
            count(distinct order_id) as total_orders,
            count(distinct customer_key) as total_customers,
            round(sum(revenue)::numeric, 2) as total_revenue,
            round(avg(revenue)::numeric, 2) as avg_item_revenue
        from {SCHEMA}.fct_orders
    """)

    repeat_data = run_query(f"""
        select
            count(*) as total_customers,
            sum(case when is_repeat_customer then 1 else 0 end) as repeat_customers
        from {SCHEMA}.dim_customers
    """)

    clv_data = run_query(f"""
        select
            round(avg(lifetime_revenue)::numeric, 2) as avg_clv
        from {SCHEMA}.dim_customers
        where lifetime_order_count > 0
    """)

    col1, col2, col3, col4, col5 = st.columns(5)
    with col1:
        st.metric("Total Revenue", f"${kpi_data['total_revenue'].iloc[0]:,.0f}")
    with col2:
        st.metric("Total Orders", f"{kpi_data['total_orders'].iloc[0]:,}")
    with col3:
        st.metric("Active Customers", f"{kpi_data['total_customers'].iloc[0]:,}")
    with col4:
        repeat_rate = repeat_data['repeat_customers'].iloc[0] / repeat_data['total_customers'].iloc[0] * 100
        st.metric("Repeat Purchase Rate", f"{repeat_rate:.1f}%")
    with col5:
        st.metric("Avg CLV", f"${clv_data['avg_clv'].iloc[0]:,.0f}")

    st.divider()

    # Revenue trend
    revenue_trend = run_query(f"""
        select
            d.date_day::date as order_date,
            sum(f.revenue) as daily_revenue,
            count(distinct f.order_id) as daily_orders
        from {SCHEMA}.fct_orders f
        join {SCHEMA}.dim_date d on f.date_key = d.date_key
        group by d.date_day
        order by d.date_day
    """)

    # Weekly rolling average
    revenue_trend['weekly_avg'] = revenue_trend['daily_revenue'].rolling(7).mean()

    col_left, col_right = st.columns(2)

    with col_left:
        st.subheader("Revenue Trend")
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=revenue_trend['order_date'],
            y=revenue_trend['daily_revenue'],
            mode='lines',
            name='Daily Revenue',
            line=dict(color='#c4d4e0', width=1),
            opacity=0.5,
        ))
        fig.add_trace(go.Scatter(
            x=revenue_trend['order_date'],
            y=revenue_trend['weekly_avg'],
            mode='lines',
            name='7-Day Avg',
            line=dict(color='#1f77b4', width=2),
        ))
        fig.update_layout(
            height=350,
            margin=dict(l=20, r=20, t=20, b=20),
            legend=dict(orientation='h', y=1.1),
            yaxis_title='Revenue ($)',
        )
        st.plotly_chart(fig, use_container_width=True)

    with col_right:
        st.subheader("AOV by Month")
        aov_monthly = run_query(f"""
            select
                date_trunc('month', d.date_day)::date as month,
                round((sum(f.revenue) / count(distinct f.order_id))::numeric, 2) as aov
            from {SCHEMA}.fct_orders f
            join {SCHEMA}.dim_date d on f.date_key = d.date_key
            group by date_trunc('month', d.date_day)
            order by month
        """)
        fig = px.bar(
            aov_monthly,
            x='month',
            y='aov',
            color_discrete_sequence=['#2ca02c'],
        )
        fig.update_layout(
            height=350,
            margin=dict(l=20, r=20, t=20, b=20),
            xaxis_title='',
            yaxis_title='Average Order Value ($)',
        )
        st.plotly_chart(fig, use_container_width=True)

    # Customer count trend
    st.subheader("Cumulative Customer Growth")
    customer_growth = run_query(f"""
        select
            date_trunc('month', signup_at)::date as month,
            count(*) as new_customers,
            sum(count(*)) over (order by date_trunc('month', signup_at)) as cumulative_customers
        from {SCHEMA}.dim_customers
        group by date_trunc('month', signup_at)
        order by month
    """)
    fig = px.area(
        customer_growth,
        x='month',
        y='cumulative_customers',
        color_discrete_sequence=['#ff7f0e'],
    )
    fig.update_layout(
        height=300,
        margin=dict(l=20, r=20, t=20, b=20),
        xaxis_title='',
        yaxis_title='Customers',
    )
    st.plotly_chart(fig, use_container_width=True)


# ============================================================
# TAB 2: RFM Segments
# ============================================================
with tab2:
    # Segment distribution
    segment_dist = run_query(f"""
        select
            rfm_segment,
            count(*) as customer_count,
            round(avg(lifetime_revenue)::numeric, 2) as avg_revenue,
            round(avg(lifetime_order_count)::numeric, 1) as avg_orders,
            round(avg(avg_order_value)::numeric, 2) as avg_aov
        from {SCHEMA}.dim_customers
        group by rfm_segment
        order by customer_count desc
    """)

    col_left, col_right = st.columns(2)

    with col_left:
        st.subheader("Segment Distribution")
        segment_order = ['Champions', 'Loyal', 'New', 'Promising', 'Needs Attention', 'At Risk', 'Hibernating']
        segment_colors = {
            'Champions': '#2ca02c',
            'Loyal': '#1f77b4',
            'New': '#ff7f0e',
            'Promising': '#9467bd',
            'Needs Attention': '#8c564b',
            'At Risk': '#d62728',
            'Hibernating': '#7f7f7f',
        }
        fig = px.bar(
            segment_dist.sort_values('customer_count', ascending=True),
            x='customer_count',
            y='rfm_segment',
            orientation='h',
            color='rfm_segment',
            color_discrete_map=segment_colors,
        )
        fig.update_layout(
            height=400,
            margin=dict(l=20, r=20, t=20, b=20),
            showlegend=False,
            xaxis_title='Number of Customers',
            yaxis_title='',
        )
        st.plotly_chart(fig, use_container_width=True)

    with col_right:
        st.subheader("Segment x Metric Heatmap")
        heatmap_data = segment_dist.set_index('rfm_segment')[['avg_revenue', 'avg_orders', 'avg_aov']]
        heatmap_data.columns = ['Avg Revenue', 'Avg Orders', 'Avg AOV']

        # Normalize for heatmap
        normalized = heatmap_data.apply(lambda x: (x - x.min()) / (x.max() - x.min()) if x.max() != x.min() else 0)

        fig = go.Figure(data=go.Heatmap(
            z=normalized.values,
            x=heatmap_data.columns,
            y=heatmap_data.index,
            text=heatmap_data.values.round(1),
            texttemplate='%{text}',
            colorscale='Blues',
            showscale=False,
        ))
        fig.update_layout(
            height=400,
            margin=dict(l=20, r=20, t=20, b=20),
        )
        st.plotly_chart(fig, use_container_width=True)

    # Revenue share by segment
    st.subheader("Revenue Share by Segment")
    revenue_share = run_query(f"""
        select
            rfm_segment,
            sum(lifetime_revenue) as total_revenue,
            round((sum(lifetime_revenue) / (select sum(lifetime_revenue) from {SCHEMA}.dim_customers) * 100)::numeric, 1) as revenue_pct
        from {SCHEMA}.dim_customers
        group by rfm_segment
        order by total_revenue desc
    """)
    fig = px.pie(
        revenue_share,
        values='total_revenue',
        names='rfm_segment',
        color='rfm_segment',
        color_discrete_map=segment_colors,
        hole=0.4,
    )
    fig.update_layout(
        height=350,
        margin=dict(l=20, r=20, t=20, b=20),
    )
    st.plotly_chart(fig, use_container_width=True)

    # Segment summary table
    st.subheader("Segment Details")
    display_df = segment_dist.copy()
    display_df.columns = ['Segment', 'Customers', 'Avg Revenue', 'Avg Orders', 'Avg AOV']
    display_df['Revenue %'] = (display_df['Avg Revenue'] * display_df['Customers'] /
                                (display_df['Avg Revenue'] * display_df['Customers']).sum() * 100).round(1)
    st.dataframe(display_df, hide_index=True, use_container_width=True)


# ============================================================
# TAB 3: Channel Attribution
# ============================================================
with tab3:
    # Revenue by channel
    channel_revenue = run_query(f"""
        select
            f.channel_group,
            ch.channel_category,
            count(distinct f.order_id) as orders,
            round(sum(f.revenue)::numeric, 2) as revenue,
            count(distinct f.customer_key) as customers
        from {SCHEMA}.fct_orders f
        left join {SCHEMA}.dim_channels ch on f.channel_key = ch.channel_key
        group by f.channel_group, ch.channel_category
        order by revenue desc
    """)

    col_left, col_right = st.columns(2)

    with col_left:
        st.subheader("Revenue by Channel")
        fig = px.bar(
            channel_revenue.sort_values('revenue', ascending=True),
            x='revenue',
            y='channel_group',
            orientation='h',
            color='channel_category',
            color_discrete_map={
                'Paid': '#d62728',
                'Organic': '#2ca02c',
                'Owned': '#1f77b4',
                'Direct': '#ff7f0e',
                'Earned': '#9467bd',
                None: '#7f7f7f',
            },
        )
        fig.update_layout(
            height=400,
            margin=dict(l=20, r=20, t=20, b=20),
            xaxis_title='Revenue ($)',
            yaxis_title='',
        )
        st.plotly_chart(fig, use_container_width=True)

    with col_right:
        st.subheader("Retention Rate by First-Touch Channel")
        retention_by_channel = run_query(f"""
            with first_touch as (
                select
                    c.customer_id,
                    a.attributed_channel as first_channel
                from {SCHEMA}.dim_customers c
                join {SCHEMA}.int_orders_attributed a on c.customer_id = a.customer_id
                where a.order_at = c.first_order_at
            ),
            retention as (
                select
                    ft.first_channel,
                    count(distinct ft.customer_id) as total_customers,
                    count(distinct case when dc.is_repeat_customer then dc.customer_id end) as repeat_customers
                from first_touch ft
                join {SCHEMA}.dim_customers dc on ft.customer_id = dc.customer_id
                group by ft.first_channel
            )
            select
                first_channel,
                total_customers,
                repeat_customers,
                round((repeat_customers::numeric / nullif(total_customers, 0) * 100)::numeric, 1) as retention_rate
            from retention
            where total_customers >= 50
            order by retention_rate desc
        """)
        fig = px.bar(
            retention_by_channel,
            x='first_channel',
            y='retention_rate',
            color_discrete_sequence=['#1f77b4'],
            text='retention_rate',
        )
        fig.update_traces(texttemplate='%{text:.1f}%', textposition='outside')
        fig.update_layout(
            height=400,
            margin=dict(l=20, r=20, t=20, b=20),
            xaxis_title='',
            yaxis_title='Retention Rate (%)',
            yaxis=dict(range=[0, retention_by_channel['retention_rate'].max() * 1.2]),
        )
        st.plotly_chart(fig, use_container_width=True)

    # Cohort retention heatmap
    st.subheader("Cohort Retention Heatmap")
    cohort_data = run_query(f"""
        select
            cohort_month,
            count(*) as cohort_size,
            round(avg(is_retained_30d::int) * 100, 1) as retained_30d,
            round(avg(is_retained_60d::int) * 100, 1) as retained_60d,
            round(avg(is_retained_90d::int) * 100, 1) as retained_90d,
            round(avg(is_retained_180d::int) * 100, 1) as retained_180d,
            round(avg(is_retained_365d::int) * 100, 1) as retained_365d
        from {SCHEMA}.int_cohorts_monthly
        group by cohort_month
        order by cohort_month
    """)

    heatmap_df = cohort_data.set_index('cohort_month')[
        ['retained_30d', 'retained_60d', 'retained_90d', 'retained_180d', 'retained_365d']
    ]
    heatmap_df.columns = ['30 Days', '60 Days', '90 Days', '180 Days', '365 Days']
    heatmap_df.index = pd.to_datetime(heatmap_df.index).strftime('%Y-%m')

    fig = go.Figure(data=go.Heatmap(
        z=heatmap_df.values,
        x=heatmap_df.columns,
        y=heatmap_df.index,
        text=heatmap_df.values,
        texttemplate='%{text:.0f}%',
        colorscale='RdYlGn',
        showscale=True,
        colorbar=dict(title='%'),
    ))
    fig.update_layout(
        height=max(300, len(heatmap_df) * 25),
        margin=dict(l=20, r=20, t=20, b=20),
        xaxis_title='Retention Window',
        yaxis_title='Cohort Month',
        yaxis=dict(autorange='reversed'),
    )
    st.plotly_chart(fig, use_container_width=True)

    # Channel summary table
    st.subheader("Channel Performance Summary")
    st.dataframe(
        channel_revenue.rename(columns={
            'channel_group': 'Channel',
            'channel_category': 'Category',
            'orders': 'Orders',
            'revenue': 'Revenue',
            'customers': 'Customers',
        }),
        hide_index=True,
        use_container_width=True,
    )
