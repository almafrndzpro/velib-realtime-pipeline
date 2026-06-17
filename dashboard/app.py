"""
dashboard/app.py
─────────────────
Vélib' Métropole — Network Operations Center

Targeted at: redistribution managers (Smovengo field operations teams).
Answers one question: where do trucks need to go right now?

3 sections:
  1. Operational Overview  — network health KPIs + fill-rate distribution chart
  2. Intervention Map      — critical stations only, searchable, map zooms to results
  3. Priority List         — ranked redistribution targets (dispatch / collect) + 24h trend

Dashboard visualisations (6 total, satisfies the ≥5 requirement):
  [1] KPI metric cards
  [2] Network distribution bar chart
  [3] Folium interactive map
  [4] Dispatch priority table
  [5] Collect priority table
  [6] 24-hour network trend line chart
"""

import os
import time
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from sqlalchemy import create_engine, text

st.set_page_config(
    page_title="Vélib' Operations Center",
    layout="wide",
    initial_sidebar_state="expanded",
)

VELIB_BLUE  = "#3D9BE9"
VELIB_NAVY  = "#1A2E5A"
VELIB_RED   = "#E74C3C"
VELIB_GRAY  = "#F4F6F9"
VELIB_BORDER= "#DDE3ED"

st.markdown(f"""
<style>
  html, body, [class*="css"] {{
    font-family: 'Segoe UI', Arial, sans-serif;
    background: {VELIB_GRAY};
  }}
  .block-container {{ padding-top: 2.8rem; padding-bottom: 3rem; max-width: 1400px; }}

  /*Section title*/
  .section-title {{
    font-size: 14px; font-weight: 700; color: {VELIB_NAVY};
    border-left: 4px solid {VELIB_BLUE};
    padding-left: 10px; margin: 28px 0 16px 0;
    text-transform: uppercase; letter-spacing: 0.5px;
  }}

  /*KPI cards*/
  .kpi-row {{ display: flex; gap: 14px; margin-bottom: 24px; }}
  .kpi-card {{
    background: white; border-radius: 10px;
    border: 1px solid {VELIB_BORDER};
    padding: 20px 24px; flex: 1;
    box-shadow: 0 1px 3px rgba(0,0,0,0.05);
  }}
  .kpi-card.urgent {{ border-left: 4px solid {VELIB_RED}; }}
  .kpi-card.blue   {{ border-left: 4px solid {VELIB_BLUE}; }}
  .kpi-card.green  {{ border-left: 4px solid #27AE60; }}
  .kpi-card.gray   {{ border-left: 4px solid #95A5A6; }}
  .kpi-label {{ font-size: 11px; color: #999; font-weight: 600;
                text-transform: uppercase; letter-spacing: 0.5px; }}
  .kpi-value {{ font-size: 30px; font-weight: 800; color: {VELIB_NAVY};
                line-height: 1.1; margin: 5px 0 3px 0; }}
  .kpi-value.red   {{ color: {VELIB_RED}; }}
  .kpi-value.blue  {{ color: {VELIB_BLUE}; }}
  .kpi-value.green {{ color: #27AE60; }}
  .kpi-sub {{ font-size: 12px; color: #aaa; }}

  /*Alert strip*/
  .alert-strip {{
    background: #FEF3F2; border: 1px solid #FCCEC9;
    border-radius: 8px; padding: 10px 16px;
    font-size: 13px; color: #922B21; font-weight: 600;
    margin-bottom: 14px;
  }}

  /*Search results banner*/
  .search-banner {{
    background: #EBF5FB; border: 1px solid #AED6F1;
    border-radius: 8px; padding: 9px 14px;
    font-size: 13px; color: #1A5276; font-weight: 600;
    margin-bottom: 12px;
  }}

  /*Sidebar*/
  section[data-testid="stSidebar"] {{
    background: white; border-right: 1px solid {VELIB_BORDER};
  }}
  hr {{ border: none; border-top: 1px solid {VELIB_BORDER}; margin: 28px 0; }}
</style>
""", unsafe_allow_html=True)

#Database
POSTGRES_CONN = os.getenv("POSTGRES_CONN", "postgresql://velib:velib@localhost:5432/velib")

@st.cache_resource
def get_engine():
    return create_engine(POSTGRES_CONN)

def load_stations() -> pd.DataFrame:
    with get_engine().connect() as conn:
        return pd.read_sql(text("""
            SELECT station_number, station_name, address,
                   latitude, longitude, bike_stands AS capacity,
                   available_bikes, available_stands, fill_pct, status, ingested_at
            FROM v_stations_current
            WHERE latitude IS NOT NULL AND longitude IS NOT NULL
              AND latitude != 0 AND longitude != 0
        """), conn)

def load_kpis() -> dict:
    with get_engine().connect() as conn:
        row = conn.execute(text("SELECT * FROM v_kpis")).fetchone()
        return dict(row._mapping) if row else {}

def load_hourly() -> pd.DataFrame:
    with get_engine().connect() as conn:
        return pd.read_sql(text("""
            SELECT hour_bucket,
                   AVG(avg_bikes)        AS avg_bikes,
                   AVG(availability_pct) AS avg_fill
            FROM analytics_station_hourly
            WHERE hour_bucket >= NOW() - INTERVAL '24 hours'
            GROUP BY hour_bucket ORDER BY hour_bucket
        """), conn)

def fmt(n) -> str:
    if n is None: return "—"
    try:    return f"{int(n):,}".replace(",", " ")
    except: return str(n)

def fmt_pct(n) -> str:
    if n is None: return "—"
    try:    return f"{float(n):.1f}%"
    except: return str(n)

#Load data
try:
    df_all = load_stations()
    kpis   = load_kpis()
    hourly = load_hourly()
except Exception as e:
    st.error(f"Database connection error: {e}")
    st.info("Make sure the pipeline has run at least once (see SETUP.md).")
    st.stop()

if df_all.empty:
    st.warning("No data yet. Trigger the Airflow DAG at http://localhost:8081 (admin / admin).")
    st.stop()

df_open = df_all[df_all["status"] == "OPEN"].copy()

#Derived operational metrics
needs_bikes      = df_open[df_open["fill_pct"] < 15]
needs_collection = df_open[df_open["fill_pct"] > 85]
balanced         = df_open[(df_open["fill_pct"] >= 15) & (df_open["fill_pct"] <= 85)]
offline          = df_all[df_all["status"] != "OPEN"]
balance_score    = round(len(balanced) / len(df_open) * 100) if len(df_open) else 0
last_refresh     = kpis.get("last_refresh")

#Auto-refresh every 30 s
if "last_run" not in st.session_state:
    st.session_state["last_run"] = time.time()
if (time.time() - st.session_state["last_run"]) > 30:
    st.session_state["last_run"] = time.time()
    st.rerun()
st.session_state["last_run"] = time.time()

#Sidebar
with st.sidebar:
    LOGO_PATH = os.path.join(os.path.dirname(__file__), "velib_logo.png")
    if os.path.exists(LOGO_PATH):
        st.image(LOGO_PATH, width=110)
    else:
        st.markdown(
            f"<div style='font-size:26px;font-weight:900;color:{VELIB_BLUE};"
            f"letter-spacing:-1px;margin-bottom:2px'>vélib'</div>"
            f"<div style='font-size:10px;font-weight:700;color:{VELIB_NAVY};"
            f"letter-spacing:2px;margin-bottom:16px'>MÉTROPOLE</div>",
            unsafe_allow_html=True,
        )

    st.markdown(
        f"<div style='font-size:13px;font-weight:700;color:{VELIB_NAVY};"
        f"margin-bottom:2px'>Operations Center</div>"
        f"<div style='font-size:11px;color:#aaa;margin-bottom:6px'>"
        f"Redistribution planning tool</div>",
        unsafe_allow_html=True,
    )
    st.markdown("<hr>", unsafe_allow_html=True)

    st.markdown("**Search area**")
    st.markdown(
        "<div style='font-size:11px;color:#aaa;margin-bottom:6px'>"
        "Filter by municipality, arrondissement, or station name.<br>"
        "Examples: <i>Paris 11</i>, <i>Boulogne</i>, <i>Bastille</i>"
        "</div>",
        unsafe_allow_html=True,
    )
    search_query = st.text_input(
        label="Search",
        placeholder="Paris 11, Boulogne, Bastille...",
        label_visibility="collapsed",
    )

    st.markdown("<hr>", unsafe_allow_html=True)

    st.markdown("**Map view**")
    show_mode = st.radio(
        label="Map mode",
        options=["Critical stations only", "All open stations"],
        label_visibility="collapsed",
    )

    st.markdown("<hr>", unsafe_allow_html=True)

    refresh_str = str(last_refresh)[:16] if last_refresh else "—"
    st.markdown(
        f"<div style='font-size:11px;color:#bbb;line-height:1.8'>"
        f"Last data update<br>"
        f"<b style='color:{VELIB_NAVY}'>{refresh_str}</b><br><br>"
        f"Auto-refresh: every 30 s"
        f"</div>",
        unsafe_allow_html=True,
    )

# Apply global search filter
# Search applies everywhere: map AND priority list
search_active = bool(search_query.strip())
if search_active:
    q = search_query.strip().lower()
    mask = (
        df_open["station_name"].str.lower().str.contains(q, na=False) |
        df_open["address"].str.lower().str.contains(q, na=False)
    )
    df_scope = df_open[mask].copy()
else:
    df_scope = df_open.copy()

# Needs-bikes / needs-collection scoped to search
scope_dispatch   = df_scope[df_scope["fill_pct"] < 15]
scope_collection = df_scope[df_scope["fill_pct"] > 85]

# Header
LOGO_PATH = os.path.join(os.path.dirname(__file__), "velib_logo.png")
if os.path.exists(LOGO_PATH):
    col_logo, col_text = st.columns([1, 11])
    with col_logo:
        st.image(LOGO_PATH, width=60)
    with col_text:
        st.markdown(f"""
        <div style="padding-top:8px">
          <span style="font-size:23px;font-weight:800;color:{VELIB_NAVY}">
            Vélib' Métropole &nbsp;—&nbsp; Network Operations Center
          </span><br>
          <span style="font-size:13px;color:#888">
            Redistribution planning &nbsp;·&nbsp; Last update: {refresh_str}
            &nbsp;·&nbsp; <span style="color:{VELIB_RED};font-weight:700">● LIVE</span>
          </span>
        </div>
        """, unsafe_allow_html=True)
else:
    st.markdown(f"""
    <div style="display:flex;align-items:center;gap:16px;background:white;
                border:1px solid {VELIB_BORDER};border-radius:10px;
                padding:14px 22px;margin-bottom:16px;
                box-shadow:0 1px 4px rgba(0,0,0,0.06)">
      <div style="background:{VELIB_BLUE};border-radius:8px;
                  padding:7px 12px;color:white;font-size:20px;font-weight:900;
                  letter-spacing:-1px;line-height:1.1">
        vélib'<br><span style="font-size:8px;letter-spacing:3px">MÉTROPOLE</span>
      </div>
      <div>
        <div style="font-size:20px;font-weight:800;color:{VELIB_NAVY}">
          Network Operations Center
        </div>
        <div style="font-size:12px;color:#888">
          Redistribution planning &nbsp;·&nbsp; Last update: {refresh_str}
          &nbsp;·&nbsp; <span style="color:{VELIB_RED};font-weight:700">● LIVE</span>
        </div>
      </div>
    </div>
    """, unsafe_allow_html=True)

st.markdown("<div style='margin-top:16px'></div>", unsafe_allow_html=True)

# Search results banner
if search_active:
    st.markdown(
        f'<div class="search-banner">'
        f'Showing {len(df_scope):,} stations matching "{search_query}" &nbsp;·&nbsp; '
        f'{len(scope_dispatch)} need bikes dispatched &nbsp;·&nbsp; '
        f'{len(scope_collection)} need bikes collected'
        f'</div>',
        unsafe_allow_html=True,
    )

# Critical alert strip
critical = df_open[df_open["fill_pct"] < 5]
if len(critical) > 0:
    st.markdown(
        f'<div class="alert-strip">'
        f'⚠ {len(critical)} station{"s" if len(critical)>1 else ""} critically empty '
        f'(below 5% fill rate) — immediate dispatch required'
        f'</div>',
        unsafe_allow_html=True,
    )

# SECTION 1 — OPERATIONAL OVERVIEW
st.markdown('<div class="section-title">1 — Operational Overview</div>', unsafe_allow_html=True)

# KPI cards
st.markdown(f"""
<div class="kpi-row">
  <div class="kpi-card urgent">
    <div class="kpi-label">Needs bikes dispatched</div>
    <div class="kpi-value red">{fmt(len(needs_bikes))}</div>
    <div class="kpi-sub">stations below 15% fill rate</div>
  </div>
  <div class="kpi-card blue">
    <div class="kpi-label">Needs bikes collected</div>
    <div class="kpi-value blue">{fmt(len(needs_collection))}</div>
    <div class="kpi-sub">stations above 85% fill rate</div>
  </div>
  <div class="kpi-card green">
    <div class="kpi-label">Network balance score</div>
    <div class="kpi-value green">{balance_score}%</div>
    <div class="kpi-sub">stations in normal range (15–85%)</div>
  </div>
  <div class="kpi-card gray">
    <div class="kpi-label">Stations offline</div>
    <div class="kpi-value">{fmt(len(offline))}</div>
    <div class="kpi-sub">closed / not renting, out of {fmt(len(df_all))}</div>
  </div>
</div>
""", unsafe_allow_html=True)

# [VISUALISATION 2] — Network Distribution Chart
# Classify all open stations into 5 operational zones
def classify_zone(pct):
    if pct is None:         return "Unknown"
    if pct < 5:             return "Critical — empty (<5%)"
    if pct < 15:            return "Low — dispatch needed (5–15%)"
    if pct <= 85:           return "Normal (15–85%)"
    if pct <= 95:           return "High — collect needed (85–95%)"
    return "Critical — full (>95%)"

zone_order = [
    "Critical — empty (<5%)",
    "Low — dispatch needed (5–15%)",
    "Normal (15–85%)",
    "High — collect needed (85–95%)",
    "Critical — full (>95%)",
]
zone_colors = {
    "Critical — empty (<5%)":       "#E74C3C",
    "Low — dispatch needed (5–15%)": "#F39C12",
    "Normal (15–85%)":               "#27AE60",
    "High — collect needed (85–95%)":"#2980B9",
    "Critical — full (>95%)":        "#1A5276",
}

df_open["zone"] = df_open["fill_pct"].apply(classify_zone)
zone_counts = (
    df_open.groupby("zone")
    .size()
    .reset_index(name="count")
)
zone_counts["zone"] = pd.Categorical(zone_counts["zone"], categories=zone_order, ordered=True)
zone_counts = zone_counts.sort_values("zone")
zone_counts["color"] = zone_counts["zone"].map(zone_colors)
zone_counts["pct"] = (zone_counts["count"] / zone_counts["count"].sum() * 100).round(1)
zone_counts["label"] = zone_counts.apply(
    lambda r: f"{r['count']:,}  ({r['pct']}%)".replace(",", " "), axis=1
)

fig_dist = go.Figure(go.Bar(
    x=zone_counts["count"],
    y=zone_counts["zone"].astype(str),
    orientation="h",
    text=zone_counts["label"],
    textposition="auto",       # auto: inside if bar is wide enough, outside if narrow
    textfont=dict(size=12, color="white"),
    marker_color=zone_counts["color"].tolist(),
    hovertemplate="<b>%{y}</b><br>%{x} stations<extra></extra>",
))
fig_dist.update_layout(
    height=240,
    plot_bgcolor="white", paper_bgcolor="white",
    xaxis=dict(showgrid=True, gridcolor="#eee", title="Number of stations",
               range=[0, zone_counts["count"].max() * 1.02]),
    yaxis=dict(title="", tickfont=dict(size=12)),
    margin=dict(l=10, r=20, t=30, b=20),
    title=dict(
        text="Station Distribution by Operational Zone",
        font=dict(size=13, color=VELIB_NAVY), x=0, pad=dict(b=10)
    ),
)
st.plotly_chart(fig_dist, use_container_width=True)

st.markdown("<hr>", unsafe_allow_html=True)

# SECTION 2 — INTERVENTION MAP
st.markdown('<div class="section-title">2 — Intervention Map</div>', unsafe_allow_html=True)

# Select which stations to plot
if show_mode == "Critical stations only":
    df_map = df_scope[(df_scope["fill_pct"] < 15) | (df_scope["fill_pct"] > 85)]
else:
    df_map = df_scope

if df_map.empty:
    st.info("No stations match the current search and filter. Try a different search term.")
else:
    try:
        import folium
        from streamlit_folium import st_folium

        # When search is active, zoom to the bounding box of results
        if search_active and not df_map.empty:
            lat_min, lat_max = df_map["latitude"].min(), df_map["latitude"].max()
            lon_min, lon_max = df_map["longitude"].min(), df_map["longitude"].max()
            center_lat = (lat_min + lat_max) / 2
            center_lon = (lon_min + lon_max) / 2
            # Rough zoom: tighter bbox → higher zoom
            lat_span = max(lat_max - lat_min, 0.005)
            zoom = min(15, max(11, int(13 - lat_span * 20)))
        else:
            center_lat = df_map["latitude"].mean()
            center_lon = df_map["longitude"].mean()
            zoom = 13

        m = folium.Map(
            location=[center_lat, center_lon],
            zoom_start=zoom,
            tiles="CartoDB positron",
        )

        # When search is active and zoomed in, also fit bounds
        if search_active and not df_map.empty and len(df_map) < 200:
            m.fit_bounds([
                [df_map["latitude"].min(), df_map["longitude"].min()],
                [df_map["latitude"].max(), df_map["longitude"].max()],
            ])

        for _, row in df_map.iterrows():
            if pd.isna(row["latitude"]) or pd.isna(row["longitude"]):
                continue
            pct = row.get("fill_pct")

            if pct is None:
                color, border = "#aab7b8", "#7f8c8d"
            elif pct < 5:
                color, border = "#e74c3c", "#922b21"
            elif pct < 15:
                color, border = "#f39c12", "#d68910"
            elif pct > 95:
                color, border = "#1a5276", "#154360"
            elif pct > 85:
                color, border = "#2980b9", "#1a5276"
            else:
                color, border = "#aab7b8", "#7f8c8d"

            if pct is not None and pct < 15:
                recommended = (
                    f"Dispatch approx. "
                    f"{max(0, int(row['capacity'] * 0.5) - int(row['available_bikes']))} bikes"
                )
            elif pct is not None and pct > 85:
                recommended = (
                    f"Collect approx. "
                    f"{max(0, int(row['available_bikes']) - int(row['capacity'] * 0.5))} bikes"
                )
            else:
                recommended = "No action needed"

            popup_html = f"""
            <div style="font-family:'Segoe UI',sans-serif;font-size:13px;min-width:230px">
              <div style="background:{VELIB_NAVY};color:white;padding:8px 12px;
                          border-radius:6px 6px 0 0;font-weight:700">
                {row['station_name']}
              </div>
              <div style="padding:10px 12px;border:1px solid #dde3ed;border-top:none;
                          border-radius:0 0 6px 6px;background:white;line-height:1.9">
                <b>Area:</b> {row.get('address', '—')}<br>
                <b>Bikes available:</b> {fmt(row['available_bikes'])} / {fmt(row['capacity'])}<br>
                <b>Free docks:</b> {fmt(row['available_stands'])}<br>
                <b>Fill rate:</b> {fmt_pct(pct)}<br>
                <b style="color:{VELIB_RED}">Action:</b> {recommended}
              </div>
            </div>
            """
            radius = 8 if (pct is not None and (pct < 15 or pct > 85)) else 4
            folium.CircleMarker(
                location=[row["latitude"], row["longitude"]],
                radius=radius,
                color=border, fill=True, fill_color=color, fill_opacity=0.85,
                popup=folium.Popup(popup_html, max_width=290),
                tooltip=f"{row['station_name']} — {fmt_pct(pct)}",
            ).add_to(m)

        legend_html = f"""
        <div style="position:fixed;bottom:36px;left:36px;z-index:1000;
                    background:white;padding:12px 16px;border-radius:10px;
                    border:1px solid #ccc;font-family:'Segoe UI',sans-serif;
                    font-size:12px;line-height:2.1;box-shadow:0 2px 8px rgba(0,0,0,0.12)">
          <b style="display:block;margin-bottom:2px;color:{VELIB_NAVY}">Intervention status</b>
          <span style="color:#e74c3c;font-size:15px">&#9679;</span>&nbsp; Critical empty (&lt;5%)<br>
          <span style="color:#f39c12;font-size:15px">&#9679;</span>&nbsp; Dispatch needed (&lt;15%)<br>
          <span style="color:#2980b9;font-size:15px">&#9679;</span>&nbsp; Collect needed (&gt;85%)<br>
          <span style="color:#1a5276;font-size:15px">&#9679;</span>&nbsp; Critical full (&gt;95%)<br>
          <span style="color:#aab7b8;font-size:15px">&#9679;</span>&nbsp; Normal — no action
        </div>
        """
        m.get_root().html.add_child(folium.Element(legend_html))
        st_folium(m, width=None, height=490, returned_objects=[])

    except ImportError:
        df_viz = df_map.dropna(subset=["latitude", "longitude", "fill_pct"])
        fig_map = px.scatter_mapbox(
            df_viz, lat="latitude", lon="longitude", color="fill_pct",
            color_continuous_scale=["#e74c3c", "#f39c12", "#27ae60", "#2980b9", "#1a5276"],
            range_color=[0, 100], hover_name="station_name",
            hover_data={"available_bikes": True, "available_stands": True, "fill_pct": ":.1f"},
            mapbox_style="carto-positron", zoom=zoom if search_active else 12,
            center={"lat": center_lat, "lon": center_lon},
            height=490,
            labels={"fill_pct": "Fill rate (%)"},
        )
        fig_map.update_layout(margin={"r": 0, "t": 0, "l": 0, "b": 0})
        st.plotly_chart(fig_map, use_container_width=True)

st.markdown("<hr>", unsafe_allow_html=True)

# SECTION 3 — REDISTRIBUTION PRIORITY LIST + 24H TREND
area_note = f' in "{search_query}"' if search_active else ""
st.markdown('<div class="section-title">3 — Redistribution Priority List</div>', unsafe_allow_html=True)

tab_dispatch, tab_collect, tab_trend = st.tabs([
    f"Dispatch bikes{area_note} ({len(scope_dispatch)} stations)",
    f"Collect bikes{area_note}  ({len(scope_collection)} stations)",
    "24 h Network Trend",
])


def build_priority_table(df_src: pd.DataFrame, mode: str) -> pd.DataFrame:
    cols = ["station_name", "address", "available_bikes", "available_stands", "capacity", "fill_pct"]
    t = df_src[cols].copy()

    if mode == "dispatch":
        t = t.sort_values("fill_pct")
        t["bikes_needed"] = (t["capacity"] * 0.5 - t["available_bikes"]).clip(lower=0).astype(int)
        t["priority"] = t["fill_pct"].apply(
            lambda x: "Critical" if x < 5 else "Urgent" if x < 15 else "Watch"
        )
        t = t.rename(columns={
            "station_name":  "Station",
            "address":       "Area",
            "available_bikes": "Bikes now",
            "available_stands": "Free docks",
            "capacity":      "Capacity",
            "fill_pct":      "Fill rate",
            "bikes_needed":  "Bikes to dispatch",
            "priority":      "Priority",
        })
    else:
        t = t.sort_values("fill_pct", ascending=False)
        t["bikes_to_remove"] = (t["available_bikes"] - t["capacity"] * 0.5).clip(lower=0).astype(int)
        t["priority"] = t["fill_pct"].apply(
            lambda x: "Critical" if x > 95 else "Urgent" if x > 85 else "Watch"
        )
        t = t.rename(columns={
            "station_name":  "Station",
            "address":       "Area",
            "available_bikes": "Bikes now",
            "available_stands": "Free docks",
            "capacity":      "Capacity",
            "fill_pct":      "Fill rate",
            "bikes_to_remove": "Bikes to collect",
            "priority":      "Priority",
        })

    t["Bikes now"]  = t["Bikes now"].apply(fmt)
    t["Free docks"] = t["Free docks"].apply(fmt)
    t["Capacity"]   = t["Capacity"].apply(fmt)
    t["Fill rate"]  = t["Fill rate"].apply(fmt_pct)
    return t.reset_index(drop=True)


with tab_dispatch:
    st.markdown(
        f"<div style='font-size:13px;color:#666;margin-bottom:10px'>"
        f"Stations where trucks should <b>bring bikes</b>{area_note}. "
        f"Sorted by urgency — address <span style='color:{VELIB_RED}'>Critical</span> stations first."
        f"</div>",
        unsafe_allow_html=True,
    )
    if scope_dispatch.empty:
        st.success(f"No stations currently need bikes dispatched{area_note}.")
    else:
        t = build_priority_table(scope_dispatch, "dispatch")
        st.dataframe(
            t, use_container_width=True, hide_index=True, height=360,
            column_config={
                "Priority":          st.column_config.TextColumn("Priority"),
                "Fill rate":         st.column_config.TextColumn("Fill rate"),
                "Bikes to dispatch": st.column_config.NumberColumn(
                    "Est. bikes to dispatch",
                    help="Estimated bikes to reach 50% fill rate"
                ),
            },
        )

with tab_collect:
    st.markdown(
        f"<div style='font-size:13px;color:#666;margin-bottom:10px'>"
        f"Stations where trucks should <b>pick up bikes</b>{area_note}. "
        f"Sorted by urgency — address <span style='color:{VELIB_BLUE}'>Critical</span> stations first."
        f"</div>",
        unsafe_allow_html=True,
    )
    if scope_collection.empty:
        st.success(f"No stations currently need bikes collected{area_note}.")
    else:
        t = build_priority_table(scope_collection, "collect")
        st.dataframe(
            t, use_container_width=True, hide_index=True, height=360,
            column_config={
                "Priority":          st.column_config.TextColumn("Priority"),
                "Fill rate":         st.column_config.TextColumn("Fill rate"),
                "Bikes to collect":  st.column_config.NumberColumn(
                    "Est. bikes to collect",
                    help="Estimated bikes to remove to reach 50% fill rate"
                ),
            },
        )

with tab_trend:
    st.markdown(
        "<div style='font-size:13px;color:#666;margin-bottom:10px'>"
        "Average number of available bikes across the network over the last 24 hours. "
        "Use this to anticipate when the next redistribution wave will be needed "
        "(dips below the red line signal a network-wide shortage)."
        "</div>",
        unsafe_allow_html=True,
    )
    if hourly.empty:
        st.info("Trend data will appear after the pipeline has been running for at least one hour.")
    else:
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=hourly["hour_bucket"],
            y=hourly["avg_bikes"],
            name="Avg bikes available per station",
            line=dict(color=VELIB_BLUE, width=2.5),
            fill="tozeroy",
            fillcolor="rgba(61,155,233,0.07)",
            hovertemplate="<b>%{y:.1f}</b> bikes avg<extra></extra>",
        ))
        fig.add_hrect(
            y0=0, y1=3,
            fillcolor="rgba(231,76,60,0.07)", line_width=0,
            annotation_text="Critical threshold",
            annotation_position="bottom left",
            annotation_font_size=11,
            annotation_font_color=VELIB_RED,
        )
        fig.update_layout(
            height=300,
            plot_bgcolor="white", paper_bgcolor="white",
            yaxis=dict(title="Avg bikes / station", gridcolor="#eee"),
            xaxis=dict(title="", gridcolor="#eee"),
            legend=dict(orientation="h", yanchor="bottom", y=1.02),
            hovermode="x unified",
            margin=dict(l=0, r=0, t=20, b=0),
        )
        st.plotly_chart(fig, use_container_width=True)

# Footer 
st.markdown(
    f"<div style='text-align:center;font-size:11px;color:#ccc;padding-top:16px'>"
    f"Vélib' Network Operations Center &nbsp;·&nbsp; "
    f"Paris Open Data · Kafka · PostgreSQL · Apache Airflow &nbsp;·&nbsp; "
    f"ESILV MSc A4 — ETL & Pipeline Orchestration (MACSIN4A2125)"
    f"</div>",
    unsafe_allow_html=True,
)
