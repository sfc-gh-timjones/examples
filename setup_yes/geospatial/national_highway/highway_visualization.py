# Import python packages
import streamlit as st
import pandas as pd
import pydeck as pdk
from snowflake.snowpark.context import get_active_session
import plotly.express as px
import plotly.graph_objects as go

# Set page config for wide layout
st.set_page_config(page_title="Highway Weather Visualization", layout="wide")

# CSS to maximize canvas width and reduce white space
st.markdown("""
<style>
/* Force maximum width usage */
.main > div {
    max-width: 100% !important;
    padding-left: 1rem !important;
    padding-right: 1rem !important;
}

/* Remove Streamlit's default container width limits */
.block-container {
    max-width: 100% !important;
    padding-left: 1rem !important;
    padding-right: 1rem !important;
}

/* Smaller cursor on map */
.mapboxgl-canvas-container canvas {
    cursor: crosshair !important;
}

/* Force full width for all content */
.stApp > div:first-child {
    max-width: 100% !important;
}

/* Remove default margins */
.css-1d391kg {
    padding: 1rem !important;
}
</style>
""", unsafe_allow_html=True)

# Write directly to the app
st.title("🛣️ Los Angeles Highway System Visualization")
st.write("Interactive visualization of Los Angeles highway data with geospatial analysis")
st.info("🌴 **Displaying highways for city: Los Angeles** from `demo.geospatial.national_highway_system_subset`")

# Get the current credentials
session = get_active_session()

# Sidebar for controls


# Hour filter for weather data
st.sidebar.subheader("⏰ Time Filter")

# Initialize session state for hour range and map view
if 'hour_range' not in st.session_state:
    st.session_state.hour_range = (0, 23)
if 'map_view_state' not in st.session_state:
    st.session_state.map_view_state = {
        'latitude': 36.7783,
        'longitude': -119.4179,
        'zoom': 6,
        'pitch': 0
    }

# Quick time presets in sidebar
st.sidebar.markdown("**Quick Presets:**")
col1, col2 = st.sidebar.columns(2)
with col1:
    if st.button("12 AM - 4 AM", key="preset1"):
        st.session_state.hour_range = (0, 3)
    if st.button("8 AM - 12 PM", key="preset3"):
        st.session_state.hour_range = (8, 11)
    if st.button("4 PM - 8 PM", key="preset5"):
        st.session_state.hour_range = (16, 19)
with col2:
    if st.button("4 AM - 8 AM", key="preset2"):
        st.session_state.hour_range = (4, 7)
    if st.button("12 PM - 4 PM", key="preset4"):
        st.session_state.hour_range = (12, 15)
    if st.button("8 PM - 12 AM", key="preset6"):
        st.session_state.hour_range = (20, 23)

if st.sidebar.button("All Hours", key="preset_all"):
    st.session_state.hour_range = (0, 23)

hour_range = st.sidebar.slider(
    "Select hour range for weather data",
    min_value=0,
    max_value=23,
    value=st.session_state.hour_range,
    help="Filter weather data by hour of day (0 = midnight, 23 = 11 PM)",
    key="hour_slider"
)

# Main query
@st.cache_data
def load_highway_data(hour_start, hour_end):
    # Query with weather data joined and filtered by hour
    query = f"""
    WITH weather_summary AS (
        SELECT 
            CITY_NAME,
            MAX(DATE(DATETIME)) AS LATEST_DATE,
            ROUND(AVG(HUMIDITY_RELATIVE), 1) AS AVG_HUMIDITY,
            ROUND(SUM(RAIN_LWE), 2) AS TOTAL_PRECIPITATION,
            SUM(MINUTES_OF_PRECIPITATION) AS TOTAL_PRECIP_MINUTES,
            ROUND(AVG(TEMPERATURE), 1) AS AVG_TEMPERATURE,
            ROUND(AVG(WIND_SPEED), 1) AS AVG_WIND_SPEED
        FROM ACCUWEATHERS_HISTORICAL_WEATHER_DATA_SAMPLE.HISTORICAL.TOP_CITY_HOURLY_IMPERIAL
        WHERE CITY_NAME = 'Los Angeles'
        AND DATE_PART('HH', DATETIME) BETWEEN {hour_start} AND {hour_end}
        GROUP BY CITY_NAME
    )
    SELECT 
        h.* EXCLUDE (AVERAGE_ANNUAL_DAILY_TRAFFIC),
        -- Use pre-computed GeoJSON column
        h.GEOJSON as geojson_geometry,
        -- Include traffic data with comma formatting
        TO_CHAR(h.AVERAGE_ANNUAL_DAILY_TRAFFIC, '999,999,999') AS AVERAGE_ANNUAL_DAILY_TRAFFIC,
        -- Add weather data
        w.LATEST_DATE,
        w.AVG_HUMIDITY,
        w.TOTAL_PRECIPITATION,
        w.TOTAL_PRECIP_MINUTES,
        w.AVG_TEMPERATURE,
        w.AVG_WIND_SPEED
    FROM DEMO.GEOSPATIAL.NATIONAL_HIGHWAY_SYSTEM_SUBSET h
    LEFT JOIN weather_summary w ON h.CITY_NAME = w.CITY_NAME
    WHERE h.CITY_NAME = 'Los Angeles'
    """
    
    return session.sql(query).to_pandas()

# Weather trends query
@st.cache_data
def load_weather_trends(hour_start, hour_end):
    query = f"""
    SELECT 
        CITY_NAME,
        DATETIME,
        DATE_PART('HH',DATETIME) as Hour_of_Day,
        HUMIDITY_RELATIVE,
        MINUTES_OF_PRECIPITATION,
        RAIN_LWE,
        TEMPERATURE,
        WIND_GUST,
        WIND_SPEED
    FROM ACCUWEATHERS_HISTORICAL_WEATHER_DATA_SAMPLE.HISTORICAL.TOP_CITY_HOURLY_IMPERIAL
    WHERE CITY_NAME = 'Los Angeles'
    AND DATE_PART('HH', DATETIME) BETWEEN {hour_start} AND {hour_end}
    ORDER BY DATETIME
    """
    
    return session.sql(query).to_pandas()

# Load data
with st.spinner("Loading highway and weather data..."):
    df = load_highway_data(hour_range[0], hour_range[1])
    weather_df = load_weather_trends(hour_range[0], hour_range[1])

if df.empty:
    st.warning("No highway data found with the current filters. Try expanding your search criteria.")
    st.stop()

# Display summary statistics
col1, col2, col3 = st.columns(3)
with col1:
    st.metric("Total Linestring Segments", f"{len(df):,}")
with col2:
    st.metric("Total Roadway Miles", f"{df['MILES'].sum():.0f} mi")
with col3:
    # Only calculate avg speed limit for speeds > 0
    speed_data = df[df['SPEED_LIMIT'] > 0]['SPEED_LIMIT']
    if len(speed_data) > 0:
        avg_speed = speed_data.mean()
        st.metric("Avg Speed Limit", f"{avg_speed:.0f} mph")
    else:
        st.metric("Avg Speed Limit", "N/A")

# Show data processing info
if not df.empty and 'path' in df.columns:
    valid_paths = df['path'].apply(len) > 0
    st.write(f"✅ Successfully processed {valid_paths.sum():,} highway geometries for visualization")


# Create tabs for different visualizations
tab1, tab2, tab3 = st.tabs(["🗺️ Map View", "🌤️ Weather Trends", "📊 Analytics"])

with tab1:
    st.subheader("Highway Network Map")
    
    # Show current time filter
    hour_text = f"Hours {hour_range[0]}:00-{hour_range[1]}:00" if hour_range[0] != hour_range[1] else f"Hour {hour_range[0]}:00"
    st.info(f"**Current Time Filter:** {hour_text} (Use sidebar to change)")
    
    # CSS to make the map use full screen width
    st.markdown("""
    <style>
    .stApp > div:first-child > div:first-child > div:first-child {
        padding: 0;
        margin: 0;
    }
    .main .block-container {
        max-width: 100vw;
        width: 100vw;
        padding: 0;
        margin: 0;
    }
    .element-container {
        width: 100%;
    }
    div[data-testid="stDecoration"] {
        display: none;
    }
    </style>
    """, unsafe_allow_html=True)
    
    # Create map layers
    if not df.empty:
        layers = []
        
        # Convert GeoJSON strings to coordinate arrays for PathLayer
        def parse_geojson_coordinates(geojson_str):
            import json
            try:
                geojson = json.loads(geojson_str)
                coords = []
                
                if geojson['type'] == 'LineString':
                    coords = geojson['coordinates']
                elif geojson['type'] == 'MultiLineString':
                    # For MultiLineString, return the first linestring
                    coords = geojson['coordinates'][0] if geojson['coordinates'] else []
                
                # Simplify coordinates to reduce data size
                # Keep every Nth point based on length
                if len(coords) > 20:
                    step = max(1, len(coords) // 10)  # Keep ~10 points max
                    coords = coords[::step]
                
                return coords
            except:
                return []
        
        # Add coordinate paths to dataframe
        st.write("🔄 Processing highway geometries...")
        df['path'] = df['GEOJSON_GEOMETRY'].apply(parse_geojson_coordinates)
        
        # Filter out rows with empty paths
        df_with_paths = df[df['path'].apply(len) > 0].copy()
        
        # Ensure all data is serializable for PyDeck
        # Convert any numpy/pandas types to native Python types and handle NaN values
        for col in df_with_paths.columns:
            if col == 'path':  # Skip the path column
                continue
            try:
                col_dtype = df_with_paths[col].dtype
                if col_dtype == 'object':
                    # Convert object columns to strings and handle NaN
                    df_with_paths[col] = df_with_paths[col].astype(str).replace('nan', 'N/A')
                elif 'float' in str(col_dtype):
                    # Fill NaN values and convert to float
                    df_with_paths[col] = df_with_paths[col].fillna(0).astype(float)
                elif 'int' in str(col_dtype):
                    # Fill NaN values and convert to int
                    df_with_paths[col] = df_with_paths[col].fillna(0).astype(int)
            except Exception as e:
                # Skip problematic columns silently
                continue
        
        # Estimate data size
        total_coords = sum(len(path) for path in df_with_paths['path'])
        estimated_size_mb = (total_coords * 16 * 2) / (1024 * 1024)  # Rough estimate
        
        st.write(f"📊 **Data Summary:**")
        st.write(f"- Total linestrings loaded: {len(df):,}")
        st.write(f"- Valid geometries for mapping: {len(df_with_paths):,}")
        st.write(f"- Total coordinate points: {total_coords:,}")
        st.write(f"- Estimated data size: {estimated_size_mb:.1f} MB")
        
        if estimated_size_mb > 25:
            st.error(f"⚠️ Data size ({estimated_size_mb:.1f} MB) may exceed 32MB limit. Reduce highway count!")
        elif estimated_size_mb > 15:
            st.warning(f"⚠️ Data size ({estimated_size_mb:.1f} MB) is getting large. Consider reducing highway count.")
        
        if len(df_with_paths) == 0:
            st.error("❌ No valid highway geometries found for mapping. Check your filters.")
            st.stop()
        
        # Highway lines layer - GREEN roads with BLUE hover highlighting using PathLayer
        highway_layer = pdk.Layer(
            "PathLayer",
            data=df_with_paths,
            get_path='path',
            get_color='[34, 139, 34, 200]',  # Forest Green with slightly more opacity
            get_width=8,  # Doubled from 4 to 8 for much thicker lines
            width_min_pixels=4,  # Doubled minimum width
            width_max_pixels=20,  # Increased maximum width for hover
            pickable=True,
            auto_highlight=True,
            highlight_color=[0, 100, 255, 220]  # Blue highlight on hover
        )
        layers.append(highway_layer)
        
        # No reference point needed for Los Angeles-only view
        
        # Set view state - use session state to preserve zoom/pan
        # Add a unique key to prevent reset on rerun
        view_state = pdk.ViewState(
            latitude=st.session_state.map_view_state['latitude'],
            longitude=st.session_state.map_view_state['longitude'],
            zoom=st.session_state.map_view_state['zoom'],
            pitch=st.session_state.map_view_state['pitch']
        )
        
        # Create tooltip text for Los Angeles highways with weather data
        hour_text = f"Hours {hour_range[0]}:00-{hour_range[1]}:00" if hour_range[0] != hour_range[1] else f"Hour {hour_range[0]}:00"
        
        tooltip_text = f"""Highway: {{SIGN1}} - {{SPEED_LIMIT}} mph speed limit
Segment Length: {{MILES}} miles
Avg Daily Traffic: {{AVERAGE_ANNUAL_DAILY_TRAFFIC}} vehicles
Date: {{LATEST_DATE}}
--- Weather Data ({hour_text}) ---
Total Precipitation: {{TOTAL_PRECIPITATION}} in
Precip Minutes: {{TOTAL_PRECIP_MINUTES}} min
Avg Temperature: {{AVG_TEMPERATURE}}°F
Avg Wind Speed: {{AVG_WIND_SPEED}} mph
Avg Humidity: {{AVG_HUMIDITY}}%"""
        
        # Create deck with natural/satellite style map
        deck = pdk.Deck(
            layers=layers,
            initial_view_state=view_state,
            map_style='light',  # Light/natural style background
            tooltip={"text": tooltip_text},
            height=800
        )
        
        st.pydeck_chart(deck, use_container_width=True, height=800, key="highway_map")

with tab2:
    hour_text = f"Hours {hour_range[0]}:00-{hour_range[1]}:00" if hour_range[0] != hour_range[1] else f"Hour {hour_range[0]}:00"
    st.subheader(f"Weather Trends - Los Angeles ({hour_text})")
    
    if not weather_df.empty:
        # Group by hour for 24-hour trends
        hourly_weather = weather_df.groupby('HOUR_OF_DAY').agg({
            'HUMIDITY_RELATIVE': 'mean',
            'TEMPERATURE': 'mean',
            'WIND_SPEED': 'mean',
            'WIND_GUST': 'mean',
            'RAIN_LWE': 'sum',
            'MINUTES_OF_PRECIPITATION': 'sum'
        }).reset_index()
        
        # Create weather trend charts
        col1, col2 = st.columns(2)
    
        with col1:
            # Temperature and Humidity
            fig_temp_humid = go.Figure()
            fig_temp_humid.add_trace(go.Scatter(
                x=hourly_weather['HOUR_OF_DAY'],
                y=hourly_weather['TEMPERATURE'],
                mode='lines+markers',
                name='Temperature (°F)',
                line=dict(color='red')
            ))
            fig_temp_humid.add_trace(go.Scatter(
                x=hourly_weather['HOUR_OF_DAY'],
                y=hourly_weather['HUMIDITY_RELATIVE'],
                mode='lines+markers',
                name='Humidity (%)',
                yaxis='y2',
                line=dict(color='blue')
            ))
            fig_temp_humid.update_layout(
                title='Temperature & Humidity Over 24 Hours',
                xaxis_title='Hour of Day',
                yaxis=dict(title='Temperature (°F)', side='left'),
                yaxis2=dict(title='Humidity (%)', side='right', overlaying='y'),
                height=400
            )
            st.plotly_chart(fig_temp_humid, use_container_width=True)
            
            # Precipitation
            fig_precip = px.bar(
                hourly_weather,
                x='HOUR_OF_DAY',
                y='RAIN_LWE',
                title='Precipitation by Hour',
                labels={'RAIN_LWE': 'Precipitation (inches)', 'HOUR_OF_DAY': 'Hour of Day'}
            )
            st.plotly_chart(fig_precip, use_container_width=True)
        
        with col2:
            # Wind Speed and Gusts
            fig_wind = go.Figure()
            fig_wind.add_trace(go.Scatter(
                x=hourly_weather['HOUR_OF_DAY'],
                y=hourly_weather['WIND_SPEED'],
                mode='lines+markers',
                name='Wind Speed (mph)',
                line=dict(color='green')
            ))
            fig_wind.add_trace(go.Scatter(
                x=hourly_weather['HOUR_OF_DAY'],
                y=hourly_weather['WIND_GUST'],
                mode='lines+markers',
                name='Wind Gust (mph)',
                line=dict(color='orange')
            ))
            fig_wind.update_layout(
                title='Wind Speed & Gusts Over 24 Hours',
                xaxis_title='Hour of Day',
                yaxis_title='Speed (mph)',
                height=400
            )
            st.plotly_chart(fig_wind, use_container_width=True)
            
            # Precipitation Minutes
            fig_precip_min = px.bar(
                hourly_weather,
                x='HOUR_OF_DAY',
                y='MINUTES_OF_PRECIPITATION',
                title='Minutes of Precipitation by Hour',
                labels={'MINUTES_OF_PRECIPITATION': 'Minutes', 'HOUR_OF_DAY': 'Hour of Day'}
            )
            st.plotly_chart(fig_precip_min, use_container_width=True)
        
        # Weather summary statistics - make horizontal
        st.write("**📅 Weather Summary:**")
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Avg Temperature", f"{hourly_weather['TEMPERATURE'].mean():.1f}°F")
        with col2:
            st.metric("Avg Humidity", f"{hourly_weather['HUMIDITY_RELATIVE'].mean():.1f}%")
        with col3:
            st.metric("Total Precipitation", f"{hourly_weather['RAIN_LWE'].sum():.2f} in")
        with col4:
            st.metric("Avg Wind Speed", f"{hourly_weather['WIND_SPEED'].mean():.1f} mph")
    
    else:
        st.warning("No weather data available for Los Angeles.")

with tab3:
    st.subheader("Highway Analytics")
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Linestring length distribution (moved to left)
        fig_miles = px.histogram(
            df, 
            x='MILES', 
            title='Linestring Length Distribution',
            labels={'MILES': 'Linestring Length (miles)', 'count': 'Number of Linestrings'}
        )
        # Set initial x-axis range to 0-0.5 miles with scroll capability
        fig_miles.update_layout(
            xaxis=dict(
                range=[0, 0.5],
                title='Linestring Length (miles)'
            ),
            dragmode='pan'
        )
        fig_miles.update_xaxes(fixedrange=False)  # Enable horizontal scrolling
        st.plotly_chart(fig_miles, use_container_width=True)
        
        # Data summary - make horizontal
        st.write("**📊 Data Summary:**")
        col_a, col_b, col_c, col_d = st.columns(4)
        with col_a:
            if 'YEAR' in df.columns and df['YEAR'].notna().any():
                year_data = df.dropna(subset=['YEAR'])
                st.metric("Records w/ Year", f"{len(year_data):,}")
            else:
                st.metric("Records w/ Year", "0")
        with col_b:
            st.metric("Longest Segment", f"{df['MILES'].max():.1f} mi")
        with col_c:
            st.metric("Shortest Segment", f"{df['MILES'].min():.1f} mi")
        with col_d:
            st.metric("Avg Segment", f"{df['MILES'].mean():.1f} mi")
    
    with col2:
        # Speed limit distribution (moved to right)
        fig_speed = px.histogram(
            df, 
            x='SPEED_LIMIT', 
            title='Speed Limit Distribution',
            labels={'SPEED_LIMIT': 'Speed Limit (mph)', 'count': 'Number of Highways'}
        )
        st.plotly_chart(fig_speed, use_container_width=True)
        
        # Speed limit statistics - make horizontal
        st.write("**📈 Speed Limit Statistics:**")
        speed_stats = df[df['SPEED_LIMIT'] > 0]['SPEED_LIMIT']
        if len(speed_stats) > 0:
            col_a, col_b, col_c = st.columns(3)
            with col_a:
                st.metric("Most Common", f"{speed_stats.mode().iloc[0]:.0f} mph")
            with col_b:
                st.metric("Highest", f"{speed_stats.max():.0f} mph")
            with col_c:
                st.metric("Lowest", f"{speed_stats.min():.0f} mph")
        else:
            st.write("No speed limit data available")

# Footer
st.markdown("---")
st.markdown("**Data Source:** National Highway System GeoJSON + AccuWeather Historical Data | **Built with:** Streamlit + Snowflake")
