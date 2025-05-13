import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
import pymysql
from streamlit_option_menu import option_menu
from datetime import datetime
from io import BytesIO

# Database connection
@st.cache_resource
def get_db_connection():
    engine = create_engine("mysql+pymysql://root:raja@127.0.0.1/project_nasa")
    conn = pymysql.connect(user='root', password='raja', host='127.0.0.1', database='project_nasa')
    return engine, conn

engine, conn = get_db_connection()
cursor = conn.cursor()

# Page configuration
st.set_page_config(
    page_title="NASA Asteroid Dashboard", 
    layout="wide",
    page_icon="🪐"
)

# Title
st.markdown("<h1 style='text-align:center;'>🚀 NASA Asteroid Tracker 🪐</h1>", unsafe_allow_html=True)
st.markdown("<p style='text-align:center;'>Track and analyze near-Earth asteroid approaches</p>", unsafe_allow_html=True)

# Sidebar menu
with st.sidebar:
    selected = option_menu(
        menu_title='Main Menu',
        options=['Filter Criteria', 'Queries'],
        icons=['filter', 'search'],
        default_index=0,
        menu_icon="asterisk"
    )
    
    st.sidebar.markdown("---")
    st.sidebar.markdown("### Database Info")
    st.sidebar.markdown(f"Last Updated: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    st.sidebar.markdown(f"Total Asteroids: {pd.read_sql('SELECT COUNT(*) FROM asteroids', engine).iloc[0,0]:,}")
    st.sidebar.markdown(f"Total Approaches: {pd.read_sql('SELECT COUNT(*) FROM close_approach', engine).iloc[0,0]:,}")

# Cache initial data load
@st.cache_data
def load_initial_data():
    return pd.read_sql("""
        SELECT a.*, ca.close_approach_date, ca.relative_velocity_kmph, 
               ca.miss_distance_km, ca.miss_distance_lunar, ca.astronomical
        FROM asteroids a
        JOIN close_approach ca ON a.id = ca.neo_reference_id
    """, engine)

# Filter Criteria Section
if selected == "Filter Criteria":
    st.subheader("🎯 Apply Filters to Asteroid Dataset")
    
    try:
        df = load_initial_data()
        
        # UI for filter inputs - organized in columns
        col1, col2, col3 = st.columns(3)
        
        with col1:
            # Date range filter with intelligent defaults
            default_end = pd.to_datetime("today")
            default_start = default_end - pd.Timedelta(days=365)
            date_range = st.date_input(
                "Select approach date range",
                [default_start, default_end],
                min_value=pd.to_datetime(df['close_approach_date'].min()),
                max_value=pd.to_datetime(df['close_approach_date'].max())
            )
            
            # Astronomical Units filter
            st.markdown("**Astronomical Units (AU)**")
            au_min, au_max = st.slider(
                "AU range",
                float(df['astronomical'].min()), 
                float(df['astronomical'].max()),
                (0.0, 0.5),
                step=0.001,
                format="%.3f",
                help="1 AU = ~150 million km (Earth-Sun distance)"
            )
            
        with col2:
            # Lunar distance filter
            st.markdown("**Lunar Distances**")
            lunar_min, lunar_max = st.slider(
                "Distance in lunar units",
                float(df['miss_distance_lunar'].min()), 
                float(df['miss_distance_lunar'].max()),
                (0.0, 20.0),
                step=0.1,
                help="1 Lunar distance = ~384,400 km (Earth-Moon distance)"
            )
            
            # Relative velocity filter
            st.markdown("**Relative Velocity**")
            vel_min, vel_max = st.slider(
                "Velocity (km/h)",
                float(df['relative_velocity_kmph'].min()), 
                float(df['relative_velocity_kmph'].max()),
                (1000.0, 100000.0),
                step=100.0,
                format="%.0f"
            )
            
        with col3:
            # Diameter filters
            st.markdown("**Estimated Diameter (km)**")
            dia_min, dia_max = st.slider(
                "Min diameter range",
                float(df['estimated_diameter_min_km'].min()), 
                float(df['estimated_diameter_min_km'].max()),
                (0.0, 1.0),
                step=0.01,
                format="%.2f"
            )
            
            dia2_min, dia2_max = st.slider(
                "Max diameter range",
                float(df['estimated_diameter_max_km'].min()), 
                float(df['estimated_diameter_max_km'].max()),
                (0.0, 5.0),
                step=0.01,
                format="%.2f"
            )
            
            # Hazardous filter
            hazard_filter = st.selectbox(
                "Potentially Hazardous",
                options=["All", "Yes", "No"],
                index=0,
                help="Asteroids with potential to make threatening close approaches"
            )

        if st.button("Apply Filters", type="primary", help="Apply all selected filters to the dataset"):
            # Convert date range
            start_date = date_range[0]
            end_date = date_range[1] if len(date_range) > 1 else date_range[0]
            
            # Build SQL query with all filters
            query = f"""
                SELECT 
                    a.id, a.name, a.absolute_magnitude_h,
                    a.estimated_diameter_min_km, a.estimated_diameter_max_km,
                    a.is_potentially_hazardous_asteroid,
                    ca.close_approach_date, 
                    ca.relative_velocity_kmph, 
                    ca.miss_distance_km,
                    ca.miss_distance_lunar,
                    ca.astronomical
                FROM asteroids a
                JOIN close_approach ca ON a.id = ca.neo_reference_id
                WHERE ca.close_approach_date BETWEEN '{start_date}' AND '{end_date}'
                  AND ca.astronomical BETWEEN {au_min} AND {au_max}
                  AND ca.miss_distance_lunar BETWEEN {lunar_min} AND {lunar_max}
                  AND ca.relative_velocity_kmph BETWEEN {vel_min} AND {vel_max}
                  AND a.estimated_diameter_min_km BETWEEN {dia_min} AND {dia_max}
                  AND a.estimated_diameter_max_km BETWEEN {dia2_min} AND {dia2_max}
            """

            # Add hazardous filter if specified
            if hazard_filter == "Yes":
                query += " AND a.is_potentially_hazardous_asteroid = 1"
            elif hazard_filter == "No":
                query += " AND a.is_potentially_hazardous_asteroid = 0"

            # Run query with progress indicator
            with st.spinner("Searching asteroid database..."):
                try:
                    result_df = pd.read_sql(query, engine)
                    
                    if not result_df.empty:
                        # Format the display dataframe
                        display_df = result_df.copy()
                        display_df['close_approach_date'] = pd.to_datetime(display_df['close_approach_date']).dt.date
                        display_df['is_potentially_hazardous_asteroid'] = display_df['is_potentially_hazardous_asteroid'].map({1: 'Yes', 0: 'No'})
                        
                        # Rename columns for better display
                        display_df = display_df.rename(columns={
                            'absolute_magnitude_h': 'Magnitude',
                            'estimated_diameter_min_km': 'Min Diameter (km)',
                            'estimated_diameter_max_km': 'Max Diameter (km)',
                            'is_potentially_hazardous_asteroid': 'Hazardous',
                            'relative_velocity_kmph': 'Velocity (km/h)',
                            'miss_distance_km': 'Miss Distance (km)',
                            'miss_distance_lunar': 'Lunar Distance',
                            'astronomical': 'Distance (AU)'
                        })
                        
                        st.success(f"✨ Found {len(result_df):,} matching asteroids")
                        
                        # Display in an expandable section with tabs
                        with st.expander("📊 View Filtered Results", expanded=True):
                            tab1, tab2, tab3 = st.tabs(["Table View", "Summary Statistics", "Visualizations"])
                            
                            with tab1:
                                st.dataframe(
                                    display_df,
                                    column_config={
                                        "close_approach_date": st.column_config.DateColumn("Approach Date"),
                                        "Velocity (km/h)": st.column_config.NumberColumn(format="%.2f"),
                                        "Distance (AU)": st.column_config.NumberColumn(format="%.6f"),
                                        "Lunar Distance": st.column_config.NumberColumn(format="%.2f")
                                    },
                                    hide_index=True,
                                    use_container_width=True
                                )
                            
                            with tab2:
                                st.write("### 📈 Numeric Columns Summary")
                                st.dataframe(result_df.describe())
                                
                                st.write("### 🔍 Top Values")
                                col1, col2 = st.columns(2)
                                with col1:
                                    st.write("**Fastest Asteroids**")
                                    st.dataframe(display_df.nlargest(5, 'Velocity (km/h)')[['name', 'Velocity (km/h)']])
                                with col2:
                                    st.write("**Closest Approaches**")
                                    st.dataframe(display_df.nsmallest(5, 'Miss Distance (km)')[['name', 'Miss Distance (km)']])
                            
                            with tab3:
                                st.write("### 📉 Velocity Distribution")
                                st.bar_chart(result_df['relative_velocity_kmph'].value_counts().head(20))
                                
                                st.write("### 📅 Approaches Over Time")
                                monthly = result_df.groupby(pd.to_datetime(result_df['close_approach_date']).dt.to_period('M')).size()
                                st.line_chart(monthly)
                        
                        # Download options
                        st.markdown("### 💾 Download Options")
                        col1, col2 = st.columns(2)
                        with col1:
                            st.download_button(
                                label="Download CSV",
                                data=result_df.to_csv(index=False).encode('utf-8'),
                                file_name='filtered_asteroids.csv',
                                mime='text/csv',
                                help="Download filtered data as CSV file"
                            )
                        with col2:
                            # Create Excel file in memory
                            output = BytesIO()
                            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                                result_df.to_excel(writer, index=False)
                            excel_data = output.getvalue()
                            
                            st.download_button(
                                label="Download Excel",
                                data=excel_data,
                                file_name='filtered_asteroids.xlsx',
                                mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                                help="Download filtered data as Excel file"
                            )
                    else:
                        st.warning("⚠️ No asteroids match your filter criteria. Try expanding your search parameters.")
                        
                except Exception as e:
                    st.error(f"❌ Error executing query: {str(e)}")
    
    except Exception as e:
        st.error(f"❌ Error loading initial data: {str(e)}")

# Queries Section
elif selected == "Queries":
    # Layout Columns
    st.title("🌍 NASA Asteroid Close Approaches Dashboard")
    st.markdown("Explore pre-defined queries about asteroid close approaches to Earth")

    # Query Execution Helper with caching
    @st.cache_data(show_spinner="Executing query...")
    def run_query(query):
        return pd.read_sql(query, engine)

    col1, col2 = st.columns(2)

    # ------------------ Column 1 ------------------
    with col1:
        st.header("🛰️ Asteroid Approaches")
        
        with st.expander("🔍 Discovery Patterns", expanded=True):
            # 1. Count how many times each asteroid has approached Earth
            st.write("#### Approach Frequency")
            if st.button("1. Show Asteroid Approach Counts"):
                query = """SELECT a.name, COUNT(*) AS approach_count 
                           FROM close_approach c
                           JOIN asteroids a ON c.neo_reference_id = a.id
                           GROUP BY c.neo_reference_id, a.name
                           ORDER BY approach_count DESC 
                           LIMIT 20;"""
                df = run_query(query)
                st.dataframe(df)
                st.bar_chart(df.set_index('name')['approach_count'])
            
            # 2. Average velocity of asteroids
            st.write("#### Velocity Analysis")
            if st.button("2. Show Average Velocity of Asteroids"):
                query = """SELECT a.name, AVG(c.relative_velocity_kmph) AS avg_velocity 
                           FROM close_approach c
                           JOIN asteroids a ON c.neo_reference_id = a.id
                           GROUP BY c.neo_reference_id, a.name
                           ORDER BY avg_velocity DESC
                           LIMIT 20;"""
                df = run_query(query)
                st.dataframe(df)
                st.bar_chart(df.set_index('name')['avg_velocity'])

        with st.expander("⚠️ Hazard Analysis"):
            # 4. Potentially Hazardous Asteroids (>3 approaches)
            st.write("#### Frequent Hazardous Asteroids")
            if st.button("4. Show Hazardous Asteroids with >3 Approaches"):
                query = """
                SELECT a.name, COUNT(c.neo_reference_id) AS approach_count 
                FROM asteroids a 
                JOIN close_approach c ON a.id = c.neo_reference_id 
                WHERE a.is_potentially_hazardous_asteroid = TRUE 
                GROUP BY a.id, a.name 
                HAVING approach_count > 3 
                ORDER BY approach_count DESC;"""
                df = run_query(query)
                st.dataframe(df)
                
            # Hazardous vs Non-Hazardous Asteroid Count
            st.write("#### Hazardous vs Safe Asteroids")
            if st.button("13. Show Hazardous Counts"):
                query = """
                SELECT CASE WHEN is_potentially_hazardous_asteroid = 1 THEN 'Hazardous' 
                       ELSE 'Non-Hazardous' END AS asteroid_type, 
                       COUNT(*) AS asteroid_count 
                FROM asteroids 
                GROUP BY is_potentially_hazardous_asteroid;"""
                df = run_query(query)
                st.dataframe(df)
                st.bar_chart(df.set_index('asteroid_type')['asteroid_count'])

    # ------------------ Column 2 ------------------
    with col2:
        st.header("🌠 Advanced Queries")
        
        with st.expander("🚀 Top Performers"):
            # 3. Top 10 Fastest Asteroids
            st.write("#### Fastest Approaches")
            if st.button("3. Show Top 10 Fastest Asteroids"):
                query = """SELECT a.name, c.relative_velocity_kmph, c.close_approach_date 
                           FROM close_approach c 
                           JOIN asteroids a ON c.neo_reference_id = a.id 
                           ORDER BY c.relative_velocity_kmph DESC 
                           LIMIT 10;"""
                df = run_query(query)
                st.dataframe(df)
                
            # 7. Largest Asteroids
            st.write("#### Largest Asteroids")
            if st.button("7. Show Largest Asteroids by Diameter"):
                query = """
                SELECT name, estimated_diameter_max_km 
                FROM asteroids 
                ORDER BY estimated_diameter_max_km DESC 
                LIMIT 10;"""
                df = run_query(query)
                st.dataframe(df)
                st.bar_chart(df.set_index('name')['estimated_diameter_max_km'])
                
            # 16. Largest Non-Hazardous Asteroids
            st.write("#### Largest Safe Asteroids")
            if st.button("16. Show Largest Non-Hazardous Asteroids"):
                query = """
                 SELECT name, estimated_diameter_max_km
                 FROM asteroids
                 WHERE is_potentially_hazardous_asteroid = 0
                 ORDER BY estimated_diameter_max_km DESC
                 LIMIT 10;"""
                df = run_query(query)
                st.dataframe(df)

        with st.expander("📅 Temporal Patterns"):
            # 19. Year with most approaches
            st.write("#### Annual Approach Counts")
            if st.button("19. Show Year with Most Approaches"):
                query = """
                SELECT YEAR(close_approach_date) AS approach_year, COUNT(*) AS approach_count 
                FROM close_approach 
                GROUP BY approach_year 
                ORDER BY approach_count DESC;"""
                df = run_query(query)
                st.dataframe(df)
                st.bar_chart(df.set_index('approach_year')['approach_count'])
                
            # 11. Monthly approaches
            st.write("#### Monthly Approach Patterns")
            if st.button("11. Show Monthly Approaches"):
                query = """
                SELECT DATE_FORMAT(close_approach_date, '%%Y-%%m') AS approach_month, COUNT(*) AS approach_count 
                FROM close_approach 
                GROUP BY approach_month 
                ORDER BY approach_month;"""
                df = run_query(query)
                st.dataframe(df)
                st.line_chart(df.set_index('approach_month')['approach_count'])

    # ------------------ Full Width Section ------------------
    st.header("📊 Other Important Insights")
    
    with st.expander("🌑 Close Approaches"):
        # 14. Asteroids closer than Moon
        st.write("#### Closer than Moon Approaches")
        if st.button("14. Show Approaches Closer than Moon"):
            query = """
            SELECT a.name, c.close_approach_date, c.miss_distance_lunar 
            FROM close_approach c 
            JOIN asteroids a ON c.neo_reference_id = a.id 
            WHERE c.miss_distance_lunar < 1 
            ORDER BY c.miss_distance_lunar ASC;"""
            df = run_query(query)
            st.dataframe(df.head(20))
            
        # 15. Approaches within 0.05 AU
        st.write("#### Very Close Approaches (<0.05 AU)")
        if st.button("15. Show Approaches < 0.05 AU"):
            query = """
            SELECT a.name, c.close_approach_date, c.astronomical 
            FROM close_approach c 
            JOIN asteroids a ON c.neo_reference_id = a.id 
            WHERE c.astronomical < 0.05 
            ORDER BY c.astronomical ASC;"""
            df = run_query(query)
            st.dataframe(df.head(20))
            
    with st.expander("🔭 Observation Data"):
        # 12. Brightest Asteroid
        st.write("#### Brightest Asteroids")
        if st.button("12. Show Brightest Asteroid"):
            query = """
            SELECT name, absolute_magnitude_h 
            FROM asteroids 
            ORDER BY absolute_magnitude_h ASC 
            LIMIT 10;"""
            df = run_query(query)
            st.dataframe(df)
            
        # 18. Asteroids < 0.1 AU and Magnitude > 20
        st.write("#### Close & Bright Asteroids")
        if st.button("18. Show Asteroids within 0.1 AU & Magnitude > 20"):
            query = """
            SELECT a.name, c.astronomical, a.absolute_magnitude_h 
            FROM close_approach c 
            JOIN asteroids a ON c.neo_reference_id = a.id 
            WHERE c.astronomical < 0.1 AND a.absolute_magnitude_h > 20
            ORDER BY c.astronomical ASC;"""
            df = run_query(query)
            st.dataframe(df)

# Add footer
st.markdown("---")
st.markdown("""
    <div style='text-align: center;'>
        <p>NASA Asteroid Dashboard • Data from JPL Small-Body Database</p>
        <p>Note: All distances and velocities are relative to Earth</p>
    </div>
""", unsafe_allow_html=True)