import streamlit as st
import pandas as pd
from snowflake.snowpark import Session
from snowflake.core import Root
from snowflake.cortex import Complete
import os

# Set page config for wide layout
st.set_page_config(
    page_title="Video Search Chat",
    layout="wide"
)

# Snowflake connection parameters
SNOWFLAKE_CONFIG = {
    "user": st.secrets["snowflake"]["user"],
    "password": st.secrets["snowflake"]["password"],
    "account": st.secrets["snowflake"]["account"],
    "warehouse": "TED_TALKS_WH",
    "database": "TED_TALKS_DB",
    "schema": "TED_TALKS_SCHEMA"
}

# Constants
MAX_RESULTS_PER_PAGE = 50
DEFAULT_LIMIT = 100

def get_snowflake_session():
    """Get a Snowflake session."""
    return Session.builder.configs(SNOWFLAKE_CONFIG).create()

def reset_table():
    """Drop and recreate the ted_talks table."""
    session = get_snowflake_session()
    try:
        # Drop existing table if it exists
        session.sql("DROP TABLE IF EXISTS ted_talks").collect()
        
        # Create table for TED talks data with TIMESTAMP_NTZ for VIDEO_YEAR
        session.sql("""
            CREATE TABLE IF NOT EXISTS ted_talks (
                VIDEO_TITLE VARCHAR(1000),
                THUMBNAIL VARCHAR(1000),
                VIDEO_DESCRIPTION VARCHAR(4000),
                VIDEO_YEAR TIMESTAMP_NTZ
            )
        """).collect()
        return True
    except Exception as e:
        st.error(f"Error resetting table: {e}")
        return False

def initialize_snowflake():
    """Initialize Snowflake resources."""
    session = get_snowflake_session()
    try:
        # Create warehouse
        session.sql("""
            CREATE WAREHOUSE IF NOT EXISTS TED_TALKS_WH
            WITH WAREHOUSE_SIZE = 'XSMALL'
            AUTO_SUSPEND = 60
            AUTO_RESUME = TRUE
        """).collect()
        
        # Create database
        session.sql("CREATE DATABASE IF NOT EXISTS TED_TALKS_DB").collect()
        
        # Create schema
        session.sql("CREATE SCHEMA IF NOT EXISTS TED_TALKS_DB.TED_TALKS_SCHEMA").collect()
        
        # Set context
        session.sql("USE DATABASE TED_TALKS_DB").collect()
        session.sql("USE SCHEMA TED_TALKS_SCHEMA").collect()
        session.sql("USE WAREHOUSE TED_TALKS_WH").collect()
        
        # Create table if it doesn't exist
        session.sql("""
            CREATE TABLE IF NOT EXISTS ted_talks (
                VIDEO_TITLE VARCHAR(1000),
                THUMBNAIL VARCHAR(1000),
                VIDEO_DESCRIPTION VARCHAR(4000),
                VIDEO_YEAR TIMESTAMP_NTZ
            )
        """).collect()
        
        return True
    except Exception as e:
        st.error(f"Error initializing Snowflake: {e}")
        return False

def get_top_results_summary(results_df):
    """Generate a coherent summary for top 3 results using Cortex LLM Complete."""
    try:
        session = get_snowflake_session()
        
        # Create a single prompt with all three videos
        videos = []
        for idx in range(min(3, len(results_df))):
            row = results_df.iloc[idx]
            videos.append(f"""Talk {idx + 1}:
Title: {row['VIDEO_TITLE']}
Description: {row['VIDEO_DESCRIPTION']}""")
        
        prompt = f"""Here are the top 3 TED talks from a search. Please provide a coherent summary that connects these talks and their main themes in 3-4 sentences:

{chr(10).join(videos)}"""
        
        response = Complete(
            model="mistral-large2",
            prompt=prompt,
            session=session
        )
        
        return [{
            'title': 'Top 3 Talks Summary',
            'summary': response.strip()
        }]
    except Exception as e:
        st.error(f"Error generating summaries: {e}")
        return None

def upload_csv_to_snowflake(file_path):
    """Upload CSV data to Snowflake."""
    session = get_snowflake_session()
    try:
        # Read CSV file
        df = pd.read_csv(file_path)
        
        # Convert Year to timestamp (set to January 1st of each year)
        df['Year'] = pd.to_datetime(df['Year'].astype(str) + '-01-01')
        
        # Rename columns to match Snowflake table
        df = df.rename(columns={
            'Title': 'VIDEO_TITLE',
            'Thumbnail URL': 'THUMBNAIL',
            'Description': 'VIDEO_DESCRIPTION',
            'Year': 'VIDEO_YEAR'
        })
        
        # Convert DataFrame to Snowpark DataFrame
        snowpark_df = session.create_dataframe(df)
        
        # Write to Snowflake table
        snowpark_df.write.mode("overwrite").save_as_table("ted_talks")
        return True
    except Exception as e:
        st.error(f"Error uploading data: {e}")
        return False

def semantic_search(query: str, page: int = 1, year_range: tuple = None):
    """Perform semantic search using Cortex Search Service."""
    try:
        session = get_snowflake_session()
        root = Root(session)
        
        # Get the search service
        svc = root.databases["TED_TALKS_DB"].schemas["TED_TALKS_SCHEMA"].cortex_search_services["TED_SEARCH_SERVICE_CS"]
        
        # Calculate offset based on page number
        offset = (page - 1) * MAX_RESULTS_PER_PAGE
        
        # Create filter if year range is provided
        filter_obj = None
        if year_range:
            min_year, max_year = year_range
            # Convert years to YYYY-MM-DD format
            min_date = f"{min_year:04d}-01-01"
            max_date = f"{max_year:04d}-12-31"
            if min_year and max_year:
                filter_obj = {
                    "@and": [
                        {"@gte": {"VIDEO_YEAR": min_date}},
                        {"@lte": {"VIDEO_YEAR": max_date}}
                    ]
                }
        
        # Perform the search with pagination and filter
        response = svc.search(
            query,
            ["VIDEO_TITLE", "VIDEO_DESCRIPTION", "THUMBNAIL", "VIDEO_YEAR"],
            limit=MAX_RESULTS_PER_PAGE,
            offset=offset,
            filter=filter_obj
        )
        
        # Convert results to DataFrame
        results_df = pd.DataFrame(response.results)
        
        # Convert VIDEO_YEAR back to just the year for display
        if not results_df.empty and 'VIDEO_YEAR' in results_df.columns:
            results_df['VIDEO_YEAR'] = pd.to_datetime(results_df['VIDEO_YEAR']).dt.year
        
        # Get total count for pagination
        total_count = len(response.results)  # Using response length since filters affect total count
        
        return results_df, total_count
    except Exception as e:
        st.error(f"Error performing semantic search: {e}")
        return None, 0

def main():
    st.title("Video Search Chat")
    
    # Initialize session state for chat history and search results
    if 'messages' not in st.session_state:
        st.session_state.messages = []
    if 'search_results' not in st.session_state:
        st.session_state.search_results = None
        st.session_state.total_count = 0
        st.session_state.selected_years = None
    
    # Create two columns for chat and results
    chat_col, results_col = st.columns([1, 1])
    
    with chat_col:
        st.markdown("### Chat")
        
        # Display chat messages
        for message in st.session_state.messages:
            with st.chat_message(message["role"]):
                st.markdown(message["content"])
        
        # Get min and max years from the database for the filter
        session = get_snowflake_session()
        try:
            year_range = session.sql("SELECT YEAR(MIN(VIDEO_YEAR)), YEAR(MAX(VIDEO_YEAR)) FROM ted_talks").collect()[0]
            min_db_year, max_db_year = year_range[0], year_range[1]
        except:
            min_db_year, max_db_year = 2000, 2024  # Fallback values
        
        # Year filter in expander using fragment
        @st.fragment
        def year_filter():
            with st.expander("", expanded=False):
                st.write("Filter videos by year range:")
                years = st.slider(
                    "Select year range",
                    min_value=int(min_db_year),
                    max_value=int(max_db_year),
                    value=(int(min_db_year), int(max_db_year)),
                    key="year_filter"
                )
                st.session_state.selected_years = years
                current_filter = f"Filtering years: {years[0]} to {years[1]}"
                st.caption(current_filter)
        
        # Call the fragment function
        year_filter()
        
        # Chat input
        if prompt := st.chat_input("Ask about videos..."):
            # Add user message to chat history
            st.session_state.messages.append({"role": "user", "content": prompt})
            
            # Display user message
            with st.chat_message("user"):
                st.markdown(prompt)
            
            # Display assistant response
            with st.chat_message("assistant"):
                with st.spinner("Searching..."):
                    # Perform search
                    results, total_count = semantic_search(prompt, 1, st.session_state.selected_years)
                    st.session_state.search_results = results
                    st.session_state.total_count = total_count
                    
                    if results is not None and not results.empty:
                        # Generate summary
                        summaries = get_top_results_summary(results)
                        if summaries:
                            response = summaries[0]['summary']
                            st.markdown(response)
                            # Add assistant response to chat history
                            st.session_state.messages.append({"role": "assistant", "content": response})
                        else:
                            st.error("Could not generate summary.")
                    else:
                        response = "I couldn't find any relevant videos for your query."
                        st.markdown(response)
                        st.session_state.messages.append({"role": "assistant", "content": response})
    
    with results_col:
        st.markdown("### Search Results")
        
        # Display results if we have them
        if st.session_state.search_results is not None and not st.session_state.search_results.empty:
            results = st.session_state.search_results
            total_count = st.session_state.total_count
            
            st.success(f"Found {total_count} relevant videos")
            
            # Display results in a nice format
            for _, row in results.iterrows():
                with st.container(border=True):
                    st.markdown(f"### {row['VIDEO_TITLE']}")
                    st.write(f"**Year:** {row['VIDEO_YEAR']}")
                    if row['THUMBNAIL']:
                        st.image(row['THUMBNAIL'], use_container_width=True)
                    with st.expander("Show Description"):
                        st.write(row['VIDEO_DESCRIPTION'])
        elif st.session_state.search_results is not None:
            st.warning("No results found.")

if __name__ == "__main__":
    main() 