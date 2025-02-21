import streamlit as st

# Set page config for wide layout
st.set_page_config(
    page_title="Videos",
    page_icon="🎥",
    layout="wide"
)

st.title("Videos")

# Display results if we have them
if st.session_state.search_results is not None and not st.session_state.search_results.empty:
    results = st.session_state.search_results
    total_count = st.session_state.total_count
    
    st.success(f"Found {total_count} relevant videos")
    
    # Create rows of 4 videos each
    for i in range(0, len(results), 4):
        cols = st.columns(4)
        # Get the next 4 videos (or remaining videos if less than 4)
        row_videos = results.iloc[i:min(i+4, len(results))]
        
        # Display each video in its column
        for col, (_, video) in zip(cols, row_videos.iterrows()):
            with col:
                with st.container(border=True):
                    if video['THUMBNAIL']:
                        st.image(video['THUMBNAIL'], use_container_width=True)
                    st.markdown(f"**{video['VIDEO_TITLE'][:40]}...**" if len(video['VIDEO_TITLE']) > 40 else f"**{video['VIDEO_TITLE']}**")
                    st.caption(f"Year: {video['VIDEO_YEAR']}")
                    with st.expander("Description"):
                        st.write(video['VIDEO_DESCRIPTION'][:150] + "..." if len(video['VIDEO_DESCRIPTION']) > 150 else video['VIDEO_DESCRIPTION'])
elif st.session_state.search_results is not None:
    st.warning("No results found.")
else:
    st.info("Use the chat on the home page to search for videos. Results will appear here!") 
