# TED Talks Data Uploader

A simple Streamlit application to upload TED talks YouTube data to Snowflake.

## Features

- Automatically creates Snowflake warehouse, database, and schema
- Creates a table structure for TED talks data
- Uploads CSV data to Snowflake
- Provides a preview of the uploaded data

## Setup

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Create `.streamlit/secrets.toml` with your Snowflake credentials:
```toml
[snowflake]
user = "your_username"
password = "your_password"
account = "your_account"
```

3. Make sure your `youtube_data_TED.csv` file is in the same directory as the application.

## Usage

1. Run the Streamlit app:
```bash
streamlit run main.py
```

2. The app will automatically initialize Snowflake resources (warehouse, database, schema, and table).

3. Click "Upload TED Talks Data" to upload the CSV file to Snowflake.

4. Use "Preview Data" to view the first 5 rows of the uploaded data.

## Snowflake Resources Created

- Warehouse: `TED_TALKS_WH` (XSmall, auto-suspend after 60s)
- Database: `TED_TALKS_DB`
- Schema: `TED_TALKS_SCHEMA`
- Table: `ted_talks` 