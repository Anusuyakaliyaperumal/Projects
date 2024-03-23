# YouTube-API-project

# YouTube Data Harvesting and Warehousing

## Table of Contents
- [Overview]
- [Features]
- [Installation]
- [Usage]
- [Data Warehousing]
- [Streamlit App]
  [Sample Queries]
## Overview
This project allows you to harvest and warehouse YouTube data, including channel information, video details, and video comments. It leverages the YouTube Data API to retrieve data, stores it in both a MongoDB database and a SQLite database, and provides a Streamlit web application to interact with the data and run SQL queries.

## Features
- Harvest YouTube channel information, video details, and video comments.
- Store data in MongoDB for scalability and flexibility.
- Create SQL database tables for structured data storage.
- Develop a Streamlit web application for data visualization and querying.

## Installation
1. Clone this repository to your local machine.
2. Install the required Python packages using `pip`:
   ```
   pip install -r requirements.txt
   ```

## Usage
1. Obtain a YouTube Data API key from the [Google Developer Console](https://console.cloud.google.com/).
2. Update the `api_key` variable in the `api_connection` function in `project.py` with your API key.
3. Run the `project.py` script to start harvesting and warehousing YouTube data.

## Data Warehousing
The project stores data in two databases:

### MongoDB
- MongoDB is used for flexible and scalable data storage.
- The database name is `YouTube_Data`.
- It contains three collections: `YouTube_Channels`, `YouTube_Videos`, and `YouTube_Comments`.

### SQLite
- SQLite is used for structured data storage.
- It creates three tables: `Channels`, `Videos`, and `Comments` in a database named `youtubedata.db`.

## Streamlit App
- Run the Streamlit app using the following command:
  ```
  streamlit run project.py
  ```

- The app provides a user interface to:
  - Retrieve data for a YouTube channel.
  - View stored data in tables.
  - Execute SQL queries on the data.

## Sample Queries
- The project includes sample SQL queries for data analysis and reporting. You can customize and expand these queries according to your requirements.

## Contributing
Contributions to this project are welcome. You can contribute by opening issues, suggesting enhancements, or creating pull requests.
