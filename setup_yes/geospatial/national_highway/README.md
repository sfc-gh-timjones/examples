# 🛣️ Highway Geospatial & Weather Analysis

This project provides an end-to-end solution for ingesting, analyzing, and visualizing geospatial and weather data related to national highways directly within Snowflake.

---

## 📁 Repository Structure

### `geojson/` **(Data Folder)**

* Contains the **GeoJSON** files necessary for the project.
* **Data Ingestion:** These files must be downloaded and manually uploaded to a **Snowflake Internal Stage**.
* > 💡 **Production Note:** In a real-world production environment, this step would be automated, placing files directly into the Internal Stage or a Cloud Storage Bucket (e.g., S3, Azure Blob) referenced by a Snowflake **External Stage**.

---

## 📝 Key Files

| File Name | Type | Purpose |
| :--- | :--- | :--- |
| `national_highway_weather.sql` | SQL | The primary script for **end-to-end data ingestion and geospatial analysis**. Run this script to process the raw data. |
| `highway_visualization.py` | Python | The **Streamlit application script** for visualizing the processed highway and weather data in Snowsight. |

---

## 📊 Streamlit Visualization Setup

The `highway_visualization.py` script is used to power a Streamlit application directly in the Snowflake UI (Snowsight).

### Setup Steps:

1.  **Create the App:**
    * Navigate to **Projects** $\rightarrow$ **Streamlit** $\rightarrow$ **+ Streamlit App**.
    * **Configuration:** Name the application, select the appropriate database/schema, and choose a **Warehouse** (a **Medium** size is recommended). Click "**Create**."
2.  **Paste Code:** Copy the content of `highway_visualization.py` and paste it into the Snowsight Streamlit editor.
3.  **Install Dependencies:** The app requires external Python libraries. You will initially see an error (e.g., `"No module named 'pydeck'"`) until these are installed.
    * Use the **"Packages"** dropdown located above the script editor.
    * Search for and **install** the required libraries: **`pydeck`** and **`plotly`**.

The Streamlit application will now run successfully, displaying the data visualization.
