# Social Media Ads Data Pipeline

This repository contains Python scripts for fetching and processing advertising data from **Facebook Ads** and **Reddit Ads**. The scripts retrieve insights on campaign performance and store the data in Google BigQuery for further analysis.

## 📂 Overview

### **1. `facebook.py`**
- Fetches advertising insights from **Facebook Ads API**.
- Retrieves data on **campaigns, ad sets, ads, clicks, impressions, reach, and spend**.
- Uses the **Google BigQuery** client to store the results in the `Ads_Meta` dataset.
- Implements a **batch processing mechanism** to handle large datasets efficiently.
- Handles **asynchronous requests** for fetching reports.

### **2. `reddit.py`**
- Connects to the **Reddit Ads API** and retrieves **ad spend** data.
- Uses **OAuth authentication** to get access tokens.
- Fetches **daily ad spend statistics** grouped by account and date.
- Stores the collected data in the `Ads_Reddit` dataset in **Google BigQuery**.
- Ensures smooth ingestion by handling missing or empty reports.

## 🛠️ Requirements

Before running the scripts, install the required dependencies:

```bash
pip install requests pandas google-cloud-bigquery
```

Additionally, ensure you have:
- **Google Cloud Service Account JSON Key** for BigQuery authentication.
- **Facebook Ads API Access Token**.
- **Reddit API Credentials** (Client ID, Client Secret, and Refresh Token).

## 🚀 Usage

### **Running Facebook Ads Data Pipeline**
Modify the script to include your **Facebook Ads API credentials**, then run:

```bash
python facebook.py
```

### **Running Reddit Ads Data Pipeline**
Ensure your **Reddit API credentials** are set up, then execute:

```bash
python reddit.py
```

## 📌 Notes
- Data is fetched **incrementally** to avoid duplicate records.
- Both scripts automatically **append new data** to BigQuery tables.
- Make sure to **update your API tokens** regularly to maintain access.

## 📜 License
This project is licensed under the MIT License.

