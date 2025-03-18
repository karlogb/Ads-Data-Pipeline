def run_pipeline(request):

    import requests
    import datetime
    import pandas as pd
    from google.cloud import bigquery
    import time


    headers = {
        'User-Agent': '',
        'Content-Type': 'application/x-www-form-urlencoded',
    }

    data = {
        'grant_type': 'refresh_token',
        'refresh_token': '',
    }

    response = requests.post(
        'https://www.reddit.com/api/v1/access_token',
        headers=headers,
        data=data,
        auth=('', ''),
    )

    PROJECT = ''
    DATASET = 'Ads_Reddit'
    TABLE_NAME = 'daily_account_stats_raw'
    table_id = f"{PROJECT}.{DATASET}.{TABLE_NAME}"


    access_token = response.json()["access_token"]
    user_agent = 'OluKai_Data_Transfer'
    start_date = datetime.date(2023, 4, 1)
    end_date = datetime.datetime.now().strftime("%Y-%m-%d")
    print(f'Start date: {start_date}')
    print(f'End date: {end_date}')

    report_url = f"https://ads-api.reddit.com/api/v2.0/accounts/t2_XXX/reports" 
    report_params = {"start_date": start_date, "end_date": end_date,"group_by":"account_id,date"}   
    report_headers = {"User-Agent": user_agent, "Authorization": f"bearer {access_token}"}
    report_response = requests.get(report_url, headers=report_headers, params=report_params)

    data = report_response.json()
    if 'data' not in data.keys() or not data["data"]:
        print('The report is empty')
        return 'OK'
    dataframe = pd.json_normalize(data["data"])[['date','spend']]
    ts = str(time.time())
    dataframe['ingest_timestamp'] = ts 

    client = bigquery.Client.from_service_account_json(json_credentials_path='./olukai-keys.json')

    job_config = bigquery.LoadJobConfig(
    #schema=[bigquery.SchemaField(column_name, "STRING") for column_name in columns],
    write_disposition="WRITE_APPEND"
    )
    job = client.load_table_from_dataframe(
    dataframe, table_id, job_config=job_config
    )
    job.result()
    return 'OK'
