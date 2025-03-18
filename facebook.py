def run_pipeline(request):
  import requests
  import json
  import time 
  import pandas as pd
  from google.cloud import bigquery
  from datetime import date, timedelta,datetime
  import urllib.parse

  import sys

  client = bigquery.Client.from_service_account_json(json_credentials_path='')

  YOUR_PROJECT = ''
  YOUR_DATASET = ''
  YOUR_TABLE_NAME = ''
  table_id = f"{YOUR_PROJECT}.{YOUR_DATASET}.{YOUR_TABLE_NAME}"
  ts = str(time.time())

  api_version = 'v20.0'

  token = ''

  #fields = 'account_name,account_id,campaign_name,campaign_id,adset_name,adset_id,ad_name,ad_id,impressions,reach,clicks,spend,purchase_roas,date_start'
  #fields = 'account_currency,account_id,account_name,action_values,actions,ad_id,ad_name,adset_id,adset_name,attribution_setting,buying_type,campaign_id,campaign_name,canvas_avg_view_percent,canvas_avg_view_time,catalog_segment_value,clicks,conversion_rate_ranking,conversion_values,conversions,converted_product_quantity,converted_product_value,cost_per_action_type,cost_per_conversion,cost_per_estimated_ad_recallers,cost_per_inline_link_click,cost_per_inline_post_engagement,cost_per_outbound_click,cost_per_thruplay,cost_per_unique_action_type,cost_per_unique_click,cost_per_unique_inline_link_click,cost_per_unique_outbound_click,cpc,cpm,cpp,ctr,date_start,date_stop,dda_results,engagement_rate_ranking,estimated_ad_recall_rate,estimated_ad_recallers,frequency,full_view_impressions,full_view_reach,impressions,inline_link_click_ctr,inline_link_clicks,inline_post_engagement,instant_experience_clicks_to_open,instant_experience_clicks_to_start,instant_experience_outbound_clicks,mobile_app_purchase_roas,objective,optimization_goal,outbound_clicks,outbound_clicks_ctr,place_page_name,purchase_roas,qualifying_question_qualify_answer_rate,quality_ranking,reach,social_spend,spend,video_30_sec_watched_actions,video_avg_time_watched_actions,video_p100_watched_actions,video_p25_watched_actions,video_p50_watched_actions,video_p75_watched_actions,video_p95_watched_actions,video_play_actions,video_play_curve_actions,website_ctr,website_purchase_roas'
  fields = 'account_name,action_values,actions,ad_id,ad_name,adset_id,adset_name,campaign_id,campaign_name,cpm,clicks,impressions,reach,spend,outbound_clicks_ctr,outbound_clicks,website_purchase_roas,date_start'

  filtering = "%5B%7Bfield%3A%20%22ad.effective_status%22%2Coperator%3A%22IN%22%2C%20value%3A%20%5B'ACTIVE'%2C%20'PAUSED'%2C%20'DELETED'%2C%20'PENDING_REVIEW'%2C%20'DISAPPROVED'%2C%20'PREAPPROVED'%2C%20'PENDING_BILLING_INFO'%2C%20'CAMPAIGN_PAUSED'%2C%20'ARCHIVED'%2C%20'ADSET_PAUSED'%2C%20'IN_PROCESS'%2C%20'WITH_ISSUES'%5D%7D%5D"
  #Non-url-encoded format example: filtering=[{field: "ad.effective_status",operator:"IN", value: ['ACTIVE', 'PAUSED', 'DELETED', 'ARCHIVED']}]
  #All statuses: ACTIVE, PAUSED, DELETED, PENDING_REVIEW, DISAPPROVED, PREAPPROVED, PENDING_BILLING_INFO, CAMPAIGN_PAUSED, ARCHIVED, ADSET_PAUSED, IN_PROCESS, WITH_ISSUES

  #date_preset = '&date_preset=last_30d'
  #date_preset = '&date_preset=yesterday'
  #time_range = '&time_range[since]=2022-05-01&time_range[until]=2022-07-24'


  #sort = '&sort=date_start_descending'
  sort = ''


  #account_id_ca = ''
  #account_id_us = ''
  account_id = 'act_'

  def process(length,count,end_date=date.today()):

    for i in range(count):
      since = (end_date - timedelta(days=(i+1)*length)).strftime('%Y-%m-%d')
      until = (end_date  - timedelta(days=1+i*length)).strftime('%Y-%m-%d')
      print(f"Iteration {i}: Since {since} Until {until}")


      date_preset = ''

      time_range = f'&time_range[since]={since}&time_range[until]={until}'  

      attribution_window = urllib.parse.quote('["1d_view","7d_click"]')
      attribution = f'&action_attribution_windows={attribution_window}'

      #url_us = f"https://graph.facebook.com/{api_version}/{account_id_us}/insights?level=ad{date_preset}{time_range}{attribution}&fields={fields}&time_increment=1{sort}&filtering={filtering}&limit=8000&access_token={token}"
      #url_ca = f"https://graph.facebook.com/{api_version}/{account_id_ca}/insights?level=ad{date_preset}{time_range}{attribution}&fields={fields}&time_increment=1{sort}&filtering={filtering}&limit=8000&access_token={token}"
      #url_us = "https://graph.facebook.com/v14.0/act_30695751/insights?level=ad&date_preset=last_30d&fields=account_currency,account_id,account_name,action_values,actions,ad_id,ad_name,adset_id,adset_name,attribution_setting,buying_type,campaign_id,campaign_name,canvas_avg_view_percent,canvas_avg_view_time,catalog_segment_value,clicks,conversion_rate_ranking,conversion_values,conversions,converted_product_quantity,converted_product_value,cost_per_action_type,cost_per_conversion,cost_per_estimated_ad_recallers,cost_per_inline_link_click,cost_per_inline_post_engagement,cost_per_outbound_click,cost_per_thruplay,cost_per_unique_action_type,cost_per_unique_click,cost_per_unique_inline_link_click,cost_per_unique_outbound_click,cpc,cpm,cpp,ctr,date_start,date_stop,dda_results,engagement_rate_ranking,estimated_ad_recall_rate,estimated_ad_recallers,frequency,full_view_impressions,full_view_reach,impressions,inline_link_click_ctr,inline_link_clicks,inline_post_engagement,instant_experience_clicks_to_open,instant_experience_clicks_to_start,instant_experience_outbound_clicks,mobile_app_purchase_roas,objective,optimization_goal,outbound_clicks,outbound_clicks_ctr,place_page_name,purchase_roas,qualifying_question_qualify_answer_rate,quality_ranking,reach,social_spend,spend,video_30_sec_watched_actions,video_avg_time_watched_actions,video_p100_watched_actions,video_p25_watched_actions,video_p50_watched_actions,video_p75_watched_actions,video_p95_watched_actions,video_play_actions,video_play_curve_actions,website_ctr,website_purchase_roas&time_increment=1&sort=date_start_descending&filtering=%5B%7Bfield%3A%20%22ad.effective_status%22%2Coperator%3A%22IN%22%2C%20value%3A%20%5B'ACTIVE'%2C%20'PAUSED'%2C%20'DELETED'%2C%20'ARCHIVED'%5D%7D%5D&limit=1000000&access_token="
      url = f"https://graph.facebook.com/{api_version}/{account_id}/insights?level=ad{date_preset}{time_range}{attribution}&fields={fields}&time_increment=1{sort}&filtering={filtering}&limit=8000&access_token={token}"
      
      def call_and_upload(request_url):
        response = requests.request("POST", request_url)
        print(f"First response: {response.text}")
        report_run_id = json.loads(response.text)["report_run_id"]

        request_url = f"https://graph.facebook.com/{api_version}/{report_run_id}?access_token={token}"
        response = requests.request("GET", request_url)

        while(json.loads(response.text)['async_percent_completion'] != 100):
          response = requests.request("GET", request_url)
          print(f"Async Request {json.loads(response.text)['async_percent_completion']}: {response.text}")
          if json.loads(response.text)['async_status'] == "Job Failed":
            raise Exception("Job Failed")
          time.sleep(7)
        
        request_url = f"https://graph.facebook.com/{api_version}/{report_run_id}/insights?limit=800&access_token={token}"
        response = requests.request("GET", request_url)
        
        
        while not "data" in json.loads(response.text):
          print(f"Upload: {json.loads(response.text)}")
          if '(#80000)' in json.loads(response.text)['error']['message']:
              return 'Error'
          response = requests.request("GET", request_url)
          time.sleep(5)

        data = json.loads(response.text)['data']

        df = pd.json_normalize(data)
        df['ingest_timestamp'] = ts 

        job_config = bigquery.LoadJobConfig(
          write_disposition="WRITE_APPEND"
        )
        job = client.load_table_from_dataframe(
          df, table_id, job_config=job_config
        )
        job.result()

        page = 2
        while 'next' in json.loads(response.text)['paging']:
          next = json.loads(response.text)['paging']['next']
          response = requests.request("GET", next)
          print(f"GET page {page}: {next}")
          page += 1
          while not "data" in json.loads(response.text):
            print(f"Upload: {json.loads(response.text)}")
            if '(#80000)' in json.loads(response.text)['error']['message']:
              return 'Error'
            response = requests.request("GET", request_url)
            time.sleep(3)
          data = json.loads(response.text)['data']
          df = pd.json_normalize(data)
          df['ingest_timestamp'] = ts 
          job_config = bigquery.LoadJobConfig(
              write_disposition="WRITE_APPEND"
          )
          job = client.load_table_from_dataframe(
              df, table_id, job_config=job_config
          )
          job.result()
        
        dml_statement = (f"DELETE FROM `{YOUR_PROJECT}.{YOUR_DATASET}.{YOUR_TABLE_NAME}` WHERE DATE(date_start) = CURRENT_DATE();")
        query_job = client.query(dml_statement) 
        query_job.result() 
      
        return ''
  
#      res = call_and_upload(url_us)
#      if res == 'Error':
#        return ''
#      res = call_and_upload(url_ca)
#      if res == 'Error':
#        return ''
      
      res = call_and_upload(url)
      if res == 'Error':
        return ''

  #until_day = datetime.strptime('2022-07-19', '%Y-%m-%d').date()
  #process(length=5,count=32,end_date=until_day)
  process(length=2,count=1)


  print("Done.")
  
  return f'OK'