import os

from google.cloud import bigquery

def load_csv_to_bq(data, context):
        client = bigquery.Client()
        
        dataset_ref = client.dataset(os.environ['DATASET'])
        job_config = bigquery.LoadJobConfig()
        job_config.write_disposition = 'WRITE_TRUNCATE'
        job_config.skip_leading_rows = 1
        job_config.source_format = bigquery.SourceFormat.CSV
        #CSV format

        # get the URI for uploaded CSV in GCS
        uri = 'gs://rube_goldberg_project/reddit_data/reddit_Sentiment.csv'

        # load the data into BQ
        load_job = client.load_table_from_uri(
                uri,
                dataset_ref.table(os.environ['TABLE']),
                job_config=job_config)

        load_job.result()  
        print('Cloud storage to BigQuery worked')

#env variable setup on cloud function
#Dataset and Table =bq dataset and bq table