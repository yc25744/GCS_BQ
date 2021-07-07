import logging
import os
import re

from google.cloud import bigquery

GCP_PROJECT = os.environ.get('GCP_PROJECT')

# create bigquery client
client = bigquery.Client()

def bigqueryImport(data, context):
    # get storage update data
    bucketname = data['bucket']
    filename = data['name']
    timeCreated = data['timeCreated']

    # check filename format - dataset_name/table_name.json
    if not re.search('^[a-zA-Z_-]+/[a-zA-Z_-]+.json$', filename):
        logging.error('Wrong filename format: %s' % (filename))
        return

    # parse filename
    datasetname, tablename = filename.replace('.json', '').split('/')
    table_id = client.dataset(datasetname).table(tablename)

    # GET URI FROM DATA INPUT
    uri = 'gs://%s/%s' % (bucketname, filename)

    # get dataset reference
    dataset_ref = client.dataset(datasetname)

    # check if dataset exists, otherwise create
    try:
        client.get_dataset(dataset_ref)
    except Exception:
        logging.warn('Creating dataset: %s' % (datasetname))
        client.create_dataset(dataset_ref)

    # create a bigquery load job config
    job_config = bigquery.LoadJobConfig()
    job_config.autodetect = True
    job_config.create_disposition = 'CREATE_IF_NEEDED',
    job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
    job_config.write_disposition = 'WRITE_TRUNCATE',

    # create a bigquery load job
    load_job = client.load_table_from_uri(
        uri,
        table_id,
        job_config=job_config,
    )
    load_job.result()

# need to set up the variables inside of Cloud Function
#'project' = 'name of GCP project' 