import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io import WriteToText, ReadFromText
from datetime import datetime, timedelta
import os

path_to_account = '/Users/sreeram/Projects/GoogleCloud/bigquery-demo-385800-0deb753c8487.json'

os.environ['GOOGLE_APPLICATION_CREDITIONALS'] = path_to_account

pipeline_options = PipelineOptions(
    flags=None,
    runner='DirectRunner', # DirectRunner runs on local. DataflowRunner runs on Cloud
    project='bigquery-demo-385800',
    region='us-central1',
    job_name='data-flow-job-lastextract',
    temp_location='gs://temp_bucket_randomtrees/temp',
    staging_location='gs://temp_bucket_randomtrees/stage'
)

# filter for the last extract date
last_extract_date = datetime.now() - timedelta(days=1)

def filter_since_last_extract(element):
    element_date = datetime.strptime(element['timestamp'], "%Y-%m-%d %H:%M:%S")
    return element_date >= last_extract_date

pipeline = beam.Pipeline(options=pipeline_options)

table_spec = 'bigquery-demo-385800.dataset_python.table_py'
# read from bq table
full_table = (
    pipeline
    | 'ReadTable' >> beam.io.ReadFromBigQuery(table=table_spec)
)

# Filter the data since the last extract
filtered_data = full_table | 'Filter since last extract' >> beam.Filter(filter_since_last_extract)

# Write to GCS
full_table | 'Write to GCS' >> beam.io.WriteToText(
        'gs://temp_bucket_randomtrees/frombq.csv',
        file_name_suffix='.csv',
        header='name,gender,count'
    )

pipeline.run()
