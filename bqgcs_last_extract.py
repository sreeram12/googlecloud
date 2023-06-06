# reference: https://medium.com/analytics-vidhya/deploying-apache-beam-pipelines-on-google-dataflow-70e9e90624d9
# used the above reference when I got error of "datetime not found" and fixed it following the pipeline options outlined in it

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import datetime
import os
from pytz import timezone
import pytz

path_to_account = '/Users/sreeram/Projects/GoogleCloud/bigquery-demo-385800-0deb753c8487.json'

os.environ['GOOGLE_APPLICATION_CREDITIONALS'] = path_to_account

p_options= {
    'runner':'DataflowRunner', # DirectRunner runs on local. DataflowRunner runs on Cloud
    'project':'bigquery-demo-385800',
    'region':'us-central1',
    'job_name':'data-flow-job-lastextract3',
    'temp_location':'gs://temp_bucket_randomtrees/temp',
    'staging_location':'gs://temp_bucket_randomtrees/stage',
    'save_main_session':True
}
pipeline_options = PipelineOptions(flags=None, **p_options)

def filter_since_last_extract(element):
    # filter for the last extract date
    last_extract_date = datetime.datetime(year=2021, month=1, day=1, hour=1, minute=1, second=1, tzinfo=timezone('UTC'))
    element_date = element['created_date']
    return element_date >= last_extract_date

pipeline = beam.Pipeline(options=pipeline_options)

table_spec = 'bigquery-demo-385800.dataset_python.service_request_data'
# read from bq table
# can use a direct sub_query
full_table = (
    pipeline
    | 'ReadTable' >> beam.io.ReadFromBigQuery(table=table_spec)
)

# Filter the data since the last extract
filtered_data = full_table | 'Filter since last extract' >> beam.Filter(filter_since_last_extract)

# Write the filtered data to GCS
filtered_data | 'Write to GCS' >> beam.io.WriteToText(
    f'gs://temp_bucket_randomtrees/extracted_data_{datetime.datetime.now()}',
    file_name_suffix='.csv',
    header='unique_key, complaint, source, status, status_change_date, created_date, last_updat_date, close_date, address, street_num, street_name, city, zip, county, x_coords, y_coords, lat, long, location,  council_district, map_page, map_tile'
)

pipeline.run()
