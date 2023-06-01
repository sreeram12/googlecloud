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
    'runner':'DirectRunner', # DirectRunner runs on local. DataflowRunner runs on Cloud
    'project':'bigquery-demo-385800',
    'region':'us-central1',
    'job_name':'data-flow-job-lastextract2-4',
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
full_table = (
    pipeline
    | 'ReadTable' >> beam.io.ReadFromBigQuery(table=table_spec)
)

# Filter the data since the last extract
filtered_data = full_table | 'Filter since last extract' >> beam.Filter(filter_since_last_extract)

# Write the filtered data to GCS
filtered_data | 'Write to GCS' >> beam.io.WriteToText(
    'gs://temp_bucket_randomtrees/extracted_data.csv',
    file_name_suffix='.csv',
    header='unique_key, complaint, source, status, status_change_date, created_date, last_updat_date, close_date, address, street_num, street_name, city, zip, county, x_coords, y_coords, lat, long, location,  council_district, map_page, map_tile'
)

# get the closest date to the current date in the filtered data
class ClosestTimestampFn(beam.CombineFn):
    def create_accumulator(self):
        return None

    def add_input(self, accumulator, input):
        current_timestamp = input
        if accumulator is None or abs(current_timestamp - datetime.datetime.now(pytz.utc)) < abs(accumulator - datetime.datetime.now(pytz.utc)):
            return current_timestamp
        else:
            return accumulator

    def merge_accumulators(self, accumulators):
        return min(accumulators, key=lambda x: abs(x - datetime.datetime.now(pytz.utc)))

    def extract_output(self, accumulator):
        return accumulator.strftime("%Y%m%d_%H%M%S")

closest_timestamp = (
  filtered_data
  | 'Extract Timestamp' >> beam.Map(lambda element: element['created_date'])
  | 'Find Closest Timestamp' >> beam.CombineGlobally(ClosestTimestampFn())
)
def convert_to_string(element):
    return element.strftime("%Y%m%d_%H%M%S")

closest_timestamp_str = closest_timestamp | 'Convert to String' >> beam.Map(convert_to_string)

# write filtered data to a new BQ table and save the date of the previous extract
filtered_table_spec = 'bigquery-demo-385800.dataset_python.service_request_data_extract'
filtered_schema = 'unique_key:STRING, complaint:STRING, source:STRING, status:STRING, status_change_date:TIMESTAMP, created_date:TIMESTAMP, last_update_date:TIMESTAMP, close_date:TIMESTAMP, address:STRING, street_num:STRING, street_name:STRING, city:STRING, zip:INTEGER, county:STRING, x_coords:STRING, y_coords:FLOAT, lat:FLOAT, long:FLOAT,location:STRING, council_district:INTEGER, map_page:STRING, map_tile:STRING'

# class ConvertToJSON(beam.DoFn):
#     def process(self, element):
#         json_data = {
#             "unique_key": element[0],
#             "complaint": element[1],
#             "source": element[2],
#             "status": element[3],
#             "status_change_date": element[4],
#             "created_date": element[5],
#             "last_update_date": element[6],
#             "close_date": element[7],
#             "address": element[8],
#             "street_num": element[9],
#             "street_name": element[10],
#             "city": element[11],
#             "zip": element[12],
#             "county": element[13],
#             "x_coords": element[14],
#             "y_coords": element[15],
#             "lat": element[16],
#             "long": element[17],
#             "location": element[18],
#             "council_district": element[19],
#             "map_page": element[20],
#             "map_tile": element[21]
#         }
#         yield json_data

(filtered_data
    | 'Write to BigQuery' >> beam.io.WriteToBigQuery(
        filtered_table_spec,
        schema=filtered_schema,
        write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE
    )
)

pipeline.run()
