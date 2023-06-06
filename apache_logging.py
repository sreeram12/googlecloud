import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.internal.clients import bigquery
import os
import logging
import gcsfs
import datetime

path_to_account = '/Users/sreeram/Projects/GoogleCloud/bigquery-demo-385800-0deb753c8487.json'

os.environ['GOOGLE_APPLICATION_CREDITIONALS'] = path_to_account

p_options= {
    'runner':'DirectRunner', # DirectRunner runs on local. DataflowRunner runs on Cloud
    'project':'bigquery-demo-385800',
    'region':'us-central1',
    'job_name':'data-flow-job-gcslog4',
    'temp_location':'gs://temp_bucket_randomtrees/temp',
    'staging_location':'gs://temp_bucket_randomtrees/stage',
    'save_main_session':True
}
pipeline_options = PipelineOptions(flags=None, **p_options)

pipeline = beam.Pipeline(options=pipeline_options)

class ConvertToJSON(beam.DoFn):
    def process(self, element):
        element = element.split(',')
        json_data = {
            "name": element[0],
            "gender": element[1],
            "count": element[2]
        }
        yield json_data
class FilterNonNull(beam.DoFn):
    def process(self, element):
        if element is not None:
            yield element

# 2 outputs
# sending each row into custom function and get output
# output at the end of pipeline
class CustomDoFn(beam.DoFn):
    def process(self, element,timestamp=beam.DoFn.TimestampParam):
        if element[0] is not None and element[1] is not None and element[2] is not None:
            yield beam.pvalue.TaggedOutput('inserted', element)
        else:
            yield beam.pvalue.TaggedOutput('rejected', element)

full_table = (
    pipeline
    | 'read table gcs' >> beam.io.ReadFromText('gs://demo_bucket_randomtrees/yob1880.csv', skip_header_lines=1)
)

log_table = full_table | beam.ParDo(CustomDoFn()).with_outputs(
        'inserted', 'rejected')
def print_type(element):
    print(type(element))
    return element
inserts_count = (log_table.inserted 
                 | 'count inserts' >> beam.combiners.Count.Globally()
                 | 'convert insert to int' >> beam.Map(lambda count: int(count))
)
rejects_count = (log_table.rejected 
                 | 'count rejects' >> beam.combiners.Count.Globally()
                 | 'convert reject to int' >> beam.Map(lambda count: int(count))
)

# insert into log table
# file name, inserts, rejects, total rows, timestamp
log_table_spec = 'bigquery-demo-385800.dataset_python.audit_table'
log_table_schema = 'filename:STRING,inserts:INTEGER,rejects:INTEGER,total:INTEGER,timestamp:TIMESTAMP'
row_data = {'filename': 'yob1880.csv', 'inserts': inserts_count, 'rejects': rejects_count, 'total': inserts_count+rejects_count, 'timestamp': datetime.datetime.now()}

row = pipeline | 'create row' >> beam.Create(row_data)
log_table_write = (row
  | 'write log table' >> beam.io.WriteToBigQuery(
     log_table_spec,
     schema=log_table_schema,
     create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
     write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
 ))

# write inserted rows into its own table
table_spec = 'bigquery-demo-385800.dataset_python.inserts_table_logging'
table_schema = 'name:STRING,gender:STRING,count:INTEGER'

write_to_bq = (log_table.inserted
  | 'filter non null' >> beam.ParDo(FilterNonNull())
  | 'convert to json' >> beam.ParDo(ConvertToJSON())
  | 'write full table' >> beam.io.WriteToBigQuery(
     table_spec,
     schema=table_schema,
     create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
 ))

source_bucket = 'demo_bucket_randomtrees'
source_file_path = 'yob1880.csv'
destination_bucket = 'demo_bucket_randomtrees'
destination_file_path = 'archive/yob1880.csv'

# move file to new location
var = gcsfs.GCSFileSystem()
# move the file from the source bucket to the destination bucket
with var.open(f"{source_bucket}/{source_file_path}", "rb") as source_file:
    with var.open(f"{destination_bucket}/{destination_file_path}", "wb") as destination_file:
        destination_file.write(source_file.read())

# Log the destination file path
logging.info(f"DESTINATION FILE PATH: {destination_bucket}/{destination_file_path}")

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  result = pipeline.run()
  result.wait_until_finish()
