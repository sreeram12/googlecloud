import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import os
import logging

path_to_account = '/Users/sreeram/Projects/GoogleCloud/bigquery-demo-385800-0deb753c8487.json'

os.environ['GOOGLE_APPLICATION_CREDITIONALS'] = path_to_account

pipeline_options = PipelineOptions(
    flags=None,
    runner='DirectRunner',
    project='bigquery-demo-385800',
    region='us-central1',
    job_name='data-flow-job-gcslog',
    temp_location='gs://temp_bucket_randomtrees/temp',
    staging_location='gs://temp_bucket_randomtrees/temp'
)

source_bucket = 'demo_bucket_randomtrees'
source_file_path = 'yob1880.csv'
destination_bucket = 'temp_bucket_randomtrees'
destination_file_path = 'yob1880_copy.csv'

pipeline = beam.Pipeline(options=pipeline_options)

row_count = 0

full_table = (
    pipeline
    | 'read table gcs' >> beam.io.ReadFromText(f'gs://{source_bucket}/{source_file_path}', skip_header_lines = 1)
)

table_spec = 'bigquery-demo-385800.dataset_python.copied_table'
schema_table = 'name:STRING,gender:STRING,count:INTEGER'

write_to_bq = (full_table
 | 'write full table' >> beam.io.WriteToBigQuery(
     table_spec,
     schema=schema_table,
     create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
 ))

class LogInsertCountFn(beam.DoFn):
    def process(self, element, insert_count=beam.DoFn.SideInputParam):
        insert_count_value = len(element)
        logging.info(f"Number of Rows Inserted: {insert_count_value}")
        yield element
# Count the number of rows inserted per key
key_counts = (
    write_to_bq
    | 'Assign Key' >> beam.Map(lambda row: ('row_count_key', 1))
    | 'Count Rows Per Key' >> beam.CombinePerKey(sum)
)
# Sum up the counts across all keys
insert_count = (
    key_counts
    | 'Sum Counts Globally' >> beam.combiners.Count.Globally()
)
# log number of rows inserted
insert_count | 'Log Insert Count' >> beam.ParDo(LogInsertCountFn(), insert_count=beam.pvalue.AsDict(key_counts))


# move file to new location
(full_table
 | 'move file to new location' >> beam.io.WriteToText(f'gs://{source_bucket}/{source_file_path}')
)

# Log the destination file path
full_table | 'Log Destination File Path' >> beam.Map(
    lambda element: logging.info(f"Destination File Path: {os.path.join(destination_file_path, os.path.basename(source_file_path))}")
)

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  result = pipeline.run()
  result.wait_until_finish()
