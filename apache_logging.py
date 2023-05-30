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
    job_name='data-flow-job-gcslogging',
    temp_location='gs://temp_bucket_randomtrees/temp',
    staging_location='gs://temp_bucket_randomtrees/temp'
)

pipeline = beam.Pipeline(options=pipeline_options)

table_spec = 'bigquery-demo-385800.dataset_python.table_py'

full_table = (
    pipeline
    | 'read table gcs' >> beam.io.ReadFromText('gs://demo_bucket_randomtrees/yob1880.csv', skip_header_lines=1)
)

table_spec = 'bigquery-demo-385800.dataset_python.copied_table'
schema_table = 'name:STRING,gender:STRING,count:INTEGER'

(full_table
 | 'write full table' >> beam.io.WriteToBigQuery(
     table_spec,
     schema=schema_table,
     create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
 ))


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  pipeline.run()
