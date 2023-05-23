import apache_beam as beam
import argparse
from apache_beam.options.pipeline_options import PipelineOptions
import os

# path_to_account = '/Users/sreeram/Projects/GoogleCloud/bigquery-demo-385800-0deb753c8487.json'

# os.environ['GOOGLE_APPLICATION_CREDITIONALS'] = path_to_account

pipeline_options = PipelineOptions(
    flags=None,
    runner='DataflowRunner',
    project='bigquery-demo-385800',
    region='us-central1',
    job_name='data-flow-job-bqreadandwrite',
    temp_location='gs://temp_bucket_randomtrees/temp',
    staging_location='gs://temp_bucket_randomtrees/stage'
)

pipeline = beam.Pipeline(options=pipeline_options)

table_spec = 'bigquery-demo-385800.dataset_python.table_py'

full_table = (
    pipeline
    | 'ReadTable' >> beam.io.ReadFromBigQuery(table=table_spec))

copy_table_spec = 'bigquery-demo-385800.dataset_python.copied_table'

schema_table = 'name:STRING,gender:STRING,count:INTEGER'

(full_table
 | 'write to json' >> beam.io.WriteToBigQuery(
     copy_table_spec,
     schema=schema_table,
     create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
 ))

from apache_beam.runners.runner import PipelineState
ret = pipeline.run()
if ret.state == PipelineState.DONE:
    print('Success!!!')
else:
    print('Error Running beam pipeline')
