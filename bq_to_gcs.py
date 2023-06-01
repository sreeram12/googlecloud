import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import os

path_to_account = '/Users/sreeram/Projects/GoogleCloud/bigquery-demo-385800-0deb753c8487.json'

os.environ['GOOGLE_APPLICATION_CREDITIONALS'] = path_to_account

pipeline_options = PipelineOptions(
    flags=None,
    runner='DataflowRunner', # DirectRunner runs on local. DataflowRunner runs on Cloud
    project='bigquery-demo-385800',
    region='us-central1',
    job_name='data-flow-job-bqgcs2',
    temp_location='gs://temp_bucket_randomtrees/temp',
    staging_location='gs://temp_bucket_randomtrees/stage'
)

pipeline = beam.Pipeline(options=pipeline_options)

table_spec = 'bigquery-demo-385800.dataset_python.table_py'

# read from bq table
full_table = (
    pipeline
    | 'ReadTable' >> beam.io.ReadFromBigQuery(table=table_spec)
)

# Write to GCS
full_table | 'Write to GCS' >> beam.io.WriteToText(
        'gs://temp_bucket_randomtrees/frombq',
        file_name_suffix='.csv',
        header='name,gender,count'
    )

pipeline.run()
