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
    job_name='data-flow-job-bqagg5',
    temp_location='gs://temp_bucket_randomtrees/temp',
    staging_location='gs://temp_bucket_randomtrees/stage'
)

pipeline = beam.Pipeline(options=pipeline_options)

table_spec = 'bigquery-demo-385800.dataset_python.table_py'

full_table = (
    pipeline
    | 'ReadTable' >> beam.io.ReadFromBigQuery(table=table_spec)
)

class ConvertTupleToJSON(beam.DoFn):
    def process(self, element):
        json_data = {
            "gender": element[0],
            "total_count": element[1]
        }
        yield json_data

aggregated_table = (
    full_table
    | "ExtractGenderCount" >> beam.Map(lambda row: (row["gender"], row["count"]))
    | "GroupByGender" >> beam.GroupByKey()
    | "AggregateCount" >> beam.Map(lambda gender_count: (gender_count[0], sum(gender_count[1])))
    | 'convert to JSON' >> beam.ParDo(ConvertTupleToJSON()) # if facing JSON not readable issue then turn the data into a dict manually
)

copy_table_spec = 'bigquery-demo-385800.dataset_python.copied_table'
aggregated_table_spec = 'bigquery-demo-385800.dataset_python.aggregated_table'

schema_table = 'name:STRING,gender:STRING,count:INTEGER'
schema_aggregation = 'gender:STRING,total_count:INTEGER'

(full_table
 | 'write full table' >> beam.io.WriteToBigQuery(
     copy_table_spec,
     schema=schema_table,
     create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
 ))

(aggregated_table
 | 'write aggregated table' >> beam.io.WriteToBigQuery(
     aggregated_table_spec,
     schema=schema_aggregation,
     create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
 ))

pipeline.run()
