from google.cloud import bigquery

SERVICE_ACCOUNT_KEY = r'/Users/sreeram/Projects/GoogleCloud/bigquery-demo-385800-0deb753c8487.json'

client = bigquery.Client.from_service_account_json(SERVICE_ACCOUNT_KEY)

table_name = "bigquery-demo-385800.dataset_python.table_py"

config = bigquery.LoadJobConfig(
    schema = [
        bigquery.SchemaField("name", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("gender", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("count", "INTEGER", mode="NULLABLE")
    ],
    source_format = bigquery.SourceFormat.CSV, skip_leading_rows=1
)

file_path = r'/Users/sreeram/Projects/GoogleCloud/yob1880.txt'

open_file = open(file_path, "rb")

job = client.load_table_from_file(open_file, table_name, job_config=config)

job.result()

table = client.get_table(table_name)

print("loaded {} rows in {}".format(table.num_rows, table_name))
