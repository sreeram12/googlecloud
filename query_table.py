from google.cloud import bigquery

SERVICE_ACCOUNT_KEY = r'/Users/sreeram/Projects/GoogleCloud/bigquery-demo-385800-0deb753c8487.json'

client = bigquery.Client.from_service_account_json(SERVICE_ACCOUNT_KEY)

query = "select * from bigquery-demo-385800.dataset_python.table_py limit 10"

query_job = client.query(query)

print(query_job)
for row in query_job:
    print(row)
print("done with query")
