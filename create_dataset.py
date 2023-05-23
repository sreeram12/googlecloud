from google.cloud import bigquery

SERVICE_ACCOUNT_KEY = r'/Users/sreeram/Projects/GoogleCloud/bigquery-demo-385800-0deb753c8487.json'

# create a bigquery client
client = bigquery.Client.from_service_account_json(SERVICE_ACCOUNT_KEY)

dataset_name = "bigquery-demo-385800.dataset_python"

dataset = bigquery.Dataset(dataset_name)

dataset.location = 'US'
# all other attributes can also be set as needed

dataset_ref = client.create_dataset(dataset, timeout = 20)

print("created dataset {}.{}".format(client.project, dataset_ref.dataset_id))
