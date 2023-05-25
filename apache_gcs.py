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
    job_name='data-flow-job-gcstobq',
    temp_location='gs://temp_bucket_randomtrees/temp',
    staging_location='gs://temp_bucket_randomtrees/temp'
)

p = beam.Pipeline(options = pipeline_options)

def remove_last_colon(row):		# OXJY167254JK,11-09-2020,8:11:21,854A854,Chow M?ein:,65,Cash,Sadabahar,Delivered,5,Awesome experience
    cols = row.split(',')		# [(OXJY167254JK) (11-11-2020) (8:11:21) (854A854) (Chow M?ein:) (65) (Cash) ....]
    item = str(cols[4])			# item = Chow M?ein:
    
    if item.endswith(':'):
        cols[4] = item[:-1]		# cols[4] = Chow M?ein

    return ','.join(cols)		# OXJY167254JK,11-11-2020,8:11:21,854A854,Chow M?ein,65,Cash,Sadabahar,Delivered,5,Awesome experience
	
def remove_special_characters(row):    # oxjy167254jk,11-11-2020,8:11:21,854a854,chow m?ein,65,cash,sadabahar,delivered,5,awesome experience
    import re
    cols = row.split(',')			# [(oxjy167254jk) (11-11-2020) (8:11:21) (854a854) (chow m?ein) (65) (cash) ....]
    ret = ''
    for col in cols:
        clean_col = re.sub(r'[?%&]','', col)
        ret = ret + clean_col + ','			# oxjy167254jk,11-11-2020,8:11:21,854a854,chow mein:,65,cash,sadabahar,delivered,5,awesome experience,
    ret = ret[:-1]						# oxjy167254jk,11-11-2020,8:11:21,854A854,chow mein:,65,cash,sadabahar,delivered,5,awesome experience
    return ret

def print_row(row):
    print (row)


cleaned_data = (
	p
	| beam.io.ReadFromText('gs://demo_bucket_randomtrees/food_daily.csv', skip_header_lines=1)
	| beam.Map(remove_last_colon)
	| beam.Map(lambda row: row.lower())
	| beam.Map(remove_special_characters)
	| beam.Map(lambda row: row+',1')		# oxjy167254jk,11-11-2020,8:11:21,854a854,chow mein,65,cash,sadabahar,delivered,5,awesome experience,1
)

delivered_orders = (
	cleaned_data
	| 'delivered filter' >> beam.Filter(lambda row: row.split(',')[8].lower() == 'delivered')
)

other_orders = (
    cleaned_data
    | 'Undelivered Filter' >> beam.Filter(lambda row: row.split(',')[8].lower() != 'delivered')
)

(cleaned_data
 | 'count total' >> beam.combiners.Count.Globally() 		# 920
 | 'total map' >> beam.Map(lambda x: 'Total Count:' +str(x))	# Total Count: 920
 | 'print total' >> beam.Map(print_row)
)

(delivered_orders
 | 'count delivered' >> beam.combiners.Count.Globally()
 | 'delivered map' >> beam.Map(lambda x: 'Delivered count:'+str(x))
 | 'print delivered count' >> beam.Map(print_row)
 )


(other_orders
 | 'count others' >> beam.combiners.Count.Globally()
 | 'other map' >> beam.Map(lambda x: 'Others count:'+str(x))
 | 'print undelivered' >> beam.Map(print_row)
 )

	
def to_json(csv_str):
    fields = csv_str.split(',')
    
    json_str = {"customer_id":fields[0],
                 "date": fields[1],
                 "timestamp": fields[2],
                 "order_id": fields[3],
                 "items": fields[4],
                 "amount": fields[5],
                 "mode": fields[6],
                 "restaurant": fields[7],
                 "status": fields[8],
                 "ratings": fields[9],
                 "feedback": fields[10],
                 "new_col": fields[11]
                 }

    return json_str

table_schema = 'customer_id:STRING,date:STRING,timestamp:STRING,order_id:STRING,items:STRING,amount:STRING,mode:STRING,restaurant:STRING,status:STRING,ratings:STRING,feedback:STRING,new_col:STRING'

#project-id:dataset_id.table_id
delivered_table_spec = 'bigquery-demo-385800.dataset_python.delivered_orders'
#project-id:dataset_id.table_id
other_table_spec = 'bigquery-demo-385800.dataset_python.other_status_orders'

(delivered_orders
	| 'delivered to json' >> beam.Map(to_json)
	| 'write delivered' >> beam.io.WriteToBigQuery(
	delivered_table_spec,
	schema=table_schema,
	create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
	write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
	)
)

(other_orders
	| 'others to json' >> beam.Map(to_json)
	| 'write other_orders' >> beam.io.WriteToBigQuery(
	other_table_spec,
	schema=table_schema,
	create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
	write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
	)
)

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  p.run()
