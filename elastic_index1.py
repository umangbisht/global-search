# check whther particular index is available/exists in elastic search or not

from elasticsearch import Elasticsearch
import json

# Elasticsearch configuration
es = Elasticsearch([{"host": "localhost", "port": 9200, "scheme": "http"}])

#search particular index is available or not
# index = "ernet_index"
# try:
#     response = es.search(index=index)
#     print(response)
# except Exception as e:
#     print(str(e))


#search index based on pattern (using regular expression)
# index = "sms*"
# try:
#     response = es.search(index=index)
#     print(response['_shards']['total'])              # 2 will print as we have 2 index starting with 'sms'
# except Exception as e:
#     print(str(e))

# delete index based on pattern

index_pattern = "debezium.json*"
response = es.indices.get_alias(index=index_pattern)
if len(response) > 0:
    for index in response:
        delete_response = es.indices.delete(index=index)
        print(delete_response)
else:
    print("no index has been  found for the given search pattern.")

