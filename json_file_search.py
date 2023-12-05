from elasticsearch import Elasticsearch, helpers
import json
from flask import Flask, jsonify, request
from flask_cors import CORS
import requests
# from elasticsearch.helpers import bulk

app = Flask(__name__)
CORS(app)

# Elasticsearch configuration
es = Elasticsearch([{"host": "localhost", "port": 9200, "scheme": "http"}], verify_certs=False, http_auth=None)




@app.route('/search-file', methods=['GET'])
def get_search_data():
    # Get query parameters from the request
    search_query = request.args.get('data')
    # search_query_2 = request.args.get('must_not')
    print("search_query", search_query)

    # Get all indices
    all_indices = es.indices.get_alias(index="*").keys()

    # Convert the indices to a list
    index_list = list(all_indices)
    print("indices", index_list)


    # Generate the search query
    query = {
        "_source": [],
        "min_score": 0.5,
        "size": 10000,
        "query": {
            "bool": {
                "should": [
                    {
                        "query_string": {
                        "query": f"*{search_query}* OR {search_query}*",
                        "fields": ["*"]
                    }
                        # "multi_match": {
                        #     "query": search_query,
                        #     "type": "phrase",
                        
                        # }
                    },
                  
                ],
                
            },
            "bool": {
                "should": [
                    {
                    #     "query_string": {
                    #     "query": f"*{search_query}* OR {search_query}*",
                    #     "fields": ["*"]
                    # }
                        "multi_match": {
                            "query": search_query,
                            "type": "phrase",
                        
                        }
                    },
                  
                ],
                
            }
        }
    }

 

    # Execute the search
    response = es.search(index=index_list, body=query)

    # Process the search results
    hits = response['hits']['hits']


    if hits:
        # Extract sources from all hits
        result_list = [{"_source": hit['_source'], "_index": hit['_index']} for hit in hits]
        return jsonify(result_list)
    else:
        # Return an empty list if no hits
        return jsonify([])
    
@app.route('/indexing-file', methods=['POST'])
def make_file_index():
    FILE_TO_CONVERT = ['csv','txt']
    # Check if the post request has the file part
    if 'file' not in request.files:
        return jsonify({'error': 'No file part'})

    file = request.files['file']
    filename = file.filename.lower()
    filename = filename.replace(" ", "")
    # Split from the last dot
    split_result = filename.rsplit('.', 1)

    print("split_result", split_result)

    # Check if the file is not empty
    if filename == '':
        return jsonify({'error': 'No selected file'})
    


    # url = f"http://localhost:9200/{filename}"
    # try:
    #     response = requests.head(url, timeout=1)
    #     if response.status_code == 200:
    #         print("Index exists")
    #         response = {"message": f"Index '{filename}' already exists."}
    #         return jsonify([response])
    #     elif response.status_code == 404:
    #         print("Index does not exist")
    #         response = {"message": f"Index '{filename}' already exists."}
    #         return jsonify([response])
    #     else:
    #         print(f"Unexpected status code: {response.status_code}")
    #         response = {"message": f"Unexpected status code: {response.status_code}"}
    #         return jsonify([response])
    # except requests.RequestException as e:
    #     print(f"Request error: {e}")
    #     response = {"message": f"Request error: {e}"}
    #     return jsonify([response])
    if es.indices.exists(index=filename):
        print("uuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuu")
        response = {"message": f"Index '{filename}' already exists."}
        return jsonify([response])
    

    # Save the uploaded file to a designated folder
    file.save('./datastore/' + filename)
    print('./datastore/' + filename)
    print("file successfully saved")
    json_file_path = './datastore/'+filename
    # Check if the index already filename
    
    # Indexing JSON data
    print("ooooooooooooooooooooooooooooooooooooooooooooooooooooooo")
    try:
        with open(json_file_path, 'r') as json_file:
            data = json.load(json_file)
        #     # Index each document from the JSON file
        #     for idx, document in enumerate(data):
        #         es.index(index=filename, body=document, id=idx)
            actions = [
                {
                    "_op_type":"index",
                    "_index": filename,
                    "_source": document,
                }
                for document in data
            ]

            try:
                # Use the helpers.bulk() method for bulk indexing
                success, failed = helpers.bulk(es, actions,index=filename, raise_on_error=True)
                print(f"Successfully indexed: {success} documents")
                print(f"Failed to index: {failed} documents")
                response = {"message": f"Successfully indexed: {success} documents"}
                # Print the details of failed documents
                for idx, document in enumerate(data):
                    # success, failed = helpers.bulk(es, [document], raise_on_error=False)
                    if failed:
                        print(f"Failed to index document at index {idx}: {failed[0]['index']['error']}")
                print("999999999999999999999999999999999999999999999999999")
                return jsonify([response])
                
                # if failed:
                #     for item in failed:
                #         response = {"message": f"Indexing error: {item['index']['error']}"}
                #         return jsonify([response])
                        
            except Exception as e:
                print(f"Error during bulk indexing: {e}")
                response = {"message": f"Error during bulk indexing: {e}"}
                # Print the failed documents for more details
                print("data")

                return jsonify([response])
    except Exception as e:

        return str(e)


     

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)