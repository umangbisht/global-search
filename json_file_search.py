from elasticsearch import Elasticsearch
import json
from flask import Flask, jsonify, request
from flask_cors import CORS
from elasticsearch.helpers import bulk

app = Flask(__name__)
CORS(app)

# Elasticsearch configuration
es = Elasticsearch([{"host": "localhost", "port": 9200, "scheme": "http"}], verify_certs=False, http_auth=None)




@app.route('/search-file', methods=['GET'])
def get_search_data():
    # Get query parameters from the request
    search_query = request.args.get('data')
    print("search_query", search_query)

    # Perform a simple full-text search for books with titles containing 'python'
    search_query = search_query
    # Get all indices
    all_indices = es.indices.get_alias(index="*").keys()

    # Convert the indices to a list
    index_list = list(all_indices)
    print("indices", index_list)

    # Generate the search query
    query = {
        "query": {
            "multi_match": {
                "query": search_query,
                "type": "phrase",
             
            }
        }
    }
    # Print the query for debugging

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
    # Check if the post request has the file part
    if 'file' not in request.files:
        return jsonify({'error': 'No file part'})

    file = request.files['file']
    filename = file.filename

    # Check if the file is not empty
    if filename == '':
        return jsonify({'error': 'No selected file'})

    # Save the uploaded file to a designated folder
    file.save('../datastore/' + filename)
    
    # Check if the index already filename
    if es.indices.exists(index=filename):
        response = {"message": f"Index '{filename}' already exists."}
        return jsonify([response])
    # Indexing JSON data
    json_file_path = '../datastore/'+filename

    try:
        with open(json_file_path, 'r') as json_file:
            data = json.load(json_file)
            # Index each document from the JSON file
            for idx, document in enumerate(data):
                es.index(index=filename, body=document, id=idx)
            response = {"message":"Index created successfully!"}
            return jsonify([response])
    except Exception as e:

        return str(e)


     

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)