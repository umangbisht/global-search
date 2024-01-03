from elasticsearch import Elasticsearch, helpers
import json
from flask import Flask, jsonify, request
from flask_cors import CORS
import requests
import csv
from io import StringIO
import pandas as pd
from io import BytesIO
from elasticsearch.helpers import BulkIndexError
import re
# from elasticsearch.helpers import bulk

app = Flask(__name__)
CORS(app)

# Elasticsearch configuration
es = Elasticsearch([{"host": "localhost", "port": 9200, "scheme": "http"}], verify_certs=False, http_auth=None)




@app.route('/search-file', methods=['POST'])
def get_search_data():
    # Get query parameters from the request
    search_query = request.json.get('data')
    # search_query_2 = request.args.get('must_not')
    print("search_queryyyyyyyyyyyyyyyyyyyyyyyyyyyyy", search_query)

    # Get all indices
    all_indices = es.indices.get_alias(index="*").keys()

    # Convert the indices to a list
    index_list = list(all_indices)
    print("indices", index_list)


    # Generate the search query
    query = {
        "min_score": 0.5,
        "size": 100000,
        "query": {
            "bool": {
                "should": [
                    {
                        "multi_match": {
                            "query": search_query,
                            "type": "phrase",
                        
                        }
                    },
                  
                ],
                "must": [
                    {
                        "query_string": {
                        "query": f"*{search_query}* OR {search_query}*",
                        "fields": ["*"]
                    }

                    },
                  
                ],  
            },
        }
    }

 

    # Execute the search
    response = es.search(index=index_list,method='POST', body=query)

    # Process the search results
    hits = response['hits']['hits']


    if hits:
        # Extract sources from all hits
        result_list = [{"_source": hit['_source'], "_index": hit['_index']} for hit in hits]
        return jsonify(result_list)
    else:
        # Return an empty list if no hits
        return jsonify([])

def clean_phone_number(phone_number):
    cleaned_phone_num = re.sub(r'\D', '', phone_number)
    cleaned_phone_num = re.sub(r'^91', '', cleaned_phone_num)
    return cleaned_phone_num   
    
@app.route('/indexing-file', methods=['POST'])
def make_file_index():
    FILE_TO_CONVERT = ['csv','txt','xlsx']
    # Check if the post request has the file part
    if 'file' not in request.files:
        return jsonify({'error': 'No file part'})
    
    

    file = request.files['file']
    filename = file.filename.lower()
    # Check if the file is not empty
    if filename == '':
        return jsonify({'error': 'No selected file'})
    filename = filename.replace(" ", "").replace(',', '').replace('_', '').replace("'", "")

    # Split from the last dot
    file_extension = filename.rsplit('.', 1)[1]

    if es.indices.exists(index=filename):
        response = {"message": f"Index '{filename}' already exists."}
        return jsonify([response])

    if request.form.get('file_format'):
        file_format = request.form.get('file_format')
    
        if file_format  in FILE_TO_CONVERT and file_format == 'csv':
            # Process the file and convert to JSON
            json_data = convert_csv_to_json(file)
            # Save JSON data to a file

            filename = filename.rsplit('.', 1)[0]

            filename = filename+'.json'

            save_json_to_file(json_data, './datastore/' + filename)
            if es.indices.exists(index=filename):
                response = {"message": f"Index '{filename}' already exists."}
                return jsonify([response])
            json_file_path = './datastore/'+filename

        elif file_format in FILE_TO_CONVERT and file_format == 'xlsx':
            # Process the file and convert to JSON
            json_data = convert_excel_to_json(file)
            # Save JSON data to a file
            filename = filename.rsplit('.', 1)[0]
            filename = filename+'.json'

            save_json_to_file(json_data, './datastore/' + filename)
            if es.indices.exists(index=filename):
                response = {"message": f"Index '{filename}' already exists."}
                return jsonify([response])
            json_file_path = './datastore/'+filename

    else:
        json_file_path = './datastore/'+filename
        
        # Save the uploaded file to a designated folder
        file.save(json_file_path)
    # Check if the index already filename
    print("filesuccessfully saved!!!!!!")
    
    # Indexing JSON data
    try:
        # Load data from the JSON file
        with open(json_file_path, 'r') as json_file:
            data = json.load(json_file)

        # Create the actions list outside the loop
        actions = [
            {
                "_op_type": "index",
                "_index": filename,
                "_source": document,
            }
            for document in data
        ]

        # Loop through the data to modify "Mobile" values and perform bulk indexing
        for idx, document in enumerate(data):
            for key, value in document.items():
                # Check if the key contains the string "Mobile"
                if "Mobile" in key or "mobile" in key or "phone" in key:
                    # Convert the corresponding value to a string
                    value = str(value)
                    # Assign the updated value back to the key in the document dictionary
                    document[key] = value
                    # Optionally, call the clean_phone_number function
                    value = clean_phone_number(value)
                    print("Modified document:", document)

        # Use the helpers.bulk() method for bulk indexing
        success, failed = helpers.bulk(es, actions, index=filename, raise_on_error=True)
        response = {"message": f"Successfully indexed: {success} documents"}

        # Print the details of failed documents
        for idx, document in enumerate(data):
            if failed:
                print(f"Failed to index document at index {idx}: {failed[0]['index']['error']}")

        return jsonify([response])

    except BulkIndexError as e_bulk:
        response = {"message": f"Error during bulk indexing: {e_bulk}"}

        # Get the details of failed documents from the BulkIndexError
        failed_items = e_bulk.errors
        print("Failed items:", failed_items)
        if failed_items:
            for idx, failed_item in enumerate(failed_items):
                if 'index' in failed_item and 'error' in failed_item['index']:
                    error_message = failed_item['index']['error']['reason']
                    print(f"Failed to index document at index {idx}: {error_message}")
                else:
                    print(f"Failed to index document at index {idx}: Unknown error")

        return jsonify([response])
    

def convert_csv_to_json(file):
    # Use StringIO to treat the file as a string
    with StringIO(file.read().decode('utf-8')) as csv_file:
        # Create a CSV reader
        csv_reader = csv.DictReader(csv_file)

        # Convert CSV to JSON
        json_data = json.dumps(list(csv_reader), indent=2)

    return json_data

def convert_excel_to_json(excel_file):
    try:
        # Read Excel file into DataFrame
        excel_data_df = pd.read_excel(BytesIO(excel_file.read()))

        # Convert DataFrame to JSON string
        json_str = excel_data_df.to_json(orient='records')

        # Save the JSON string to a file or do further processing
        # For example, you can return the JSON string to the frontend

        print('Conversion Successful from Excel to JSON!')
        return json_str
    except Exception as e:
        print(f'Error: {e}')
        return None

def save_json_to_file(json_data, filename):
    with open(filename, 'w') as json_file:
        json.dump(json.loads(json_data), json_file, indent=2)


     

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)