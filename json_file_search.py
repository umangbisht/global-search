from elasticsearch import Elasticsearch, helpers
import json
from flask import Flask, jsonify, request
from flask_cors import CORS
import requests
import csv
from io import StringIO
import pandas as pd
from io import BytesIO
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
                        "multi_match": {
                            "query": search_query,
                            "type": "phrase",
                        
                        }
                        # "multi_match": {
                        #     "query": search_query,
                        #     "type": "phrase",
                        
                        # }
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
            # "bool": {
            #     "must": [
            #         {
            #         #     "query_string": {
            #         #     "query": f"*{search_query}* OR {search_query}*",
            #         #     "fields": ["*"]
            #         # }
            #             "multi_match": {
            #                 "query": search_query,
            #                 "type": "phrase",
                        
            #             }
            #         },
                  
            #     ],
                
            # }
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
            print("json_data json_data", json_data)
            # Save JSON data to a file

            filename = filename.rsplit('.', 1)[0]

            filename = filename+'.json'
            print('./datastore/' + filename)

            save_json_to_file(json_data, './datastore/' + filename)
            if es.indices.exists(index=filename):
                response = {"message": f"Index '{filename}' already exists."}
                return jsonify([response])
            json_file_path = './datastore/'+filename

            # # CSV file path
            # csv_file_path = '../datastore/Telegram/linkedin_united kingdom_0.csv'

            # # JSON file path (where you want to save the converted JSON data)
            # json_file_path = '../json/linkedin_united kingdom_0.json'

            

            # Open the CSV file for reading
            # with open(csv_file_path, 'r') as csv_file:
            #     # Create a CSV reader
            #     csv_reader = csv.DictReader(csv_file)
                
            #     # Convert CSV data to a list of dictionaries
            #     data = [row for row in csv_reader if any(row.values())]
            #     print("data", data)

            # # Write the JSON data to a file
            # with open(json_file_path, 'w') as json_file:
            #     # Use the json module to dump the data to JSON format
            #     json.dump(data, json_file, indent=2)
        

    # Save the uploaded file to a designated folder
    # file.save('./datastore/' + filename)
    # print('./datastore/' + filename)
    # print("file successfully saved")
    else:
        json_file_path = './datastore/'+filename
        
        # Save the uploaded file to a designated folder
        file.save(json_file_path)
    # Check if the index already filename
    
    # Indexing JSON data
    try:
        print("ooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooo")
        print(json_file_path)
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

            print("444444444444444444444444444444444444444444444444444444444444444444444444444",filename)

            try:
                # Use the helpers.bulk() method for bulk indexing
                success, failed = helpers.bulk(es, actions,index=filename, raise_on_error=True)

                response = {"message": f"Successfully indexed: {success} documents"}
                # Print the details of failed documents
                for idx, document in enumerate(data):
                    # success, failed = helpers.bulk(es, [document], raise_on_error=False)
                    if failed:
                        print(f"Failed to index document at index {idx}: {failed[0]['index']['error']}")
                print("3333333333333333333333333333333333333333333333333333333333333", response)

                return jsonify([response])
                
                # if failed:
                #     for item in failed:
                #         response = {"message": f"Indexing error: {item['index']['error']}"}
                #         return jsonify([response])
                        
            except Exception as e:
                print("Error during bulk indexing1111111111111111111111: {e}")
                response = {"message": f"Error during bulk indexing: {e}"}
                # Print the failed documents for more details
                for idx, document in enumerate(data):
                    print(f"Failed document at index {idx}: {document}")
                # Print the failed documents for more details

                return jsonify([response])
    except Exception as e:
        print("222222222222222222222222222222222222222222222222222222222222222222222",str(e))
        return str(e)
    

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