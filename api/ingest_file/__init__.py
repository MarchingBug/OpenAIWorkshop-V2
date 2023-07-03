import logging
import azure.functions as func

from azure.core.credentials import AzureKeyCredential
from azure.ai.formrecognizer import DocumentAnalysisClient
from dotenv import load_dotenv
import os
import requests
import csv
import urllib.request, urllib.parse, urllib.error,  urllib


SEARCH_ENDPOINT = os.environ["AZSEARCH_EP"]
SEARCH_API_KEY = os.environ["AZSEARCH_KEY"]
SEARCH_INDEX = os.environ["INDEX_NAME"]
api_version = os.environ["AZSEARCH_API_VERSION"]

#if SEARCH_ENDPOINT is missing / at the end, add it
if SEARCH_ENDPOINT[-1] != '/':
    SEARCH_ENDPOINT = SEARCH_ENDPOINT + '/'

#if api_version does not contains "?" then add it
if len(api_version) != 0:
    if api_version[0] != '?':
        api_version = '?api-version=' + api_version 
    
headers = {'Content-Type': 'application/json',
        'api-key': SEARCH_API_KEY }


params = urllib.parse.urlencode({
      'api-version':api_version
})

endpoint = os.environ["AFR_ENDPOINT"]
key = os.environ["AFR_API_KEY"]

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP ingest_file.')

    #read things from the request
    #api_version is not null
    
    # if len(api_version) == 0 :
    #    api_version = '?api-version=2021-04-30-Preview' 
    # else:
    #    logging.info('API version is: %s', api_version)
   
    file_URL = req.params.get('URL')
    if not file_URL:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            file_URL = req_body.get('URL')
            SEARCH_INDEX = req_body.get('INDEX_NAME')
            index_operation = req_body.get('INDEX_OPERATION') #create, update

    
    document_analysis_client = DocumentAnalysisClient(
    endpoint=endpoint, credential=AzureKeyCredential(key)
    )   

    index_name = SEARCH_INDEX

    index_schema = {
        "name": index_name,
        "fields": [
            {
            "name": "id",
            "type": "Edm.String",
            "facetable": False,
            "filterable": False,
            "key": True,
            "retrievable": True,
            "searchable": False,
            "sortable": False,
            "indexAnalyzer": None,
            "searchAnalyzer": None,
            "synonymMaps": [],
            "fields": []
            },
            {
            "name": "text",
            "type": "Edm.String",
            "facetable": False,
            "filterable": False,
            "key": False,
            "retrievable": True,
            "searchable": True,
            "sortable": False,
            "indexAnalyzer": None,
            "searchAnalyzer": None,
            "synonymMaps": [],
            "fields": []
            },
            {
            "name": "fileName",
            "type": "Edm.String",
            "facetable": False,
            "filterable": False,
            "key": False,
            "retrievable": True,
            "searchable": False,
            "sortable": False,
            "indexAnalyzer": None,
            "searchAnalyzer": None,
            "synonymMaps": [],
            "fields": []
            },
            {
            "name": "pageNumber",
            "type": "Edm.String",
            "facetable": False,
            "filterable": False,
            "key": False,
            "retrievable": True,
            "searchable": False,
            "sortable": False,
            "indexAnalyzer": None,
            "searchAnalyzer": None,
            "synonymMaps": [],
            "fields": []
            },
            {
            "name": "summary",
            "type": "Edm.String",
            "facetable": False,
            "filterable": False,
            "key": False,
            "retrievable": True,
            "searchable": True,
            "sortable": False,
            "analyzer": "standard.lucene",
            "indexAnalyzer": None,
            "searchAnalyzer": None,
            "synonymMaps": [],
            "fields": []
            },
            {
            "name": "title",
            "type": "Edm.String",
            "facetable": False,
            "filterable": False,
            "key": False,
            "retrievable": True,
            "searchable": True,
            "sortable": False,
            "analyzer": "standard.lucene",
            "indexAnalyzer": None,
            "searchAnalyzer": None,
            "synonymMaps": [],
            "fields": []
            },
            {
            "name": "embedding",
            "type": "Collection(Edm.Double)",
            "facetable": False,
            "filterable": False,
            "retrievable": True,
            "searchable": False,
            "analyzer": None,
            "indexAnalyzer": None,
            "searchAnalyzer": None,
            "synonymMaps": [],
            "fields": []
            }
            
        ],
        "suggesters": [],
        "scoringProfiles": [],
        "defaultScoringProfile": "",
        "corsOptions": None,
        "analyzers": [],
        "semantic": {
            "configurations": [
            {
                "name": "semantic-config",
                "prioritizedFields": {
                "titleField": {
                        "fieldName": "title"
                    },
                "prioritizedContentFields": [
                    {
                    "fieldName": "text"
                    }            
                ],
                "prioritizedKeywordsFields": [
                    {
                    "fieldName": "text"
                    }             
                ]
                }
            }
            ]
        },
        "charFilters": [],
        "tokenFilters": [],
        "tokenizers": [],
        "@odata.etag": "\"0x8D8B90E3409E48F\""
        }

    
    #delete_search_index(index_name)
    create_search_index(index_schema, index_name)
    
    if(file_URL != ""):
      logging.info(f"Analyzing form from URL {file_URL}...")
      poller = document_analysis_client.begin_analyze_document_from_url("prebuilt-document", file_URL)
      result = poller.result()
      logging.info(f"Processing result...this might take a few minutes...")      
      process_afr_result(result, "",index_name)   


    if file_URL:
        return func.HttpResponse(f"Hello, {file_URL}. This HTTP triggered function executed successfully.")
    else:
        return func.HttpResponse(
             "This HTTP triggered function executed successfully. Pass a file_url in the query string or in the request body for a personalized response.",
             status_code=200
        )
        

def delete_search_index(index_name):
    try:
        url = SEARCH_ENDPOINT + "indexes/" + index_name + api_version 
        response  = requests.delete(url, headers=headers)
        logging.info("Index deleted")
    except Exception as e:
        logging.info(e)

def create_search_index(index_schema,index_name):
    try:
        # Create Index
        url = SEARCH_ENDPOINT + "indexes/" + index_name + api_version
        response  = requests.put(url, headers=headers, json=index_schema)
        index = response.json()
        logging.info("Index created")
    except Exception as e:
        logging.info(e)

def add_document_to_index(page_idx, documents,search_index):
    try:
        url = SEARCH_ENDPOINT + "indexes/" + search_index + "/docs/index" + api_version
        response  = requests.post(url, headers=headers, json=documents)
        logging.info(f"page_idx is {page_idx} - {len(documents['value'])} Documents added")
    except Exception as e:
        logging.info(e)
        
def process_afr_result(result, filename,search_index):
    logging.info(f"Processing {filename } with {len(result.pages)} pages into Azure Search....this might take a few minutes depending on number of pages...")
    for page_idx in range(len(result.pages)):
        docs = []
        content_chunk = ""
        for line_idx, line in enumerate(result.pages[page_idx].lines):
            #print("...Line # {} has text content '{}'".format(line_idx,line.content.encode("utf-8")))
            content_chunk += str(line.content.encode("utf-8")).replace('b','') + "\n"

            if line_idx != 0 and line_idx % 20 == 0:
              search_doc = {
                    "id":  f"page-number-{page_idx + 1}-line-number-{line_idx}",
                    "text": content_chunk,
                    "fileName": filename,
                    "pageNumber": str(page_idx+1)
              }
              docs.append(search_doc)
              content_chunk = ""
        search_doc = {
                    "id":  f"page-number-{page_idx + 1}-line-number-{line_idx}",
                    "text": content_chunk,
                    "fileName": filename,
                    "pageNumber": str(page_idx + 1)
        }
        docs.append(search_doc)   
        add_document_to_index(page_idx, {"value": docs},search_index)
        #create_chunked_data_files(page_idx, search_doc)

def create_chunked_data_files(page_idx, search_doc):
    try:
        output_path = os.path.join(os.getcwd(), "data-files", f'{page_idx}-data.csv')
        with open(output_path, 'w') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow([search_doc['id'], search_doc['text']])
            
    except Exception as e:
        logging.info(e)