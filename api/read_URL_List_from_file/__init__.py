import logging

import azure.functions as func
import requests
import azure.storage.blob
from azure.storage.blob import BlobServiceClient, BlobSasPermissions, generate_blob_sas,generate_container_sas,ContainerClient
from azure.storage.blob import ResourceTypes, AccountSasPermissions, generate_account_sas
import os
import sys
import datetime
from datetime import datetime, timedelta

AZURE_ACC_NAME = os.environ['AccountName']
AZURE_PRIMARY_KEY = os.environ['AccountAccessKey']
STORAGE_ACCOUNT_CONTAINER = os.environ['StorageAccountContainer']
DESTINATION_ACCOUNT_CONTAINER = os.environ['DestinationStorageAccountContainer']

def parseMasterKey():
    master_key = os.environ['AzureFunctionKey']    
    master_key_value = "?code=" + master_key
    if len(master_key)== 0:
        return ""
    else:
        return master_key_value

def main(req: func.HttpRequest,context: func.Context) -> func.HttpResponse:
    logging.info('Python HTTP read_URL_List_from_file function processed a request.')

    my_url = req.url
    
    domain = my_url.replace(context.function_name,"")
    logging.info(domain)

    read_storage_account_files_url =  domain + "read_storage_account_files" + parseMasterKey()
    ingest_files_url = domain + "ingest_file" + parseMasterKey()

    source_file_name = req.params.get('source_file_name')
    if not source_file_name:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            source_file_name = req_body.get('source_file_name')
            if req_body.get('storage_account_container') != None:
                STORAGE_ACCOUNT_CONTAINER = req_body.get('storage_account_container')
    
    files_to_read = []
            
    storage_account_connection_string = "DefaultEndpointsProtocol=https;AccountName="+AZURE_ACC_NAME+";AccountKey="+AZURE_PRIMARY_KEY+";EndpointSuffix=core.windows.net"

    try:        
        blob_service_client   = BlobServiceClient.from_connection_string(storage_account_connection_string)   
        
        sas_token = generate_container_sas(
            blob_service_client.account_name,
            account_key=blob_service_client.credential.account_key,
            container_name=STORAGE_ACCOUNT_CONTAINER,
            resource_types=ResourceTypes(object=True),
            permission=AccountSasPermissions(read=True),
            expiry=datetime.utcnow() + timedelta(hours=2)
        )
           
        blob_service_client = BlobServiceClient(account_url=f"https://{AZURE_ACC_NAME}.blob.core.windows.net", credential=sas_token)      
        blob_client = blob_service_client.get_blob_client(container=STORAGE_ACCOUNT_CONTAINER, blob=source_file_name)
        blob_data = blob_client.download_blob().content_as_text()
        
        lines = blob_data.split('\n')
        for line in lines:
            logging.info(line)
            json_format = '{"file":"' + line + '"}'
            files_to_read.append(json_format)
       
        result = '{"files":['+ ','.join(files_to_read) +']}'   
        
        return func.HttpResponse( 
                    body=result, mimetype="application/json",
                    status_code=200
            ) 
        
        
    except:
        exc_tuple = sys.exc_info()
        logging.error(str(exc_tuple))
        errors = [ { "message": "Failure during read_URL_list_from_file e: " + str(exc_tuple)}]
        return func.HttpResponse( 
                    body=errors, mimetype="application/json",
                    status_code=500
            ) 
