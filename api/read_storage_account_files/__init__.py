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

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP read_storage_account_files function processed a request.')

    file_pattern = req.params.get('file_pattern')
    if not file_pattern:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            file_pattern = req_body.get('file_pattern')
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
            permission=AccountSasPermissions(read=True,list=True),
            expiry=datetime.utcnow() + timedelta(hours=2)
        )
       
        this_container_url = "https://"+AZURE_ACC_NAME+".blob.core.windows.net/"+STORAGE_ACCOUNT_CONTAINER+"?"+sas_token
        
        #blob_service_client = BlobServiceClient(account_url= "https://"+AZURE_ACC_NAME+".blob.core.windows.net/", credential=sas_token)        
       
        container = ContainerClient.from_container_url(this_container_url)
        blob_list = container.list_blobs()   
      
     
        #generate SAS token for each file
        for blob in blob_list:                          
            doit = True            
           
            if file_pattern == None:
                doit = True
            else:
                doit = False
                if file_pattern in blob.name:
                   doit = True
            
            if doit == True:
                sas_token = generate_blob_sas(
                        account_name=AZURE_ACC_NAME,
                        container_name=STORAGE_ACCOUNT_CONTAINER,
                        blob_name=blob.name,
                        account_key=AZURE_PRIMARY_KEY,
                        permission=BlobSasPermissions(read=True),
                        expiry=datetime.utcnow() + timedelta(hours=2)
                    )
                
                filewithsas=  "https://"+AZURE_ACC_NAME+".blob.core.windows.net/"+STORAGE_ACCOUNT_CONTAINER+"/"+blob.name+"?"+sas_token  
            
                filewithsas = '{"file":"' + filewithsas + '"}'            
                # if counter == number_of_blobs:
                #     fileswithsas = fileswithsas[:-1]
            
                files_to_read.append(filewithsas)            
      
        result = '{"files":['+ ','.join(files_to_read) +']}'                
            
        response_body = result
        return func.HttpResponse( 
                    body=response_body, mimetype="application/json",
                    status_code=200
            )  
    
    except:
        exc_tuple = sys.exc_info()
        logging.error(str(exc_tuple))
        errors = [ { "message": "Failure during read_storage_account_files e: " + str(exc_tuple)}]
        return func.HttpResponse( 
                    body=errors, mimetype="application/json",
                    status_code=500
            )  
