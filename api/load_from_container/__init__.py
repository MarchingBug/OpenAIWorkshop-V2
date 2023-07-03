import logging
import requests
import json
import sys  

import azure.functions as func
import os


SEARCH_INDEX = ''

def main(req: func.HttpRequest,context: func.Context) -> func.HttpResponse:
    logging.info('Python HTTP trigger executor function processed a request.')

    my_url = req.url
    
    domain = my_url.replace(context.function_name,"")
    logging.info(domain)

    read_storage_account_files_url =  domain + "read_storage_account_files" + parseMasterKey()
    ingest_files_url = domain + "ingest_file" + parseMasterKey()

    storage_account_container = req.params.get('storage_account_container')
    if not storage_account_container:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            storage_account_container = req_body.get('storage_account_container')
            SEARCH_INDEX = req_body.get('INDEX_NAME')
            
    #if varibles are null or blank set it to read environment variable
    if len(SEARCH_INDEX) == 0 :
        SEARCH_INDEX = os.environ['INDEX_NAME']
    
    
    if storage_account_container:
    
        question_body = '{' 
        question_body = question_body + '"storage_account_container" :"' + storage_account_container + '"'        
        question_body =  question_body + '}' 
       
        headers = {'Content-Type': 'application/json'}
        
        logging.info(f'reading files in container {storage_account_container} from storage account')
        
        response = requests.request("GET",  read_storage_account_files_url, headers=headers, data=question_body)
        logging.debug(f"Response: {response.text}")
        
        #convert response to json
        try:
            list_of_files = response.text
            files = json.loads(list_of_files)
            logging.debug(f"Response: {response}")
            
            #iterate through the response and add the file names to the files_to_read list           
            for file in files['files']:
                the_file = file['file']
                logging.debug(f"File: {the_file}")
                #parse payload using URL and CreateIndex
                #call the ingest_file function
                logging.info(f"Processing the file {the_file} ")                
              
                payload = '{' 
                payload = payload + '"URL" :"' + the_file + '",'                              
                payload = payload + '"INDEX_NAME" :"' + SEARCH_INDEX +'"' 
                payload = payload + '}' 
              
                headers = {'Content-Type': 'application/json'}
                response = requests.request("POST", ingest_files_url, headers=headers, data=payload) 
                    
        except:
          exc_tuple = sys.exc_info()
          logging.error(str(exc_tuple))
          errors = [ { "message": "Failure files executor e: " + str(exc_tuple)}]
              
        
          return func.HttpResponse( 
                 body=errors, mimetype="application/json",
                 status_code=500
              )  
    
        
    else:
        return func.HttpResponse(
             "This HTTP triggered function executed successfully. Pass a name in the query string or in the request body for a personalized response.",
             status_code=200
        )
        
        
    return func.HttpResponse(
           "Python HTTP trigger executor function processed a request successfully.",
            status_code=200
       )


def parseMasterKey():
    master_key = os.environ['AzureFunctionKey']    
    master_key_value = "?code=" + master_key
    if len(master_key)== 0:
        return ""
    else:
        return master_key_value
