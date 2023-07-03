import logging
import azure.functions as func
import os
import urllib.request
import json
import os
import ssl
import json
import os
import pandas as pd
from azure.core.credentials import AzureKeyCredential
from azure.search.documents import SearchClient
import openai


GPT_ENGINE = os.environ["GPT_ENGINE"]

openai.api_type = "azure"
openai.api_key = os.environ["OPENAI_API_KEY"]  # SET YOUR OWN API KEY HERE
openai.api_base = os.environ["OPENAI_RESOURCE_ENDPOINT"]  # SET YOUR RESOURCE ENDPOINT
openai.api_version = "2023-03-15-preview"
admin_key = os.environ["AZSEARCH_KEY"] # Cognitive Search Admin Key
index_name = os.environ["INDEX_NAME"] # Cognitive Search index name
credential = AzureKeyCredential(admin_key)
default_system_message = os.environ["SYSTEM_MESSAGE"]

# Create an SDK client
endpoint = os.environ["AZSEARCH_EP"]

semantic_config = os.environ["SEMANTIC_CONFIG"]



def run_openai(prompt, engine=GPT_ENGINE):
    """Recognize entities in text using OpenAI's text classification API."""
    max_response_tokens = 1250
    token_limit= 4096
    try:
        response = openai.ChatCompletion.create(
                    engine=GPT_ENGINE,
                    messages = prompt,
                    temperature=0,
                    max_tokens=max_response_tokens,
                    stop=f"Answer:"
                    )
        return response['choices'][0]['message']['content']
    except Exception as e:
        return e.user_message


def azcognitive_score(user_query, topk,index_name, default_system_message):
    
    search_client = SearchClient(endpoint=endpoint,
                    index_name=index_name,
                    api_version="2021-04-30-Preview",
                    credential=credential)
    
    results = search_client.search(search_text=user_query, include_total_count=True, query_type='semantic', query_language='en-us',semantic_configuration_name=semantic_config)
    document=""
    sources = []

    i=0
    while i < topk:
        try:
            item = next(results)
            document += (item['text'])
            sourceInfo = {"fileName": item['fileName'], "pageNumber":  item['pageNumber']}
            sources.append(sourceInfo)
        except Exception as e:
            print(e)
            break
        i+=1
    #system_message="""
    #You are an AI search Assitant. You are given a question and a context. You need to answer the question using only the context.
    #If you do not know the Answer, you can say "I don't know".
    #The context is a collection of documents.
    #"""
    system_message =  {"role": "system", "content": default_system_message}
    question = {"role":"user", "content":f"Question: {user_query} \n <context> {document} </context>"}
    prompt= [system_message] +[question]
    return prompt, sources


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python orchestrator trigger function processed a request.')

    prompt = ""
    user_system_message = ""
    index_name = ""

    prompt = req.params.get('prompt')
    user_system_message = req.params.get('system_message')
    index_name = req.params.get('INDEX_NAME')    
   
    #set default value for topk to 5 if not provided
    topk = 5
    try:
        topk = int(req.params.get('num_search_result'))
    except Exception as e:
        pass    
        
    if prompt is None:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            prompt = req_body.get('prompt')
            prompt = req_body.get('prompt')
            user_system_message = req_body.get('system_message')
            index_name = req_body.get('INDEX_NAME')
        
    if user_system_message is not None:
        default_system_message = user_system_message
      
                
    gpt_prompt = ""
    sources = ""

    try:
        gpt_prompt, sources = azcognitive_score(prompt,topk, index_name, default_system_message)
    except Exception as e:
        return func.HttpResponse(json.dumps(e))

    print("gpt_prompt ",gpt_prompt )

    #result = None
    #try:
    result = run_openai(gpt_prompt)

    # load json into pandas dataframe
    if len(sources) > 0:
        df = pd.DataFrame(sources)
        arr_unique_filename = df['fileName'].unique()
        citations = ""
        for filename in arr_unique_filename:
            page_numbers = df.loc[df['fileName'] == filename]
            citations += f"\n{filename}, page numbers:"
            for page in page_numbers['pageNumber']:
                citations += f"[{page}]\n"

        result += result + f"\n\n\nCitations: \n{citations}"

    return func.HttpResponse(json.dumps({"result":result, "gpt_prompt":gpt_prompt, "sources":sources}))
    #except Exception as e:
    #    return func.HttpResponse(json.dumps(e))

