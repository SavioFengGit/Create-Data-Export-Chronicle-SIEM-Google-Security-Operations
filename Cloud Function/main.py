import os
import time
from google.oauth2 import service_account 
import requests 
import json
import utils
from pprint import pprint
from datetime import datetime, timedelta, timezone
from google.auth.transport.requests import AuthorizedSession

#Creates a new data export.
#Dates/Times are in UTC
now = datetime.now(timezone.utc)
endDate = (now - timedelta(days=30)).strftime("%Y-%m-%d")  # End date is 1 month ago
startDate = (now - timedelta(days=365)).strftime("%Y-%m-%d")  # Start date is 12 months ago

#Time should be in the format HH:MM:SS including leading zeros
startTime = "00:00:00" #@param {type: "string"}
endTime = "23:59:59" #@param {type: "string"}

startTime = f"{startDate}T{startTime}Z"
endTime = f"{endDate}T{endTime}Z"

ENV_CHRONICLE_BUCKET_STORAGE ="CHRONICLE_BUCKET_STORAGE"
ENV_CHRONICLE_SERVICE_ACCOUNT = "CHRONICLE_SERVICE_ACCOUNT"
credentials_file = utils.get_env_var(ENV_CHRONICLE_SERVICE_ACCOUNT, is_secret=True)
bucket_storage = utils.get_env_var(ENV_CHRONICLE_BUCKET_STORAGE, is_secret=True)
credentials_file = json.loads(credentials_file)
SCOPES = ["https://www.googleapis.com/auth/chronicle-backstory"]

# Load the credentials
credentials = service_account.Credentials.from_service_account_info(
    credentials_file,
    scopes = SCOPES,
)
auth_session = AuthorizedSession(credentials)


import functions_framework
@functions_framework.http
def main(req): 
    print("Cloud Function Started")
    logType = "ALL_TYPES" 
    body = {
      "startTime": startTime,
      "endTime": endTime,
      "logType": logType,
      "gcsBucket": bucket_storage,
    }
     
    region_prefix = "europe"
    uri_to_post = f"https://{region_prefix}-backstory.googleapis.com/v1/tools/dataexport"
    print(uri_to_post)

    
    try:

        bucket_client = utils.connect_bucket(bucket_storage.split("/")[-1])
        stats = utils.get_stats(bucket_client)

        resp = auth_session.post(uri_to_post, json=body)
        resp.raise_for_status()
        pprint(resp.json())


        resp = resp.json()
        stats[resp["dataExportId"]] = {}
        stats[resp["dataExportId"]]["status"] = resp["dataExportStatus"]["stage"]
        stats[resp["dataExportId"]]["notified"] = "no"
        
        utils.write_stats(bucket_client,stats)



    #print(resp.text)
    #is raised when an HTTP request returns an error status. resp.raise_for_status() will raise an HTTPError if the response returned from the 
    #server has an error status (e.g., 404, 500, etc.).
    except requests.exceptions.HTTPError as errh:
        return "Http Error:"+errh,300
    #is raised in case of network problems, like when the connection to the server fails
    except requests.exceptions.ConnectionError as errc:
        return "Error Connecting:"+errc,300
    # is raised when a request exceeds the maximum time set for its execution
    except requests.exceptions.Timeout as errt:
        return "Timeout Error:"+errt,300
    # is raised if a request exceeds the configured number of maximum redirections. This can occur when a server returns a redirect 
    #response (like 301, 302) in response to a request
    except requests.exceptions.TooManyRedirects as errr:
        return "Too Many Redirects:"+errr,300
    #This exception is raised if a valid URL is not provided for the request.
    except requests.exceptions.URLRequired as erru:
        return "A valid URL is required to make a request:"+erru,300
    #This exception is raised if the URL schema (e.g., http or https) is missing.
    except requests.exceptions.MissingSchema as errs:
        return "The URL schema (e.g. http or https) is missing:"+errs,300
    #This exception is raised if the URL schema provided is invalid.
    except requests.exceptions.InvalidSchema as erri:
        return "The URL schema is invalid:"+erri,300
    #This exception is raised if the URL provided is not valid.
    except requests.exceptions.InvalidURL as errv:
        return "The URL is not valid:"+errv,300
    #This exception is raised when a request fails to retry.
    except requests.exceptions.RetryError as errre:
        return "Request failed to retry:"+errre,300
    #Default exception
    except requests.exceptions.RequestException as err:
        return "Something went wrong with the request:"+err,300
    
    print("Successfull Creation export!")
    return "ok",200