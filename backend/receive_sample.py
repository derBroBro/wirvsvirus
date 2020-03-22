import logging
import os
import json
import boto3
import urllib.parse
import time
from datetime import datetime, timedelta
import hashlib

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

BUCKET = "wirvsvirus-data"

def write_key(filename, data):
    logger.info(f"write file {filename} to {BUCKET}")
    s3_client = boto3.resource("s3")
    s3_obj = s3_client.Object(BUCKET, filename)
    json_data = json.dumps(data)
    s3_obj.put(Body=json_data)

def get_post_parameter(event):
    body_vars = {}
    if not "body" in event:
        return body_vars

    body = urllib.parse.unquote(event["body"])
    for line in body.split("&"):
        line_data = line.split("=")
        if len(line_data) == 2:
            body_vars[line_data[0]] = line_data[1]
    return body_vars
    
def create_response(body, code=200, contenttype="text/html"):
    logger.info(f"{code} --> {body}")
    return {"statusCode": code, "body": body, "headers": {"Content-Type": contenttype,}}


def redirect(url):
    logger.info(f"Redirect to {url}")
    return {"statusCode": 301, "body": "body", "headers": {"Location": url,}}

def md5(text):
    return hashlib.md5(text.encode()).hexdigest()

def lambda_handler(event, context):
    items = get_post_parameter(event)
    logger.info(items)
    

    
    # {'fingerprint': '1a7b408070ce01dd293f61f6974f3e5b', 'target': 'me', 'alias': 'self', 'age': 'less_25', 'city': '', 'throat': '0', 'headache': '0', 'limb': '0', 'cough': '0', 'fever': '0', 'tested': '0', 'tested_result': '0'}
    
    now = datetime.utcnow()
    now_unix = int(time.mktime(now.timetuple()))
    
    sender_fingerprint = items["fingerprint"]
    alias = items["alias"]
    target = items["target"]

    # Hash this to prevent sending under wrong sender id
    sender_id = md5(sender_fingerprint)
    person_path = f"{sender_id}/{alias}"
    person_id = md5(person_path)

    logger.info(f"Got sample from {sender_fingerprint}(id:{sender_id}) for {alias} ({person_path} = id:{person_id})")
    
    if target == "other":
        logger.info("Sample was submitted for somene else")
        

    duration = items["duration"]
    start_time = now - timedelta(hours=int(duration)*24)
    
    key = f"{sender_id}/{alias}"
    data = {
        "sample_creation": now.isoformat(),
        "sender_id": sender_id,
        "person_id": person_id,
        "target_kind": target,
        "age": items["age"],
        "location" : items["location"],
        "symptoms_duration" : items["duration"],
        "symptoms_calculated_start": start_time.isoformat(),
        "symptoms_throat": items["throat"],
        "symptoms_headache": items["headache"],
        "symptoms_limb": items["limb"],
        "symptoms_cough": items["cough"],
        "symptoms_fever": items["fever"],
        "covid19_tested": items["tested"],
        "covid19_test_result": items["test_result"],
        "covid19_test_duration": items["test_duration"],
    }
    write_key(f"{person_path}/{now_unix}.json",data)
    
    #return redirect("http://localhost:8000/frontend")
    return create_response(json.dumps(data, indent=4))