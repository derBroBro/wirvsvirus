import logging
import os
import json
import boto3
from datetime import datetime
import hashlib

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

BUCKET_SOURCE = "wirvsvirus-data"
BUCKET_TARGET = "wirvsvirus-public"

def write_key(filename, data):
    logger.info(f"write file {filename} to {BUCKET_TARGET}")
    s3_client = boto3.resource("s3")
    s3_obj = s3_client.Object(BUCKET_TARGET, filename)
    s3_obj.put(Body=data)
    

    
def read_all_data():
    result = []
    s3_client = boto3.client("s3")
    files = s3_client.list_objects_v2(Bucket=BUCKET_SOURCE)
    for file in files["Contents"]:
        file_path = file["Key"]
        logger.info(f"read file {file_path} from {BUCKET_SOURCE}")
        s3_client = boto3.resource("s3")
        s3_obj = s3_client.Object(BUCKET_SOURCE, file_path)
        try:
            data = s3_obj.get()["Body"].read()
            result.append(json.loads(data))
        # make nicer and chat execption
        except Exception as e:
            logger.warn(f"Failed to read or parse")
    return result
            
def md5(text):
    return hashlib.md5(text.encode()).hexdigest()

def lambda_handler(event, context):
    now = datetime.utcnow()
    data = read_all_data();
    lines = []
    lines.append( "sender_id;person_id;target_kind;age;location;symptoms_duration;symptoms_calculated_start;symptoms_throat;symptoms_headache;symptoms_limb;symptoms_cough;symptoms_fever;covid19_tested;covid19_test_result;covid19_test_duration" )
    for item in data:
        logger.info(item)
        # Hash this to prevent sending under wrong sender id
        fingerprint = item["fingerprint"]
        alias = item["alias"]
        target = item ["target"]
        
        # hash for data_privacy
        sender_id = md5(fingerprint)
        person_path = f"{fingerprint}/{alias}"
        person_id = md5(person_path)
        
        lines.append( f"{sender_id};{person_id};{target};{item['age']};{item['location']};{item['symptoms_duration']};{item['symptoms_calculated_start']};{item['symptoms_throat']};{item['symptoms_headache']};{item['symptoms_limb']};{item['symptoms_cough']};{item['symptoms_fever']};{item['covid19_tested']};{item['covid19_test_result']};{item['covid19_test_duration']}" )
        
    
    file_prefix = now.strftime("%Y-%m-%d_%H-00")
    logger.info(f"Write to {file_prefix}.json")
    write_key(f"data/{file_prefix}.csv","\n".join(lines))

    return "ok"
    