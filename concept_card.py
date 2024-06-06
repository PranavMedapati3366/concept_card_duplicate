import csv
import requests #pip install requests
import json
import xlrd  #pip install xlrd,openpyxl
import pandas as pd
from urllib.parse import urlparse, unquote
import os
import logging

csv_payload_template = {
    "concept_card_id": "",
    "concept_card_name": "",
    "type": "CONCEPT_CARDS",
    "language": "ENGLISH",
    "super_topic":"",
    "topic":"",
    "sub_topic":"",
    "offline_center":"",
    "offline_stream":"",
    "offline_subject":"",
    "offline_super_topic":"",
    "offline_topic":""
}

taxanomy_payload_template= {
      "name": "",
      "description": "",
      "type": "CONCEPT_CARDS",
      "sub_type": "CONCEPT_CARD",
      "language": "ENGLISH",
      "content_stakeholders": {
        "faculty_name": "vallari.srivastava",
        "created_by": "some_ops"
      },
      "client_id": "console",
      "tenant_id": "aUSsW8GI03dyQ0AIFVn92",
      "material_id": "",
      "session": "04_2024__03_2025",
      "center_id": "KOTA",
      "review_details": {
        "reviewed_by": "someone",
        "reviewed_on": 894289892
      },
      "content_expiry": 894289892,
      "duration_minutes": 80,
      "hashtags": [],
      "learning_category": "SELF_PACED",
      "master_course": "",
      "stream": "",
      "taxonomy_attributes": {
        
        "class": "Class 12",
        "subject": "",
        "super_topic": "",
        "topic": "",
        "sub_topic": []
        
      },
      "file_name": ""
    }

def get_presigned_url(concept_card_id):
    url = f'https://api.allen-live.in/api/v1/learningMaterials/{concept_card_id}/presigned_url'
    response = requests.get(url)
    response = json.loads(response.text)
    # print(response)
    response = response.get('data',{}).get('upload_data',{})
    # print(response['presigned_url'])
    return response['presigned_url']


def save_file(presigned_url):
    parsed_url = urlparse(presigned_url)
    path = parsed_url.path
    filename = unquote(path.split('/')[-1])  # Decodes any percent-encoded characters
    # print(filename)

    # The local path where the file will be saved
    local_filename = filename

    # Send a GET request to the pre-signed URL
    response = requests.get(presigned_url, stream=True)

    # Check if the request was successful
    if response.status_code == 200:
        # print("started")
        with open(local_filename, 'wb') as file:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:  # Filter out keep-alive new chunks
                    file.write(chunk)
        # logging.info(f"File downloaded successfully and saved as {local_filename}")
        
        return filename
    else:
        logging.error(f"Failed to download the file. Status code: {response.status_code}")
        return 
    

def get_material_formattype(concept_card_id):
    url = f"https://api.allen-live.in/api/v1/learningMaterials/{concept_card_id}"

    response = requests.get(url)
    
    response = json.loads(response.text)
    # print(response)
    response = response.get('data',{}).get('material_info',{}).get('content_file').get('file_format')
    # print(response)
    return response
    

def validate_bulk(csvdata):
    # print(csvdata)
    payload =taxanomy_payload_template.copy()
    
    payload['name'] = csvdata['concept_card_name']
    payload['type'] = csvdata['type']
    payload['stream'] = csvdata['offline_stream']
    payload['taxonomy_attributes']['subject'] = csvdata['offline_subject']
    payload['taxonomy_attributes']['super_topic'] = csvdata['offline_super_topic']
    payload['taxonomy_attributes']['topic'] = csvdata['offline_topic']
    # print(payload)
    headers = {
        'Content-Type':'application/json',
        'Authorization': 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJhdWQiOiJhVVNzVzhHSTAzZHlRMEFJRlZuOTIiLCJkX3R5cGUiOiJ3ZWIiLCJkaWQiOiJmNjFhMWUzOS1mZWM1LTQwZDEtYTZmMy1hMGJkNDgwMmY0OWQiLCJlX2lkIjoiOTQxOTY3MjU4IiwiZXhwIjoxNzE3NzE4Mjc2LCJpYXQiOiIyMDI0LTA2LTA2VDA1OjU3OjU2LjY2NjY1NDU2NloiLCJpc3MiOiJhdXRoZW50aWNhdGlvbi5hbGxlbi1wcm9kIiwiaXN1IjoiIiwicHQiOiJJTlRFUk5BTF9VU0VSIiwic2lkIjoiOTZlOGM1YTktMWQyNS00MDc0LThiMjgtYWY3YjdjNDZkZTgxIiwidGlkIjoiYVVTc1c4R0kwM2R5UTBBSUZWbjkyIiwidHlwZSI6ImFjY2VzcyIsInVpZCI6Ild4NUlDR3VxM2tkQnpqTkpVdE1GQiJ9.oV5lRCqgf6ikrbpunFD7W6N8XqEwsbhOsfeBqPc57c0'
    }
    payload = json.dumps({
        "requests":[
            payload
        ]
    })
    bulk_upload_url = "https://api.allen-live.in/api/v1/learningMaterials/validate/bulk"

    response = requests.request("POST",bulk_upload_url,headers=headers,data=payload)
    if(response.status_code == 200):
    
        response = json.loads(response.text)
        # print(response)
        # # create(response,filename)
        # logging.info("valdate bulk called")
        return response
    else:
        logging.error("Error Ocuured in validate Bulk",)
        return None

def create(res,filename):
    url = f"https://api.allen-live.in/api/v1/learningMaterials/bulk_create"

    payload = json.dumps(
            {
                "learning_material":{
                    "name": "",
                    "type": "",
                    "taxonomies": [
                        {
                            "taxonomy_id": "",
                            "node_id": ""
                        }
                    ],
                    "language": "",
                    "content_stakeholders": {
                        "faculty_name": "",
                        "created_by": "pranavreddy",
                        "faculty_emp_id": ""
                    },
                    "stream": "",
                    "duration_minutes": 0,
                    "client_id": "Console",
                    "tenant_id": "",    #ALLEN
                    "session": "",
                    "center": "KOTA",
                    "center_id": "",  #centerid=facility_0KzO1wXO2r9apf0DRy4ex
                    "master_course": "",
                    "sub_type": "",
                    "learning_category": "",
                    "hashtags": []
                },
                "filename":""
        }
    )
    payload = json.loads(payload)
    # print(payload)
    res= res.get('data',{}).get('results')[0].get('validation_result')
    # print(res)
    payload['learning_material']['name'] = res['name']
    payload['learning_material']['type'] = res['type']
    payload['learning_material']['taxonomies'][0]['taxonomy_id'] = res['taxonomy_attributes']['taxonomy_id']
    payload['learning_material']['taxonomies'][0]['node_id'] = res['taxonomy_attributes']['topic']['node_id']
    payload['learning_material']['language'] = res['language']
    payload['learning_material']['stream'] = res['stream']
    payload['learning_material']['tenant_id'] = res['tenant_id']
    payload['learning_material']['session'] = res['session']
    payload['learning_material']['center_id'] = "fa_xFoPmDT8HQJEOVoaYMLao"  #res['center_id'] 
    payload['learning_material']['sub_type'] = res['sub_type']
    payload['learning_material']['learning_category'] = res['learning_category']
    payload['filename'] = filename
    # print(payload)

    headers = {
        'Content-Type':'application/json'
    }
    payload= json.dumps({
        "requests":[
            payload
        ],
        "draft":False
    })

    response = requests.request("POST",url,headers=headers,data=payload)
    # logging.info("create called ")
    if(response.status_code == 200):
        response = json.loads(response.text)
        # print(response['data']['materials'][0]["id"])
        return response['data']['materials'][0]["id"]
        # print(payload)
    else:
        logging.error("error occurred at create func")
        return None

def init_multipart_upload(id,file_format):

    url = f"https://api.allen-live.in/api/v1/learningMaterials/{id}/init_multipart_upload"
    payload = json.dumps({
        'file_format':file_format
    })

    headers = {
        'Content-Type':'application/json'
    }

    response = requests.request("POST",url,headers=headers,data=payload)
    if(response.status_code == 200):
        response = json.loads(response.text)
        # print("id is ",response['data']['upload_data']['upload_id'])
        # logging.info('multipart_upload called')
        return response['data']['upload_data']['upload_id']
    else:
        logging.error("Error at multipart_upload")
        return None
   
def upload_part(id,upload_id):
    url = f"https://api.allen-live.in/api/v1/learningMaterials/{id}/upload_part"

    payload = json.dumps({
        "part_number":1,
        "upload_id":upload_id
    })
    headers = {
        'Content-Type':'application/json'
    }
    response = requests.request("POST",url,headers=headers,data=payload)
    if(response.status_code == 200):
        response = json.loads(response.text)
        return response['data']['upload_data']['presigned_url']
    else:
        logging.error("Error occurerd at Upload part")
        return None
    
def upload_file(new_presigned_url, filename , file_format):
    payload = ""
    with open(filename, 'rb') as file:
        payload = file.read()

    headers = {
        'Content-Type': 'application/json'
    }
    response = requests.request("PUT", new_presigned_url, headers=headers, data=payload)
    
    if response.status_code > 299:
        logging.error("error at upload_file")
        return 
    else:
        etag = response.headers.get('ETag')
        return etag

def complete_upload(id,etag,upload_id):
    url = f"https://api.allen-live.in/api/v1/learningMaterials/{id}/complete_multipart_upload"

    payload = json.dumps({
        "upload_id":upload_id,
        "parts":[{
            "part_number":1,
            "etag":etag
        }]
    })
    headers = {
        'Content-Type': 'application/json'
    }

    response = requests.request("POST",url,headers=headers,data=payload)
    if(response.status_code == 200):
        response = json.loads(response.text)
        return response['data']
    else:
        logging.error("error at complete upload")
        return False

def delete_file(filename):
    if os.path.exists(filename):
                os.remove(filename)
                # print(f"File {filename} deleted successfully.")
    else:
        # print(f"File {filename} not found, unable to delete.")
        logging.error("unable to find file and not deleted")

def create_csv_file():
    csv_file = 'concept_card_id_mapping'
    with open(csv_file, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
    
        # Write the header
        writer.writerow(['old_concept_card_id', 'new_concept_card_id'])
    return csv_file

def upload_to_csv_file(csv_file_name,old_id,new_id):
    with open(csv_file_name, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
    
        # Write the header
        writer.writerow([old_id,new_id])



csv_file = 'class12_JEE.csv'
with open(csv_file,mode='r',encoding='utf-8') as file:
    csv_reader = csv.DictReader(file)
    csv_file = create_csv_file()
    i=1
    payloads = []
    for i in range(1,21):
        print(i)
        next(csv_reader)

    i=i+1    
    for row in csv_reader:
        if(i==22):
            break
        try:
            payload = csv_payload_template.copy()

            payload['concept_card_id'] = row['Concept Card ID']
            payload['concept_card_name'] = row['Concept Card Name']
            payload['super_topic'] = row['super_topic']
            payload['topic'] = row['topic']
            payload['sub_topic'] = row['sub_topic']
            payload['offline_center'] = row['Offline_center']
            payload['offline_stream'] = row['Offline_stream']
            payload['offline_subject'] = row['Offline_subject']
            payload['offline_super_topic'] = row['Offline_suptertopic']
            payload['offline_topic'] = row['Offline_topic']

            print(i,"     ",row["Concept Card Name"])
            # print(payload)

            presigned_url = get_presigned_url(row['Concept Card ID'])
            # print(presigned_url)
            filename = save_file(presigned_url)
            # print(filename)
            file_format = 'json'
            # get_material_formattype(row['Concept Card ID'])
            bulkresponse = validate_bulk(payload)
            # print(bulkresponse)
            id = create(bulkresponse,filename)
            upload_id = init_multipart_upload(id,file_format)
            new_presigned_url = upload_part(id,upload_id)
            etag = upload_file(new_presigned_url,filename,file_format)
            complete_upload(id,etag,upload_id)
            delete_file(filename)

            print(i," done")
        except Exception as e:
            logging.error(e)
            logging.error("at concept_card_id  ",row["Concept Card ID"])
        i=i+1
        

# get_material_formattype("dd850bed-12c1-4b81-974a-c725411d9cf2")
# get_presigned_url("dd850bed-12c1-4b81-974a-c725411d9cf2")
