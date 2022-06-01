import sys

import boto3
import os
from dotenv import load_dotenv
import json

def upload_s3(cbsacode, data_dict):
    print("Uploading geojson for {}".format(cbsacode))

    try:
        #Creating Session With Boto3.
        load_dotenv()

        aws_accesskey = os.getenv("AWS_ACCESSKEY")
        aws_secretkey = os.getenv("AWS_SECRETKEY")

        session = boto3.Session(
            aws_access_key_id=aws_accesskey,
            aws_secret_access_key=aws_secretkey
        )

        #Creating S3 Resource From the Session.
        s3 = session.resource('s3')
        txt_data = json.dumps(data_dict)
        object = s3.Object('scopeout', 'tracts-geojson/{}.json'.format(cbsacode))
        result = object.put(Body=txt_data)

        print("s3 upload result: ", result)
    except Exception as e:
        print(e)
        sys.exit()