import os
import time
import boto3
from botocore.exceptions import ClientError
import argparse

def upload_file_to_s3(bucket_name, file_name, object_name= None):
    # Connect to session
    session = boto3.Session(aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
                            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"))

    # Connect to resource
    s3 = session.resource('s3')

    # Connect to bucket
    bucket = s3.Bucket(bucket_name)

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = "netflix-recommendation/"+file_name
    else:
        object_name = object_name + "/" + file_name

    try:
        # Upload file
        bucket.upload_file(file_name, object_name)
    except ClientError as e:
        boto3.logging.error(e)
        return False
    return True

if __name__ == "__main__":
    # Start timer
    start_time = time.time()
    # Create the parser
    parser = argparse.ArgumentParser(description='Upload files to S3')

    # Add the arguments
    parser.add_argument('FileName', metavar='file_name', type=str, help='the name of the file')


    # Parse the arguments
    args = parser.parse_args()

    # Start timer
    start_time = time.time()

    # Call the function with the command-line arguments
    upload_file_to_s3(bucket_name="ns-data-resources-bucket", file_name=args.FileName)
    # End timer
    end_time = time.time()
    print(f"Total time: {end_time - start_time} seconds")