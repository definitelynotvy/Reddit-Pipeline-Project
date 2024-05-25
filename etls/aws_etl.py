import boto3
from botocore.exceptions import NoCredentialsError, ClientError
from utils.constants import AWS_ACCESS_KEY_ID, AWS_ACCESS_KEY

def connect_to_s3():
    try:
        s3 = boto3.client(
            's3',
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_ACCESS_KEY
        )
        return s3
    except NoCredentialsError as e:
        print("Credentials not available", e)
    except Exception as e:
        print(e)

def create_bucket_if_not_exist(s3, bucket: str):
    try:
        response = s3.list_buckets()
        bucket_names = [b['Name'] for b in response['Buckets']]
        if bucket not in bucket_names:
            s3.create_bucket(Bucket=bucket)
            print("Bucket created")
        else:
            print("Bucket already exists")
    except ClientError as e:
        print(f"ClientError: {e}")
    except Exception as e:
        print(e)

def upload_to_s3(s3, file_path: str, bucket: str, s3_file_name: str):
    try:
        s3.upload_file(file_path, bucket, 'raw/' + s3_file_name)
        print('File uploaded to s3')
    except FileNotFoundError:
        print('The file was not found')
    except NoCredentialsError:
        print('Credentials not available')
    except ClientError as e:
        print(f"ClientError: {e}")
    except Exception as e:
        print(e)