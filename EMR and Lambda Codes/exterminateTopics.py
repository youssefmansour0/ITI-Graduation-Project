import boto3

s3 = boto3.client('s3')

def lambda_handler(event, context):
    bucket_name = 'iti-ecommerce-all'
    prefix = 'topics/'

    paginator = s3.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket_name, Prefix=prefix)

    deleted = 0
    for page in pages:
        if 'Contents' in page:
            delete_batch = [{'Key': obj['Key']} for obj in page['Contents']]
            s3.delete_objects(Bucket=bucket_name, Delete={'Objects': delete_batch})
            deleted += len(delete_batch)

    return {
        'statusCode': 200,
        'body': f'{deleted} objects deleted from {bucket_name}/{prefix}'
    }
