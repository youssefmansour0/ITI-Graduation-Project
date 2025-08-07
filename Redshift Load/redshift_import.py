import boto3
import os
import time

# Initialize Redshift Data API client
redshift = boto3.client('redshift-data')

# --- Environment variables ---
WORKGROUP_NAME = os.environ['WORKGROUP_NAME']     
DATABASE = os.environ['DATABASE']                
SCHEMA = os.environ['SCHEMA']                     
IAM_ROLE = os.environ['IAM_ROLE']            
S3_BUCKET = os.environ['S3_BUCKET']               
SECRET_ARN = os.environ['SECRET_ARN']             # ✅ New: Redshift credentials from Secrets Manager

# --- Table mapping ---
TABLE_MAPPINGS = {
    'rejected':'rejected'
}

def build_copy_sql(s3_folder, redshift_table):
    s3_path = f"s3://{S3_BUCKET}/DWH/{s3_folder}/"
    return f"""
        COPY {SCHEMA}.{redshift_table}
        FROM '{s3_path}'
        IAM_ROLE '{IAM_ROLE}'
        FORMAT AS PARQUET;
    """

def check_statement_status(statement_id):
    try:
        response = redshift.describe_statement(Id=statement_id)
        status = response['Status']
        
        result = {
            'statement_id': statement_id,
            'status': status,
            'created_at': str(response.get('CreatedAt', '')),
            'updated_at': str(response.get('UpdatedAt', ''))
        }
        
        if status == 'FINISHED':
            result['rows_affected'] = response.get('ResultRows', 0)
            result['duration_ms'] = response.get('Duration', 0)
        elif status == 'FAILED':
            result['error'] = response.get('Error', 'Unknown error')
        
        return result
    except Exception as e:
        return {
            'statement_id': statement_id,
            'status': 'CHECK_ERROR',
            'error': str(e)
        }

def lambda_handler(event, context):
    if event.get('action') == 'check_status':
        statement_ids = event.get('statement_ids', [])
        
        if not statement_ids:
            return {
                'statusCode': 400,
                'error': 'No statement_ids provided for status check'
            }
        
        print(f"Checking status for {len(statement_ids)} statements...")
        status_results = []
        
        for stmt_id in statement_ids:
            status_result = check_statement_status(stmt_id)
            status_results.append(status_result)
            print(f"Statement {stmt_id}: {status_result['status']}")
            
            if status_result['status'] == 'FAILED':
                print(f"ERROR: {status_result.get('error', 'Unknown')}")

        summary = {
            'total': len(status_results),
            'finished': len([r for r in status_results if r['status'] == 'FINISHED']),
            'failed': len([r for r in status_results if r['status'] == 'FAILED']),
            'running': len([r for r in status_results if r['status'] in ['SUBMITTED', 'PICKED', 'STARTED']])
        }
        
        return {
            'statusCode': 200,
            'action': 'status_check',
            'summary': summary,
            'results': status_results
        }

    # --- SUBMIT COPY Statements ---
    print("=== SUBMITTING REDSHIFT COPY OPERATIONS ===")
    results = []
    statement_ids = []

    for s3_folder, redshift_table in TABLE_MAPPINGS.items():
        sql = build_copy_sql(s3_folder, redshift_table)
        
        print(f"Submitting COPY for: {s3_folder} -> {redshift_table}")
        print(f"S3 Path: s3://{S3_BUCKET}/DWH/{s3_folder}/")
        
        try:
            response = redshift.execute_statement(
                WorkgroupName=WORKGROUP_NAME,
                Database=DATABASE,
                SecretArn=SECRET_ARN,  # ✅ Use Secrets Manager for credentials
                Sql=sql,
                WithEvent=True
            )
            
            statement_id = response['Id']
            statement_ids.append(statement_id)
            
            results.append({
                "s3_folder": s3_folder,
                "redshift_table": redshift_table,
                "statement_id": statement_id,
                "status": "SUBMITTED",
                "s3_path": f"s3://{S3_BUCKET}/DWH/{s3_folder}/"
            })
            
            print(f"✓ Submitted successfully: {statement_id}")
            
        except Exception as e:
            error_msg = str(e)
            results.append({
                "s3_folder": s3_folder,
                "redshift_table": redshift_table,
                "error": error_msg,
                "status": "SUBMIT_FAILED"
            })
            print(f"✗ Failed to submit: {error_msg}")

    print(f"=== SUBMITTED {len(statement_ids)} COPY STATEMENTS ===")
    
    # Wait and check initial status
    if statement_ids:
        print("Waiting 10 seconds before checking initial status...")
        time.sleep(10)
        
        print("=== CHECKING INITIAL STATUS ===")
        for stmt_id in statement_ids:
            status = check_statement_status(stmt_id)
            print(f"Statement {stmt_id}: {status['status']}")
            if status['status'] == 'FAILED':
                print(f"  ERROR: {status.get('error', 'Unknown')}")

    return {
        'statusCode': 200,
        'summary': {
            'total_submitted': len(statement_ids),
            'total_failed_submit': len(results) - len(statement_ids)
        },
        'statement_ids': statement_ids,
        'results': results,
        'next_steps': {
            'message': 'COPY statements submitted. Check status with the payload below.',
            'check_status_payload': {
                'action': 'check_status',
                'statement_ids': statement_ids
            }
        }
    } 