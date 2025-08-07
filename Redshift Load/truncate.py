import boto3
import os
import logging

# Initialize logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize Redshift Data API client
redshift = boto3.client('redshift-data')

# Environment variables
WORKGROUP_NAME = os.environ['WORKGROUP_NAME']
DATABASE = os.environ['DATABASE']
SCHEMA = os.environ['SCHEMA']
SECRET_ARN = os.environ['SECRET_ARN']

REJECTED_SCHEMA = 'rejected'  # schema for rejected tables

def get_table_list(schema_name):
    """Fetch all tables in the specified schema"""
    try:
        response = redshift.list_tables(
            WorkgroupName=WORKGROUP_NAME,
            Database=DATABASE,
            SecretArn=SECRET_ARN,
            SchemaPattern=schema_name
        )
        return [table['name'] for table in response.get('Tables', [])]
    except Exception as e:
        logger.error(f"Error fetching table list for schema {schema_name}: {str(e)}")
        raise

def truncate_table(schema_name, table_name):
    """Execute TRUNCATE statement for a single table"""
    sql = f"TRUNCATE TABLE {schema_name}.{table_name};"
    try:
        response = redshift.execute_statement(
            WorkgroupName=WORKGROUP_NAME,
            Database=DATABASE,
            SecretArn=SECRET_ARN,
            Sql=sql
        )
        return {
            "schema": schema_name,
            "table": table_name,
            "status": "TRUNCATED",
            "statement_id": response['Id']
        }
    except Exception as e:
        return {
            "schema": schema_name,
            "table": table_name,
            "status": "ERROR",
            "error": str(e)
        }

def truncate_tables_in_schema(schema_name):
    """Fetch tables and truncate all tables in given schema"""
    tables = get_table_list(schema_name)
    logger.info(f"Found {len(tables)} tables in schema {schema_name}")
    results = []
    for table in tables:
        logger.info(f"Truncating table: {schema_name}.{table}")
        result = truncate_table(schema_name, table)
        results.append(result)
        if result['status'] == 'ERROR':
            logger.error(f"Error truncating {schema_name}.{table}: {result['error']}")
        else:
            logger.info(f"Successfully truncated {schema_name}.{table}")
    return results

def lambda_handler(event, context):
    try:
        # Truncate tables in the main schema
        main_results = truncate_tables_in_schema(SCHEMA)

        # Truncate tables in the rejected schema
        rejected_results = truncate_tables_in_schema(REJECTED_SCHEMA)

        # Summarize results
        all_results = main_results + rejected_results
        success_count = len([r for r in all_results if r['status'] == 'TRUNCATED'])
        error_count = len([r for r in all_results if r['status'] == 'ERROR'])

        return {
            'statusCode': 200,
            'schemas_truncated': [SCHEMA, REJECTED_SCHEMA],
            'total_tables': len(all_results),
            'successfully_truncated': success_count,
            'failed_truncations': error_count,
            'results': all_results
        }

    except Exception as e:
        logger.error(f"Lambda execution failed: {str(e)}")
        return {
            'statusCode': 500,
            'error': str(e)
        }
