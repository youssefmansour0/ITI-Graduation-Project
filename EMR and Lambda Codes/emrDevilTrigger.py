import boto3

def lambda_handler(event, context):
    emr = boto3.client('emr', region_name='eu-west-1')

    response = emr.run_job_flow(
        Name='DevilTrigger',
        ReleaseLabel='emr-6.15.0',
        Applications=[{'Name':'Hadoop'},{'Name':'Spark'}],
        LogUri='s3://iti-ecommerce-all/emr-logs/',
        Instances={
            'InstanceGroups': [
                {
                    'Name':'Master',
                    'Market':'ON_DEMAND',
                    'InstanceRole':'MASTER',
                    'InstanceType':'r6in.xlarge',
                    'InstanceCount':1
                },
                {
                    'Name':'Core',
                    'Market':'ON_DEMAND',
                    'InstanceRole':'CORE',
                    'InstanceType':'r6in.xlarge',
                    'InstanceCount':1
                }
            ],
            'Ec2KeyName':'EMR-ALL',
            'KeepJobFlowAliveWhenNoSteps':False,
            'TerminationProtected':False,
            'Ec2SubnetId':'subnet-0d42a30e9f19eaab1'
        },
        JobFlowRole='AmazonEMR-InstanceProfile-20250801T212929',
        ServiceRole='arn:aws:iam::692738841222:role/service-role/AmazonEMR-ServiceRole-20250801T212946',
        VisibleToAllUsers=True,
        Tags=[{'Key':'for-use-with-amazon-emr-managed-policies','Value':'true'}],
        Steps=[{
            'Name':'Run Spark Job',
            'ActionOnFailure':'CONTINUE',
            'HadoopJarStep':{
                'Jar':'command-runner.jar',
                'Args':[
                    '/usr/lib/spark/bin/spark-submit',
                    '--deploy-mode','client',
                    '--master','yarn',
                    '--driver-cores','2',
                    '--driver-memory','4g',
                    '--conf','spark.driver.memoryOverhead=512m',
                    '--executor-cores','2',
                    '--executor-memory','4g',
                    '--conf','spark.executor.memoryOverhead=512m',
                    '--num-executors','2',
                    '--conf','spark.default.parallelism=8',
                    '--conf','spark.sql.shuffle.partitions=8',
                    '--conf','spark.serializer=org.apache.spark.serializer.KryoSerializer',
                    '--conf','spark.sql.adaptive.enabled=true',
                    '--conf','spark.sql.adaptive.coalescePartitions.enabled=true',
                    '--conf','spark.dynamicAllocation.enabled=false',
                    '--conf','spark.hadoop.fs.s3a.fast-upload=true',
                    '--conf','spark.sql.parquet.compression.codec=snappy',
                    '--conf','spark.sql.parquet.mergeSchema=false',
                    '--conf','spark.sql.parquet.filterPushdown=true',
                    '--conf','spark.sql.hive.convertMetastoreParquet=true',
                    '--conf','spark.hadoop.fs.s3a.aws.credentials.provider=software.amazon.awssdk.auth.credentials.InstanceProfileCredentialsProvider,com.amazonaws.auth.DefaultAWSCredentialsProviderChain',
                    's3a://iti-ecommerce-all/scripts/SPARK.py'
                ]
            }
        }]
    )

    return {
        'statusCode':200,
        'body':f'EMR cluster started: {response["JobFlowId"]}'
    }
