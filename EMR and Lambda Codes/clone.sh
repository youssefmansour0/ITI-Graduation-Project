# to be used in the aws cli to create an EMR cluster with Spark job
# This script assumes you have AWS CLI configured with the necessary permissions
aws emr create-cluster \
 --name "DevilTrigger" \
 --log-uri "s3://iti-ecommerce-all/emr-logs" \
 --release-label "emr-6.15.0" \
 --service-role "arn:aws:iam::692738841222:role/service-role/AmazonEMR-ServiceRole-20250801T212946" \
 --auto-terminate \
 --unhealthy-node-replacement \
 --ec2-attributes '{"InstanceProfile":"AmazonEMR-InstanceProfile-20250801T212929","EmrManagedMasterSecurityGroup":"sg-055d75a214ce61eee","EmrManagedSlaveSecurityGroup":"sg-0917b09f7cc055452","KeyName":"EMR-ALL","SubnetIds":["subnet-0d42a30e9f19eaab1"]}' \
 --tags 'for-use-with-amazon-emr-managed-policies=true' \
 --applications Name=Hadoop Name=Spark \
 --instance-groups '[{"InstanceCount":1,"InstanceGroupType":"MASTER","Name":"Master","InstanceType":"r6in.xlarge","EbsConfiguration":{"EbsBlockDeviceConfigs":[{"VolumeSpecification":{"VolumeType":"gp2","SizeInGB":32},"VolumesPerInstance":2}]}},{"InstanceCount":1,"InstanceGroupType":"CORE","Name":"Core","InstanceType":"r6in.xlarge","EbsConfiguration":{"EbsBlockDeviceConfigs":[{"VolumeSpecification":{"VolumeType":"gp2","SizeInGB":32},"VolumesPerInstance":2}]}}]' \
 --steps '[{"Name":"Run Spark Job","ActionOnFailure":"CONTINUE","Jar":"command-runner.jar","Properties":"","Args":["/usr/lib/spark/bin/spark-submit","--deploy-mode","client","--master","yarn","--driver-cores","2","--driver-memory","4g","--conf","spark.driver.memoryOverhead=512m","--executor-cores","2","--executor-memory","4g","--conf","spark.executor.memoryOverhead=512m","--num-executors","2","--conf","spark.default.parallelism=8","--conf","spark.sql.shuffle.partitions=8","--conf","spark.serializer=org.apache.spark.serializer.KryoSerializer","--conf","spark.sql.adaptive.enabled=true","--conf","spark.sql.adaptive.coalescePartitions.enabled=true","--conf","spark.dynamicAllocation.enabled=false","--conf","spark.hadoop.fs.s3a.fast-upload=true","--conf","spark.sql.parquet.compression.codec=snappy","--conf","spark.sql.parquet.mergeSchema=false","--conf","spark.sql.parquet.filterPushdown=true","--conf","spark.sql.hive.convertMetastoreParquet=true","--conf","spark.hadoop.fs.s3a.aws.credentials.provider=software.amazon.awssdk.auth.credentials.InstanceProfileCredentialsProvider,com.amazonaws.auth.DefaultAWSCredentialsProviderChain","s3a://iti-ecommerce-all/scripts/SPARK.py"],"Type":"CUSTOM_JAR"}]' \
 --scale-down-behavior "TERMINATE_AT_TASK_COMPLETION" \
 --region "eu-west-1"