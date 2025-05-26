# MinIO environment variables
MINIO_ROOT_USER='minioadmin'
MINIO_ROOT_PASSWORD='minioadmin'
MINIO_VOLUMES='/mnt/data'
MINIO_OPTS='--console-address :9001'

# Pyiceberg config
ICEBERG_JAR = 'iceberg-spark-runtime-3.4_2.12-1.4.3.jar'
HADOOP_JAR = 'hadoop-aws-3.3.4.jar'
AWS_JAVA_SDK_JAR = 'aws-java-sdk-bundle-1.12.262.jar'