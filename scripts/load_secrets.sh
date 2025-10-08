#!/bin/bash
# scripts/load_secrets.sh

set -e

echo "Loading secrets from AWS Secrets Manager..."

RDS_SECRET=$(aws secretsmanager get-secret-value \
    --secret-id earthquake/rds/postgres \
    --region eu-north-1 \
    --query SecretString \
    --output text)

export RDS_USERNAME=$(echo $RDS_SECRET | jq -r .username)
export RDS_PASSWORD=$(echo $RDS_SECRET | jq -r .password)
export RDS_ENDPOINT=$(echo $RDS_SECRET | jq -r .host)
export RDS_PORT=$(echo $RDS_SECRET | jq -r .port)
export RDS_DATABASE=$(echo $RDS_SECRET | jq -r .dbname)

AWS_SECRET=$(aws secretsmanager get-secret-value \
    --secret-id earthquake/aws/credentials \
    --region eu-north-1 \
    --query SecretString \
    --output text)

export AWS_ACCESS_KEY_ID=$(echo $AWS_SECRET | jq -r .aws_access_key_id)
export AWS_SECRET_ACCESS_KEY=$(echo $AWS_SECRET | jq -r .aws_secret_access_key)
export AWS_REGION=$(echo $AWS_SECRET | jq -r .aws_region)
export S3_BUCKET_NAME=$(echo $AWS_SECRET | jq -r .s3_bucket_name)


# Generate .env file
cat > .env << EOF
AIRFLOW_UID=$(id -u)
_AIRFLOW_WWW_USER_USERNAME=airflow
_AIRFLOW_WWW_USER_PASSWORD=airflow

RDS_ENDPOINT=$RDS_ENDPOINT
RDS_PORT=$RDS_PORT
RDS_DATABASE=$RDS_DATABASE
RDS_USERNAME=$RDS_USERNAME
RDS_PASSWORD=$RDS_PASSWORD

AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID
AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY
AWS_REGION=$AWS_REGION
S3_BUCKET_NAME=$S3_BUCKET_NAME
EOF
