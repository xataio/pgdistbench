#!/bin/bash

set -e
set -o pipefail

usage() {
    echo "Usage: $0 -r <aws-region> -i <image-name>[:<tag>] [-e <ecr-image-name>] [-p <aws-profile>] [aws-account-id]"
    echo "  -r: AWS region"
    echo "  -i: Docker image name and optional tag (e.g., benchdriver:latest)"
    echo "  -e: (optional) ECR image name. Defaults to the local image name."
    echo "  -p: (optional) AWS profile to use."
    echo "  aws-account-id: (optional) AWS account ID. Defaults to \$AWS_ACCOUNT_ID."
    exit 1
}

AWS_REGION="us-east-1"
IMAGE_SPEC="benchdriver:latest"
ECR_IMAGE_NAME=""
AWS_PROFILE=""

while getopts ":r:i:e:p:" opt; do
  case ${opt} in
    r )
      AWS_REGION=$OPTARG
      ;;
    i )
      IMAGE_SPEC=$OPTARG
      ;;
    e )
      ECR_IMAGE_NAME=$OPTARG
      ;;
    p )
      AWS_PROFILE=$OPTARG
      ;;
    \? )
      usage
      ;;
    : )
      echo "Option -$OPTARG requires an argument" 1>&2
      usage
      ;;
  esac
done
shift $((OPTIND -1))

# Get AWS_ACCOUNT_ID from argument or environment variable
if [ "$#" -eq 1 ]; then
    AWS_ACCOUNT_ID=$1
elif [ -z "${AWS_ACCOUNT_ID}" ]; then
    echo "Error: AWS account ID must be provided as an argument or via the AWS_ACCOUNT_ID environment variable."
    usage
fi

if [ -z "${AWS_REGION}" ] || [ -z "${IMAGE_SPEC}" ]; then
    usage
fi

# Split image name and tag
if [[ "${IMAGE_SPEC}" == *":"* ]]; then
  IMAGE_NAME="${IMAGE_SPEC%:*}"
  IMAGE_TAG="${IMAGE_SPEC#*:}"
else
  IMAGE_NAME="${IMAGE_SPEC}"
  IMAGE_TAG="latest"
fi

# Use ECR_IMAGE_NAME if provided, otherwise fallback to local IMAGE_NAME
FINAL_ECR_IMAGE_NAME="${ECR_IMAGE_NAME:-$IMAGE_NAME}"

ECR_REPO="${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/${FINAL_ECR_IMAGE_NAME}"

echo "Logging in to AWS ECR..."
aws ecr get-login-password --region "${AWS_REGION}" ${AWS_PROFILE:+--profile "${AWS_PROFILE}"} | docker login --username AWS --password-stdin "${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com"

echo "Tagging image..."
docker tag "${IMAGE_NAME}:${IMAGE_TAG}" "${ECR_REPO}:${IMAGE_TAG}"

echo "Pushing image to ECR..."
docker push "${ECR_REPO}:${IMAGE_TAG}"

echo "Image pushed successfully to ${ECR_REPO}:${IMAGE_TAG}"

