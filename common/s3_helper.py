import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError
from config.config import config
class S3Helper:
    def __init__(self, aws_access_key_id=None, aws_secret_access_key=None, region_name=None):
        """
        Initialize the S3Helper class with AWS credentials and region.
        """
        self.aws_access_key_id = config.AWS_ACCESS_KEY
        self.aws_secret_access_key = config.AWS_SECRET_KEY
        self.region_name = config.AWS_REGION
        self.s3_client = self._initialize_client()

    def _initialize_client(self):
        """
        Initialize the S3 client using provided credentials.
        """
        try:
            if self.aws_access_key_id and self.aws_secret_access_key:
                return boto3.client(
                    's3',
                    aws_access_key_id=self.aws_access_key_id,
                    aws_secret_access_key=self.aws_secret_access_key,
                    region_name=self.region_name
                )
            else:
                return boto3.client('s3')  # Use default credentials
        except (NoCredentialsError, PartialCredentialsError) as e:
            print(f"Error initializing S3 client: {e}")
            raise

    def upload_file(self, file_name, bucket_name, object_name=None):
        """
        Upload a file to an S3 bucket.
        :param file_name: File to upload
        :param bucket_name: Bucket to upload to
        :param object_name: S3 object name. If not specified, file_name is used
        :return: True if file was uploaded, else False
        """
        if object_name is None:
            object_name = file_name

        try:
            self.s3_client.upload_file(file_name, bucket_name, object_name)
            print(f"File {file_name} uploaded to {bucket_name}/{object_name}")
            return True
        except Exception as e:
            print(f"Error uploading file: {e}")
            return False

    def download_file(self, bucket_name, object_name, file_name):
        """
        Download a file from an S3 bucket.
        :param bucket_name: Bucket to download from
        :param object_name: S3 object name
        :param file_name: File to save the object to
        :return: True if file was downloaded, else False
        """
        try:
            self.s3_client.download_file(bucket_name, object_name, file_name)
            print(f"File {object_name} downloaded from {bucket_name} to {file_name}")
            return True
        except Exception as e:
            print(f"Error downloading file: {e}")
            return False

    def list_files(self, bucket_name):
        """
        List files in an S3 bucket.
        :param bucket_name: Bucket name
        :return: List of file names in the bucket
        """
        try:
            response = self.s3_client.list_objects_v2(Bucket=bucket_name)
            if 'Contents' in response:
                files = [item['Key'] for item in response['Contents']]
                print(f"Files in bucket {bucket_name}: {files}")
                return files
            else:
                print(f"No files found in bucket {bucket_name}")
                return []
        except Exception as e:
            print(f"Error listing files: {e}")
            return []

    def delete_file(self, bucket_name, object_name):
        """
        Delete a file from an S3 bucket.
        :param bucket_name: Bucket name
        :param object_name: S3 object name
        :return: True if file was deleted, else False
        """
        try:
            self.s3_client.delete_object(Bucket=bucket_name, Key=object_name)
            print(f"File {object_name} deleted from bucket {bucket_name}")
            return True
        except Exception as e:
            print(f"Error deleting file: {e}")
            return False