import os
import logging
import boto3
from dotenv import load_dotenv
from pathlib import Path


# Download all files in a bucket with a certain file name
# and name them video_1.mp4, video_2.mp4, etc.
def main():
    # Set up logging
    logging.basicConfig(level=logging.INFO)

    # Load environment variables
    load_dotenv()

    bucket_name = os.getenv('bucket_name')  # Bucket name to download from
    file_name = os.getenv('file_name')  # File name to download

    if bucket_name is None or file_name is None:
        logging.critical('bucket_name or file_name not found in .env')
        exit(1)

    if '.' not in file_name:
        logging.critical('file_name must have a file extension')
        exit(1)

    # Initialize s3 client
    s3 = boto3.client('s3')

    # Get all buckets in s3
    bucket_metadata = s3.list_buckets()
    bucket_names = [bucket['Name'] for bucket in bucket_metadata['Buckets']]
    logging.debug(bucket_names)

    # Check if bucket exists
    if bucket_name not in bucket_names:
        logging.critical('Bucket not found')
        exit(1)

    # Get all objects with paths that end with file_name in bucket
    bucket = boto3.resource('s3').Bucket(bucket_name)
    filenames = [obj.key for obj in bucket.objects.all() if obj.key.endswith(file_name)]
    logging.debug(filenames)

    # Check if there are any files
    if len(filenames) == 0:
        logging.critical('No files found')
        exit(1)

    # Check if the folder exists
    if not os.path.exists(bucket.name):
        os.makedirs(bucket.name)

    # Setup file type and extension (save as <file_type>_<index>.<file_ext>)
    file_ext = file_name.split('.')[-1]  # Get file extension
    file_type = 'unknown'  # Default file type
    if file_ext in ['mp4', 'mov', 'avi']:
        file_type = 'video'
    elif file_ext in ['jpg', 'png', 'jpeg']:
        file_type = 'image'
    elif file_ext in ['mp3', 'wav']:
        file_type = 'audio'
    elif file_ext in ['txt', 'docx', 'pdf']:
        file_type = 'text'

    # Download all files in filenames
    for [filename, index] in zip(filenames, range(len(filenames))):
        bucket.download_file(filename, str(Path(bucket.name).joinpath(f'{file_type}_{index}.{file_ext}')))

    logging.info('Done')


if __name__ == '__main__':
    main()
