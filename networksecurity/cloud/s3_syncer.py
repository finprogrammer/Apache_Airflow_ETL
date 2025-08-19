
import os
class S3Sync:
    def sync_folder_to_s3(self,folder,aws_bucket_url):
        command = f"aws s3 sync {folder} {aws_bucket_url} "#just runs the command to sync the current folder and the s3 bucket
        os.system(command)

    def sync_folder_from_s3(self,folder,aws_bucket_url):
        command = f"aws s3 sync  {aws_bucket_url} {folder} "
        os.system(command)

