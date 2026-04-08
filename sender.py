import argparse
import dotenv
import os
import boto3

from tqdm import tqdm

dotenv.load_dotenv()

AWS_KEY = os.getenv("AWS_KEY")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_KEY")

class Sender:

    def __init__(self, bucket_name, bucket_folder):
        self.bucket_name = bucket_name
        self.bucket_folder = bucket_folder

        self.s3 = boto3.client('s3', 
                        aws_access_key_id=AWS_KEY, 
                        aws_secret_access_key=AWS_SECRET_KEY)

    def process_file(self, filename):
        file = filename.split("/")[-1]
        bucket_path = f"{self.bucket_folder}/{file}"

        try:
            self.s3.upload_file( filename, 
                            self.bucket_name,
                            bucket_path)
            os.remove(filename)
            return True
            
        except Exception as e:    
            print(f"Error uploading file: {e}")
            return False

    
    def process_folder(self, folder):
        files = [f for f in os.listdir(folder) if os.path.isfile(os.path.join(folder, f))]

        if not files:
            print(f"Nenhum arquivo encontrado em {folder}")
            return

        success = 0
        failed = 0

        for file in tqdm(files):
            result = self.process_file(f"{folder}/{file}")
            if result:
                success += 1
            else:
                failed += 1

        print(f"Upload concluído: {success} ok, {failed} falhou")

parser = argparse.ArgumentParser()
parser.add_argument("--bucket", type=str, required=True)
parser.add_argument("--bucket_path", type=str, required=True)
parser.add_argument("--folder", type=str, required=True)
args = parser.parse_args()

send = Sender(args.bucket, args.bucket_path)
send.process_folder(args.folder)