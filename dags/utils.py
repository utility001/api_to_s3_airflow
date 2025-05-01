# import os

import awswrangler as wr
import boto3
import pandas as pd
import requests
# load_dotenv()
from airflow.models import Variable

# from dotenv import load_dotenv


def fetch_user_info(no: int = 10):
    """
    Fetch data from random user.me
    Return the results as a list
    """
    URL = f"https://randomuser.me/api/?results={no}"
    response = requests.get(URL, timeout=10)

    # Raise an exception if a request is unsuccessful
    response.raise_for_status()

    # Fetch all user profiles from the api response
    print("Extraction successful")
    return response.json()["results"]

def normalize_profiles(profiles):
    """
    Convert the returned profiles to dataframe and do some transformations
    """
    # convert to dataframe
    profiles_df = pd.json_normalize(profiles)

    # due to datatypes error while pushing to s3  bucket, just convert all columns to string datatype
    profiles_df = profiles_df.astype(str)

    print("Normalization Successful")
    return profiles_df


# Instantiate boto3 sesson
session = boto3.session.Session(
    # aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    # aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    # region_name=os.getenv("REGION_NAME")
    aws_access_key_id=Variable.get("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=Variable.get("AWS_SECRET_ACCESS_KEY"),
    region_name=Variable.get("REGION_NAME")
)

def load_df_to_s3(df):
    """
    Load the dataframe to s3 bucket
    """
    # print(f"BUCKET PATH: {Variable.get('S3_BUCKET_PATH')}")
    load = wr.s3.to_parquet(
        df=df,
        path=Variable.get("S3_BUCKET_PATH"),
        # path=os.getenv("S3_BUCKET_PATH"),
        boto3_session=session,
        dataset=True,
        mode="append"
    )

    print("Successfully written to %s", load["paths"])

def full_pipeline():
    """
    Extract, transform and load
    """
    extract = fetch_user_info()
    transfrom = normalize_profiles(extract)
    load_df_to_s3(transfrom)

    return "Finito bro"

# if __name__ == "__main__":
#     full_pipeline()