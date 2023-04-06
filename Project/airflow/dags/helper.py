import sys,os
import json
import pandas as pd
import numpy as np
import glob
from google.cloud import storage


PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
CWD = os.getcwd()
print('current dirrectory is :', CWD)
PATH_DATA = os.path.abspath(os.path.join(CWD,'data'))

companies = ['stedin', 'liander','enduris', 'enexis','westland-infra','rendo','coteq']
column_types_mapping = {'net_manager':'str',
                        'purchase_area':'str',
                        'street':'str',
                        'zipcode_from':'str',
                        'zipcode_from':'str',
                        'city':'category',
                        'num_connections':'Int16',
                        'delivery_perc':'float',
                        'perc_of_active_connections':'float',
                        'type_conn_perc':'float',
                        'type_of_connection':'str',
                        'annual_consume_lowtarif_perc':'float',
                         'annual_consume':'float',
                        'smartmeter_perc':'float'
                        }




def transformed_to_parquet(year):
    for company in companies:
        all_files = glob.glob(f"{PATH_DATA}/Electricity/{company}*.csv")
        all_files = list(map(os.path.abspath, all_files))
        #print(all_files)
        for file in all_files:
            if year in file:
                print('uploaded company: ',company, file)
                #print(f"adding column year {year} to {file}")
                df = pd.read_csv(file, index_col=None, header=0)
                df['year'] = pd.to_datetime(year)
                df = assign_types(df,column_types_mapping)
                #print()
                df.to_parquet(file.replace('.csv','.parquet'))

def assign_types(df:pd.DataFrame ,column_types_mapping:dict):
    """Assigning types to dataframe columns"""
    for k,v in column_types_mapping.items():
        try:
            if k in ['type_conn_perc']:
                df[k] = df[k].str.replace(',', '.')
        except:
            pass
        df = df.rename(columns={"ï»¿NETBEHEERDER": "net_manager"})
        try:
            df[k] = df[k].astype(v)
        except Exception as e:
            df[k] = np.nan
            print(e)
    return df

def upload_to_bucket(year):
    print(year)
    print()
    _path = f"{PATH_DATA}/Electricity/*{year}*.parquet"
    files = glob.glob(_path)
    for path_file in files:
        file = os.path.basename(path_file)
        print(file)
        object_name = f"{year}/{file}"
        upload_to_gcs(BUCKET, object_name, path_file)
    print()


def upload_to_gcs(bucket, object_name, local_file):
    """
    :param bucket: GCS bucket name
    :param object_name: target path & file-name
    :param local_file: source path & file-name
    :return:
    """
    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    # End of Workaround
    client = storage.Client()
    bucket = client.bucket(bucket)
    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)




