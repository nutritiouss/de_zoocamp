import os
import json

CWD = os.getcwd()
print('current dirrectory is :', CWD)
KAGGLE_JSON = os.path.abspath(os.path.join(CWD,'kaggle.json'))
with open(KAGGLE_JSON) as f:
    kaggle_cred = json.load(f)
print(kaggle_cred)
os.environ['KAGGLE_USERNAME'] = kaggle_cred['username']
os.environ['KAGGLE_KEY'] =  kaggle_cred['key']
from kaggle.api.kaggle_api_extended import KaggleApi


PATH_DATA = os.path.abspath(os.path.join(CWD,'data'))
def download_kaggle_dataset():
    api = KaggleApi()
    api.authenticate()
    print('start downloading dataset Netherlands energy')
    api.dataset_download_files('lucabasa/dutch-energy', path=PATH_DATA, unzip=False)