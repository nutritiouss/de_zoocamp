# DE zoomcamp



## Deployment prerequsites
We will use native python environment and install packages manualy.
```
git clone ....
cd dezoomcamp

#If anaconda installed
conda deactivate
sudo apt-get install python3.10 python3.10-venv
/usr/bin/python3.10 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
cp config/example_config.ini config/config.ini
```

### Delploy Jupyter notebook
Deploy jupyter notebook and create new kernel
```
ipython kernel install --name "de_zoomcamp" --user
```
Use command ``jupyter-notebook&`` to run notebook. 

THe desciription of week will be in directories:
[1. Week_1](week_1/Readme_week_1.md)