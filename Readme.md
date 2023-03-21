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
pip install --upgrade pip
pip install -r requirements.txt
cp config/example_config.ini config/config.ini
```

### Delploy Jupyter notebook
Deploy jupyter notebook and create new kernel
```
ipython kernel install --name "de_zoomcamp" --user
```
Use command ``jupyter-notebook&`` to run notebook. 

THe desciription of week will be in the corresponding directory or file:

<br>[1. Week_1](week_1/Readme_week_1.md)
<br>[2. Week_2](week_1/Readme_week_2.md)
<br>[3. Week_3](week_1/Readme_week_3.md)
<br>[4. Week_4](week_1/Readme_week_4.md)
<br>[5. Week_5](week_5%2Fhomework.ipynb)
<br>[6. Week_6](week_1/Readme_week_6.md)

[homework.ipynb]

[1. Week_1](week_1/Readme_week_1.md)
[1. Week_1](week_1/Readme_week_1.md)