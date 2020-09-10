# ITIS Terms Importer
A python script that imports terms from https://www.itis.gov/ in an incremental manner, and then insert/update the terms into the pangaea database.

## Usage
To run the importer, please execute the following from the root directory. Please update config/import_template.ini with your database settings (username, password, host, port).
```
pip3 install -r requirements.txt
python3 import.py -c config/import.ini
```

