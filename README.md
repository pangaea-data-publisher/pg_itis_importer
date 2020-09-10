# ITIS Terms Importer
A python script that imports terms from https://www.itis.gov/ and insert the terms into the pangaea database.

## Usage
To run the importer, please execute the following from the root directory. Please update config/import_template.ini with your database settings (username, password, host, port).
```
pip3 install -r requirements.txt
python3 import.py -c import.ini
```

