# pg_itis_importer
ITIS Terms Importer
https://www.itis.gov/

## Usage
To run the importer, please execute the following from the root directory. Please update config/import_template.ini with your database settings (username, password, host, port).
```
pip3 install -r requirements.txt
python3 import.py -c import.ini
```

