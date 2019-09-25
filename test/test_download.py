from io import BytesIO
from zipfile import ZipFile
from urllib.request import urlopen
import configparser
import argparse
import requests
import re
from datetime import datetime
from datetime import datetime, timedelta
import os
import shutil
import requests
import io
import zipfile
#def updateConfigFile(self):
    # write to config file
    #if self.last_date:
        #config.set('DATASOURCE', 'last_harvest_date', self.last_date)
        #with open(configFile, 'w+') as configfile:
            #config.write(configfile)
        #logging.info("Last Harvest Date Updated! :%s ", self.last_date)

def download_extract_zip(url):
    """
    Download a ZIP file and extract its contents in memory
    yields (filename, file-like object) pairs
    """
    response = requests.get(url)
    with zipfile.ZipFile(io.BytesIO(response.content)) as thezip:
        for zipinfo in thezip.infolist():
            with thezip.open(zipinfo) as thefile:
                yield zipinfo.filename, thefile

def main():
    itis_sql_url='https://www.itis.gov/downloads/itisSqlite.zip'
    itis_sql_downloaded_date = '091419'
    new_date = datetime.strptime(itis_sql_downloaded_date, '%m%d%y').date()
    #content = requests.get(itis_sql_url, headers={'if-modified-since': new_date.strftime('%a, %d %b %Y 23:59:59 GMT')})
    response  = requests.get(itis_sql_url)
    print(response .status_code)

    my_dir = r"D:\Download"
    with zipfile.ZipFile(io.BytesIO(response.content)) as zip_file:
        for member in zip_file.namelist():
            filename = os.path.basename(member)
            print(filename)
            if not filename:
                continue
            # copy file (taken from zipfile's extract)
            source = zip_file.open(member)
            target = open(os.path.join(my_dir, filename), "wb")
            with source, target:
                shutil.copyfileobj(source, target)



    #zf = ZipFile(BytesIO(content.content),'r')
    # for files in zf.namelist():
    #     if files.endswith(".sqlite"):
    #         print("File in zip: " + files)
    #         dir = files.split('/')[0]
    #         dt_match = re.search(r'\d{2}\d{2}\d{2}', dir)
    #         new_date = datetime.strptime(dt_match.group(), '%m%d%y').date()
    #         last_datetime = datetime.strptime(itis_sql_downloaded_date, '%m%d%y').date()
    #         print(new_date,last_datetime)
    #         if new_date > last_datetime:
    #             print('last_datetime', last_datetime)
    #             data = zf.read(files, my_dir)
    #             # update config file
    #             # proceed with imports
    #         else:
    #             print('No changes in SQLLite DB; Aborting import..')
    #             #ignore -> log
    #
    #
    # # find the first matching csv file in the zip:
    # match = [s for s in zf.namelist() if ".sqlite" in s][0]
    # print(match)

    # zname = "matty.shakespeare.tar.gz"
    # zfile = open(zname, 'wb')
    # zfile.write(resp.content)
    # zfile.close()
    #
    # file_dir_name = "my_python_files.zip"
    #
    # # opening the zip file in READ mode
    # with ZipFile(file_name, 'r') as zip:
    #     # printing all the contents of the zip file
    #     zip.printdir()
    #     # extracting all the files
    #     print('Extracting all the files now...')
    #     zip.extractall()
    #     print('Done!')

if __name__ == "__main__":
    main()