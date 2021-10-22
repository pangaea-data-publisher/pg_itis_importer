#!/usr/bin/python
import configparser
import argparse
import pandas as pd
import datetime
import sql_itis
import logging
import os
import requests
from zipfile import ZipFile, BadZipfile
import re
from io import BytesIO
import xml.etree.ElementTree as ET

def main():
    global args
    global configParser
    global last_change_date_str
    global itis_db_file
    ap = argparse.ArgumentParser()
    ap.add_argument("-c", "--config", required=True, help="Path to import.ini config file")
    args = ap.parse_args()
    configParser = configparser.ConfigParser()
    configParser.read(args.config)

    prev_last_change_date = configParser.get('INPUT', 'itis_last_change_date')
    itis_sql_url = configParser.get('INPUT', 'itis_sql_url')
    itis_db_file = configParser.get('INPUT', 'pgitis_sql')

    #check if harvest should proceed or not
    prev_last_change_date = datetime.datetime.strptime(prev_last_change_date, "%Y-%m-%d").date()
    logger.debug("Requesting last update date from ITIS REST...")
    lastdt_req = requests.get('https://www.itis.gov/ITISWebService/services/ITISService/getLastChangeDate')
    root = ET.fromstring(lastdt_req.content)
    last_change_date = root.findall('./{http://itis_service.itis.usgs.gov}return/{http://metadata.itis_service.itis.usgs.gov/xsd}updateDate')[0].text
    last_change_date_str = re.search(r'\d{4}-\d{2}-\d{2}', last_change_date)
    last_change_date = datetime.datetime.strptime(last_change_date_str.group(), "%Y-%m-%d").date()
    if last_change_date > prev_last_change_date:
        logger.debug("Downloading ITIS zip file...")
        #dowbload latest sqlite and then proceed, and finally update new date in the log
        #targetfile = itis_db_file.rsplit('\\',1)[1] # 'data\ITIS.sqlite'
        isExtracted = extractWriteSQLLite(itis_sql_url,itis_db_file)
        if isExtracted:
            harvestTerms()
            configParser.set('INPUT', 'itis_last_change_date', last_change_date_str.group())
            with open(args.config, 'w+') as configfile:
                configParser.write(configfile)
            logger.debug("Last Harvest Date Updated! :%s ", last_change_date_str.group())
    else:
        #cancel harvesting
        logger.debug('No changes in ITIS file, harvest is aborted!')

def harvestTerms():
    global itis_vernacular_prefix
    global term_dict
    global now_dt
    global id_user_created_updated

    itis_lsid_pfx = configParser.get('INPUT', 'lsid_itis_prefix')
    id_terminology = configParser.getint('INPUT', 'id_terminology')
    id_user_created_updated = configParser.getint('INPUT', 'id_user_created_updated')
    id_term_category = configParser.getint('INPUT', 'id_term_category')
    itis_uri_prefix = configParser.get('INPUT', 'itis_uri_prefix')
    id_term_status_accepted = configParser.getint('INPUT', 'id_term_status_accepted')
    id_term_status_notaccepted = configParser.getint('INPUT', 'id_term_status_notaccepted')
    has_broader_term_pk = configParser.get('INPUT', 'has_broader_term_pk')
    is_synonym_of_pk = configParser.get('INPUT', 'is_synonym_of_pk')
    has_attribute_pk = configParser.get('INPUT', 'has_attribute_pk')
    itis_vernacular_prefix = configParser.get('INPUT', 'itis_vernacular_prefix')
    now_dt = datetime.datetime.utcnow()

    pg_user = configParser.get('DB', 'pangaea_db_user')
    pg_pwd = configParser.get('DB', 'pangaea_db_pwd')
    pg_db = configParser.get('DB', 'pangaea_db_db')
    pg_port = configParser.get('DB', 'pangaea_db_port')
    pg_localhost = configParser.get('DB', 'pangaea_db_host')

    tblterm_cols_insert = ['name', 'datetime_created', 'datetime_updated', 'description', 'semantic_uri', 'uri', 'id_term_category',
     'id_term_status', 'id_terminology', 'id_user_created', 'id_user_updated', 'datetime_last_harvest', 'id_term']
    tblterm_cols_update = ['name', 'datetime_created', 'datetime_updated', 'description', 'semantic_uri', 'uri',
                    'id_term_category','id_term_status', 'id_terminology', 'id_user_updated', 'datetime_last_harvest','id_term']

    #------ Initialize DB & Term Table Params
    sqlExec = sql_itis.SQLExecutor()
    sqlExec.setLogger(logger)
    sqlExec.setDBParams(pg_user,pg_pwd,pg_db,pg_localhost,pg_port,itis_db_file)
    sqlExec.setTermParams(itis_lsid_pfx,id_terminology,id_user_created_updated,id_term_category,itis_uri_prefix,id_term_status_accepted,id_term_status_notaccepted,
                          has_broader_term_pk,is_synonym_of_pk,has_attribute_pk,itis_vernacular_prefix)
    #conn = sqlExec.create_pg_connection()
    #if conn:
        #print('Connected!')

    #------- import all itis db
    df_itis = sqlExec.select_itis_taxonomic_units()
    df_itis['datetime_created'] = pd.to_datetime(df_itis['datetime_created'])
    df_itis['update_date'] = pd.to_datetime(df_itis['update_date'])
    df_itis['semantic_uri'] = itis_lsid_pfx + df_itis['tsn'].astype(str)
    df_itis['uri'] = itis_uri_prefix + df_itis['tsn'].astype(str)
    df_itis.loc[(df_itis.id_term_status == 'accepted') | (df_itis.id_term_status == 'valid'), 'id_term_status'] = id_term_status_accepted
    df_itis.loc[(df_itis.id_term_status == 'not accepted') | (df_itis.id_term_status == 'invalid'), 'id_term_status'] = id_term_status_notaccepted
    # Replacing Pandas or Numpy Nan with a None to use with Postgres
    df_itis = df_itis.where((pd.notnull(df_itis)), None)
    logger.debug('ITIS (Valid,Invalid) TSNs: %s' % df_itis.tsn.nunique())

    # ------- clean existing terms in pg:tbl:term
    df_clean = sqlExec.select_sql_pangaea_terms('term', ['id_term', 'semantic_uri'])
    df_clean = df_clean[df_clean.semantic_uri.notnull()]
    df_clean = df_clean[~df_clean.semantic_uri.str.startswith(itis_lsid_pfx)]
    df_clean = df_clean[~df_clean.semantic_uri.str.contains(':vern_id:')]
    if (len(df_clean) > 0):
        logger.debug('Cleaning existing PANG-ITIS TERMS %s' % df_clean.shape[0])
        df_clean['semantic_uri'] = df_clean['semantic_uri'].apply(lambda x: "{}{}".format(itis_lsid_pfx, x))
        df_clean = pd.merge(df_clean, df_itis, on=['semantic_uri'], how='left')
        df_clean = df_clean.rename(columns={"update_date": "datetime_last_harvest"})
        df_clean['datetime_updated'] = now_dt
        df_clean['id_term_category'] = id_term_category
        df_clean['id_terminology'] = id_terminology
        df_clean['id_user_created'] = id_user_created_updated
        df_clean['id_user_updated'] = id_user_created_updated
        df_clean['id_term'] = df_clean['id_term'].astype(int)
        df_clean = df_clean[tblterm_cols_update]
        sqlExec.batch_update_terms(df_clean)
    else:
        logger.debug('Cleaning existing PANG-ITIS TERMS : SKIPPED ')
    del df_clean

    # -------incremental updates of terms
    select_cols = ['id_term', 'semantic_uri', 'datetime_updated', 'datetime_last_harvest']
    df_update = sqlExec.select_sql_pangaea_terms('term', select_cols)
    df_update = df_update[df_update.semantic_uri.notnull()]
    df_update = pd.merge(df_itis, df_update, on=['semantic_uri'], how='left')
    df_update = df_update[df_update.datetime_last_harvest.notnull()]  # all existing terms have harvest date, except grammar terms
    df_update['datetime_last_harvest'] = pd.to_datetime(df_update['datetime_last_harvest'])
    df_update['update_date'] = pd.to_datetime(df_update['update_date'])
    # 'update_date' from ITIS,  pang:datetime_last_harvest with take 'update_date' from itis during new insertions
    df_update = df_update[df_update.datetime_last_harvest < df_update.update_date]
    if len(df_update) > 0:
        logger.debug('Updating existing PANG-ITIS TERMS : %s' % df_update.shape[0])
        df_update = df_update[['name', 'datetime_created', 'update_date', 'description', 'semantic_uri', 'uri', 'id_term_status','id_term']]
        df_update = df_update.rename(columns={"update_date": "datetime_last_harvest"})
        df_update['datetime_updated'] = now_dt
        df_update['id_term_category'] = id_term_category
        df_update['id_terminology'] = id_terminology
        #df_update['id_user_created'] = id_user_created_updated => CHECK!!
        df_update['id_user_updated'] = id_user_created_updated
        df_update['id_term'] = df_update['id_term'].astype(int)
        # rerrange columns for update
        df_update = df_update[tblterm_cols_update]
        sqlExec.batch_update_terms(df_update)
    else:
        logger.debug('Updating existing PANG-ITIS TERMS : SKIPPED ')
    del df_update

    # -------insert new terms
    df_insert = sqlExec.select_sql_pangaea_terms('term', select_cols)
    df_insert = pd.merge(df_itis, df_insert, on=['semantic_uri'], how='left')
    df_insert = df_insert[df_insert.datetime_last_harvest.isnull()]  # all terms in db has datetime_last_harvest
    # 2019-09-18 harvest all terms regardless of their status, i.e., accepted or not accepted
    #df_insert = df_insert[df_insert.id_term_status == id_term_status_accepted] # only insert valid terms
    if len(df_insert) > 0:
        logger.debug('Inserting new valid ITIS TERMS :%s' % df_insert.shape[0])
        df_insert = df_insert[['name', 'datetime_created', 'update_date', 'description', 'semantic_uri', 'uri', 'id_term_status']]
        df_insert = df_insert.rename(columns={"update_date": "datetime_last_harvest"})
        df_insert['datetime_updated'] = now_dt
        df_insert['id_term_category'] = id_term_category
        df_insert['id_terminology'] = id_terminology
        df_insert['id_user_created'] = id_user_created_updated
        df_insert['id_user_updated'] = id_user_created_updated
        max_term_pk = sqlExec.get_max_idterm() + 1
        df_insert['id_term'] = range(max_term_pk, max_term_pk + len(df_insert))
        df_insert = df_insert[tblterm_cols_insert]
        sqlExec.batch_insert_new_terms(df_insert, 'term')
    else:
        logger.debug('Inserting new ITIS TERMS : SKIPPED')
    del df_insert

    ## --------- Get semantic_uri_list from pangaea term table
    select_cols1 = ['semantic_uri']
    dfterm1 = sqlExec.select_sql_pangaea_terms('term', select_cols1)
    dfterm1 = dfterm1.dropna(subset=['semantic_uri'])
    semantic_uri_list = dfterm1.semantic_uri.tolist()

    # ---- main vernacular df
    df_vern = sqlExec.select_vernaculars()
    df_vern['semantic_uri_tsn']= df_vern['tsn'].apply(lambda x: "{}{}".format(itis_lsid_pfx, x))
    #only used rows that have semantic_uris in the pangaea db (some vernaculars may have relations to terms which are invalid)
    df_vern = df_vern[df_vern.semantic_uri_tsn.isin(semantic_uri_list)]
    df_vern['semantic_uri'] = df_vern.apply(lambda x: itis_vernacular_prefix.format(x['tsn'], x['vern_id']), axis=1)
    df_vern = df_vern.drop(columns=['semantic_uri_tsn'])

    # -------update vernaculars new terms
    select_cols = ['id_term', 'semantic_uri', 'datetime_updated', 'datetime_last_harvest']
    df_pang = sqlExec.select_sql_pangaea_terms('term', select_cols)
    df_pang = df_pang[df_pang.semantic_uri.notnull()]
    df_vern_update = pd.merge(df_vern, df_pang, on=['semantic_uri'], how='left')
    df_vern_update = df_vern_update[df_vern_update.datetime_last_harvest.notnull()]  # all existing terms have harvest date, except grammar terms
    df_vern_update['datetime_last_harvest'] = pd.to_datetime(df_vern_update['datetime_last_harvest'])
    df_vern_update['update_date'] = pd.to_datetime(df_vern_update['update_date'])
    df_vern_update = df_vern_update[df_vern_update.datetime_last_harvest < df_vern_update.update_date]
    if len(df_vern_update) > 0:
        logger.debug('Updating existing Vernacular-ITIS TERMS : %s' % df_vern_update.shape[0])
        df_vern_update = df_vern_update[['name', 'update_date', 'semantic_uri', 'update_date']]
        df_vern_update = df_vern_update.rename(columns={"update_date": "datetime_last_harvest"})
        df_vern_update['datetime_updated'] = now_dt
        df_update['id_term_category'] = id_term_category
        df_vern_update['id_term_status'] = id_term_status_accepted
        df_vern_update['id_terminology'] = id_terminology
        #df_vern_update['id_user_created'] = id_user_created_updated => CHECK!!
        df_vern_update['id_user_updated'] = id_user_created_updated
        df_vern_update['id_term'] = df_vern_update['id_term'].astype(int)
        # rearrange columns for update
        df_vern_update = df_vern_update[['name', 'datetime_updated', 'semantic_uri', 'id_term_category', 'id_term_status',
             'id_terminology', 'id_user_created', 'id_user_updated', 'datetime_last_harvest', 'id_term']]
        sqlExec.batch_update_vernacular_terms(df_vern_update)
    else:
        logger.debug('Updating existing Vernacular-ITIS TERMS : SKIPPED')
    del df_vern_update

    # -------insert vernaculars new terms
    df_vern_insert = pd.merge(df_vern, df_pang, on=['semantic_uri'], how='left')
    df_vern_insert = df_vern_insert[df_vern_insert.datetime_last_harvest.isnull()]  # all terms in db has datetime_last_harvest
    if len(df_vern_insert) > 0:
        logger.debug('Inserting new ITIS Vernacular Terms : %s' % df_vern_insert.shape[0])
        df_vern_insert = df_vern_insert[['name', 'update_date', 'semantic_uri']]
        df_vern_insert = df_vern_insert.rename(columns={"update_date": "datetime_last_harvest"})
        df_vern_insert['datetime_updated'] = now_dt
        df_vern_insert['id_term_category'] = id_term_category
        df_vern_insert['id_terminology'] = id_terminology
        df_vern_insert['id_term_status'] = id_term_status_accepted
        df_vern_insert['id_user_created'] = id_user_created_updated
        df_vern_insert['id_user_updated'] = id_user_created_updated
        max_term_pk = sqlExec.get_max_idterm() + 1
        df_vern_insert['id_term'] = range(max_term_pk, max_term_pk + len(df_vern_insert))
        df_vern_insert = df_vern_insert[['name', 'datetime_updated', 'semantic_uri', 'id_term_category', 'id_term_status','id_terminology', 'id_user_created', 'id_user_updated', 'datetime_last_harvest', 'id_term']]
        sqlExec.batch_insert_new_terms(df_vern_insert, 'term')
    else:
        logger.debug('Inserting new ITIS Vernacular Terms : SKIPPED')
    del df_vern_insert
    del df_pang

    ## --------- Term Dict relations
    select_cols =['id_term','semantic_uri']
    dfterm = sqlExec.select_sql_pangaea_terms('term',select_cols)
    dfterm = dfterm.dropna(subset = ['semantic_uri'])
    term_dict= dict(zip(dfterm.semantic_uri, dfterm.id_term))

    #select all existing relations, both id_term and id_term_related from ITIS (id_terminology = 2)
    #df_rels = sqlExec.select_sql_itis_relations() #tr.id_term,tr.id_term_related, tr.id_relation_type

    # -------SYNONYM: vernacular relations ( 1 tsn has many verns) is_synonym_of_pk = 3
    df_vern = df_vern[['tsn', 'vern_id']]
    df_vern = df_vern[df_vern.tsn != 0] #temp check
    df_vern['id_term'] = df_vern.apply(lambda x: get_vern_tsn_lsid(x["tsn"], x["vern_id"]), axis=1) # vern-based term
    df_vern['id_term_related'] = df_vern['tsn'].apply(lambda x: term_dict.get(itis_lsid_pfx + str(x))) # tsn-term
    df_syn_vend= create_relation_df(df_vern,is_synonym_of_pk, ['tsn', 'vern_id'])
    logger.debug('SYNONYM relations - vernaculars : %s' % df_syn_vend.shape[0])
    sqlExec.insert_update_relations(df_syn_vend, 'term_relation')

    # -------SYNONYM: main relations ( 1 tsn has many tsn_accepted) is_synonym_of_pk = 3
    df_synonym = sqlExec.select_itis_rel(['tsn', 'tsn_accepted'], 'synonym_links')
    df_synonym = df_synonym[(df_synonym.tsn != 0) & (df_synonym.tsn_accepted != 0)]  #temp check (for itis only) 0 tsn is undefined in ITIS
    df_synonym = df_synonym[df_synonym['tsn'].notnull() & df_synonym['tsn_accepted'].notnull()].reset_index(drop=True)
    df_synonym['id_term'] = df_synonym['tsn'].apply(lambda x: term_dict.get(itis_lsid_pfx + str(x)))
    df_synonym['id_term_related'] = df_synonym['tsn_accepted'].apply(lambda x: term_dict.get(itis_lsid_pfx + str(x)))
    df_syn_main = create_relation_df(df_synonym, is_synonym_of_pk, ['tsn', 'tsn_accepted'])
    logger.debug('SYNONYM relations main : %s' % df_syn_main.shape[0])
    #temp_csv = df_syn_main[['id_term','id_term_related']]
    #temp_csv.to_csv('synonyms.csv', sep='\t', encoding='utf-8')
    sqlExec.insert_update_relations(df_syn_main, 'term_relation')

    # a = df_syn_vend[['id_term','id_term_related']]
    # b = df_syn_main[['id_term', 'id_term_related']]
    # print(a.columns, b.columns)
    # c = pd.merge(a, b, how='inner', on=['id_term'])
    # print(c)

    # -------BROADER relations has_broader_term_pk = 1
    df_broad = df_itis[['tsn','parent_tsn']]
    df_broad = df_broad[(df_broad.tsn != 0) & (df_broad.parent_tsn != 0)] #tsn=0 is not exists in itis, so should be excluded
    df_broad = df_broad[df_broad['tsn'].notnull() & df_broad['parent_tsn'].notnull()].reset_index(drop=True)
    df_broad['parent_tsn'] = df_broad['parent_tsn'].astype(int)
    df_broad['tsn'] = df_broad['tsn'].astype(int)
    df_broad['id_term'] = df_broad['tsn'].apply(lambda x: term_dict.get(itis_lsid_pfx + str(x)))
    df_broad['id_term_related'] = df_broad['parent_tsn'].apply(lambda y: term_dict.get(itis_lsid_pfx + str(y)))
    df_broad_sub = create_relation_df(df_broad, has_broader_term_pk, ['tsn', 'parent_tsn'])
    logger.debug('BROADER relations : %s' % df_broad_sub.shape[0])
    sqlExec.insert_update_relations(df_broad_sub, 'term_relation')

    # HAS ATTRIBUTE has_attribute_pk = 2 (disabled for now, as PANGAEA don't want the rank terms anymore)
    #rank_types = df_itis.rank_name.unique().tolist()
    #df_rank = sqlExec.select_sql_pangaea_rank_terms('term', ['name', 'id_term'], rank_types)
    #rank_dict = dict(zip(df_rank.name, df_rank.id_term))
    #df_att = df_itis[['tsn', 'rank_name']]
    #df_att = df_att[df_att.tsn != 0]
    #df_att = df_att[df_att['tsn'].notnull() & df_att['rank_name'].notnull()].reset_index(drop=True)
    #df_att['tsn'] = df_att['tsn'].astype(int)
    #df_att['id_term'] = df_att['tsn'].apply(lambda x: term_dict.get(itis_lsid_pfx + str(x)))
    #df_att['id_term_related'] = df_att['rank_name'].apply(lambda y: rank_dict.get(str(y)))
    #df_att = create_relation_df(df_att, has_attribute_pk, ['tsn', 'rank_name'])
    #logger.debug('ATTRIBUTE relations : %s' % df_att.shape[0])
    #sqlExec.insert_update_relations(df_att, 'term_relation')

def create_relation_df(dfa, relation_id, drop_cols):
    df = dfa.copy()
    #temp = df[df.isnull().any(axis=1)]
    #print(temp)
    df = df.dropna(subset=['id_term', 'id_term_related'])
    #logger.debug('create_insert_relation_df : %s' % df.shape[0])
    df['id_term'] = df['id_term'].astype(int)
    df['id_term_related'] = df['id_term_related'].astype(int)
    df['id_relation_type'] = relation_id
    df['datetime_created'] = now_dt
    df['datetime_updated'] = now_dt
    df['id_user_created'] = id_user_created_updated
    df['id_user_updated'] = id_user_created_updated
    df = df.drop(columns=drop_cols)
    return df

def get_vern_tsn_lsid(tsn,vern):
    lsid_comb = itis_vernacular_prefix.format(tsn,vern)
    return term_dict.get(lsid_comb)

def initLog():
    cwd = os.getcwd()
    # create logger with 'spam_application'
    logger = logging.getLogger('pg_itis_importer')
    logger.setLevel(logging.DEBUG)
    # create file handler which logs even debug messages
    fh = logging.FileHandler(cwd+'/data/itis.log')
    fh.setLevel(logging.DEBUG)
    # create console handler with a higher log level
    ch = logging.StreamHandler()
    ch.setLevel(logging.ERROR)
    # create formatter and add it to the handlers
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)
    # add the handlers to the logger
    logger.addHandler(fh)
    logger.addHandler(ch)
    return logger

#test olnly
def extractWriteSQLLite(itis_sql_url,targetfile):
    #print (str(datetime.datetime.now()))
    #print(itis_sql_url)
    content = requests.get(itis_sql_url)
    #print(str(datetime.datetime.now()))
    try:
        # Create a ZipFile Object and load sample.zip in it
        with ZipFile(BytesIO(content.content), 'r') as zipObj:
            # Get a list of all archived file names from the zip and Iterate over the file names
            for f in zipObj.namelist():
                # Check filename endswith csv
                if f.endswith('.sqlite'):
                    #print(f,targetfile)
                    #zipObj.extract(f, 'data/')
                    with open(targetfile, "wb") as fw:  # open the output path for writing
                        fw.write(zipObj.read(f))
                        logger.debug('ITIS zip file extracted and saved.')
                        return True
        #print(str(datetime.datetime.now()))
        logger.debug('Zip file did not contain sqlite file.')
        return False
    except BadZipfile:
        logger.debug('Zip extraction of ITIS file failed.')
        return False

if __name__ == '__main__':
    global logger
    logger = initLog()
    logger.debug("Starting ITIS harvester...")
    a = datetime.datetime.now()
    main()
    b = datetime.datetime.now()
    diff = b-a
    logger.debug('Total execution time:%s' %diff)
    logger.debug('----------------------------------------')