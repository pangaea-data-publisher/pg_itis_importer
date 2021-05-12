import sqlite3
from sqlite3 import Error
import pandas as pd
import psycopg2
import psycopg2.extras

class SQLExecutor(object):
    
    def setLogger(self, lg):
        global logger
        logger = lg
        
    def setDBParams(self,u,p,d,h,prt,db_file):
        global user
        global pwd
        global db
        global host
        global itis_db_file
        global pt
        user = u
        pwd = p
        db = d
        host = h
        itis_db_file=db_file
        pt = prt

    def setTermParams(self,lsid_pfx,id_termi,id_user,id_tcategory,uri_prefix,status_accepted,status_notaccepted,broader_pk, synonym_pk, attribute_pk,vernacular_prefix):
        global itis_lsid_pfx
        global id_terminology
        global id_user_created_updated
        global id_term_category
        global itis_uri_prefix
        global id_term_status_accepted
        global id_term_status_notaccepted
        global itis_vernacular_prefix
        global has_broader_pk
        global has_synonym_pk
        global has_attribute_pk
        itis_lsid_pfx = lsid_pfx
        id_terminology = id_termi
        id_user_created_updated = id_user
        id_term_category = id_tcategory
        itis_uri_prefix =uri_prefix
        id_term_status_accepted = status_accepted
        id_term_status_notaccepted =status_notaccepted
        itis_vernacular_prefix =vernacular_prefix
        has_broader_pk=broader_pk
        has_synonym_pk=synonym_pk
        has_attribute_pk=attribute_pk

    def create_sqlite_connection(self):
        try:
            conn = sqlite3.connect(itis_db_file)
            return conn
        except Error as e:
            logger.debug(e)

        return None

    def create_pg_connection(self):
        try:
            connectStr = 'host={host} dbname={db} user={user} password={pwd} port={port}'.format(host=host,db=db, user=user, pwd=pwd, port=pt)
            conn = psycopg2.connect(connectStr)
            return conn
        except Error as e:
            logger.debug(e)
        return None

    def select_itis_taxonomic_units(self):
        df = pd.DataFrame()  # creates a new dataframe that's empty
        try:
            conn = self.create_sqlite_connection()
            #cur = conn.cursor()
            sql_taxonunits = "SELECT tu.tsn, tu.complete_name as name, tu.name_usage as id_term_status, tu.rank_id,trnk.rank_name, " \
                             "tath.taxon_author as description,tu.initial_time_stamp as datetime_created, tu.update_date, tu.parent_tsn " \
                             "FROM taxonomic_units as tu " \
                             "LEFT JOIN taxon_authors_lkp as tath ON tu.taxon_author_id = tath.taxon_author_id " \
                             "LEFT JOIN taxon_unit_types as trnk ON tu.rank_id= trnk.rank_id and tu.kingdom_id = trnk.kingdom_id "
            df = pd.read_sql(sql_taxonunits, conn)
        except sqlite3.Error as error:
            logger.debug('sqlite3.Error select_itis_taxonomic_units: %s' % error)
        finally:
            if conn is not None:
                conn.close()
        return df

    def select_sql_pangaea_terms(self,table,columns):
        conn_pg = self.create_pg_connection()
        cols = ','.join(columns)
        pg_sql_select_term = "SELECT {} FROM {} " \
                             "where id_terminology={} ORDER BY id_term ASC".format(cols, table, id_terminology)
        df_pg_term = pd.read_sql(pg_sql_select_term, conn_pg)
        conn_pg.close()
        return df_pg_term

    def select_sql_itis_relations(self):
        conn_pg = self.create_pg_connection()
        pg_sql_select_rel = "select tr.id_term,tr.id_term_related, tr.id_relation_type from term_relation tr " \
                             "join term t1 on tr.id_term=t1.id_term " \
                             "join term t2 on tr.id_term_related=t2.id_term " \
                             "where t1.id_terminology=2 and t2.id_terminology=2"
        df_pg_term = pd.read_sql(pg_sql_select_rel, conn_pg)
        conn_pg.close()
        return df_pg_term

    def get_max_idterm(self):
        try:
            conn_pg = self.create_pg_connection()
            cur = conn_pg.cursor()
            sql_max = "select max(id_term) from term "
            cur.execute(sql_max)
            record = cur.fetchone()
            conn_pg.commit()  ## commit the transaction
            cur.close()
        except psycopg2.DatabaseError as error:
            logger.debug(error)
        finally:
            if conn_pg is not None:
                conn_pg.close()
        return record[0]

    def batch_update_terms(self,df):
        try:
            conn_pg = self.create_pg_connection()
            conn_pg.autocommit = False
            cur = conn_pg.cursor()
            list_of_tuples = [tuple(x) for x in df.values]
            update_stmt = 'update term set name =%s, datetime_created=%s,' \
                          'datetime_updated=%s, description=%s,semantic_uri=%s,uri=%s,id_term_category=%s,' \
                          'id_term_status=%s,id_terminology=%s,' \
                          'id_user_updated=%s,datetime_last_harvest=%s where id_term=%s ;'
            #update_stmt = 'update term set name =%s, datetime_created=%s,' \
                          #'datetime_updated=%s, description=%s,semantic_uri=%s,uri=%s,id_term_category=%s,' \
                          #'id_term_status=%s,id_terminology=%s,id_user_created=%s,' \
                          #'id_user_updated=%s,datetime_last_harvest=%s where id_term=%s ;'
            psycopg2.extras.execute_batch(cur, update_stmt, list_of_tuples)
            logger.debug("batch_update_terms - record updated successfully ")
            #Commit your changes
            conn_pg.commit()
        except psycopg2.DatabaseError as error:
            logger.debug('Failed to update record to database rollback: %s ' % error)
            # reverting changes because of exception
            conn_pg.rollback()
        finally:
            if conn_pg is not None:
                cur.close()
                conn_pg.close()

    def batch_update_vernacular_terms(self,df):
        try:
            conn_pg = self.create_pg_connection()
            conn_pg.autocommit = False
            cur = conn_pg.cursor()
            list_of_tuples = [tuple(x) for x in df.values]
            update_stmt = 'update term set name =%s,semantic_uri=%s,' \
                          'id_term_category=%s,id_term_status=%s,id_terminology=%s,id_user_created=%s,' \
                          'id_user_updated=%s,datetime_last_harvest=%s where id_term=%s ;'
            psycopg2.extras.execute_batch(cur, update_stmt, list_of_tuples)
            logger.debug("batch_update_vernacular_terms - record updated successfully ")
            # Commit your changes
            conn_pg.commit()
        except psycopg2.DatabaseError as error:
            logger.debug('Failed to update record to database rollback: %s' % error)
            conn_pg.rollback()
        finally:
            if conn_pg is not None:
                cur.close()
                conn_pg.close()

    def batch_insert_new_terms(self,df, table):
        try:
            conn_pg = self.create_pg_connection()
            conn_pg.autocommit = False
            list_of_tuples = [tuple(x) for x in df.values]
            df_columns = list(df)
            columns = ",".join(df_columns)
            # create VALUES('%s', '%s",...) one '%s' per column
            values = "VALUES({})".format(",".join(["%s" for _ in df_columns]))
            # create INSERT INTO table (columns) VALUES('%s',...)
            insert_stmt = "INSERT INTO {} ({}) {}".format(table, columns, values)
            cur = conn_pg.cursor()
            psycopg2.extras.execute_batch(cur, insert_stmt, list_of_tuples)
            logger.debug("batch_insert_new_terms - record inserted successfully ")
            # Commit your changes
            conn_pg.commit()
        except psycopg2.DatabaseError as error:
            logger.debug('Failed to insert records to database rollback: %s' % (error))
            conn_pg.rollback()
        finally:
            if conn_pg is not None:
                cur.close()
                conn_pg.close()

    def select_vernaculars(self):
        df = pd.DataFrame()  # creates a new dataframe that's empty
        try:
            conn = self.create_sqlite_connection()
            select_stmt = "SELECT tsn, vernacular_name as name,update_date,vern_id from vernaculars " \
                          "where (language='English' or language='unspecified') and approved_ind='Y' "
            df = pd.read_sql(select_stmt, conn)
        except sqlite3.Error as error:
            logger.debug(error)
        finally:
            if conn is not None:
                conn.close()
        return df

    # def get_semanticuri_termid_mapping(self):
    #     select_cols = ['id_term', 'semantic_uri']
    #     dfterm = self.select_sql_pangaea_terms('term', id_terminology, select_cols)
    #     dfterm = dfterm.dropna(subset=['semantic_uri'])
    #     return dict(zip(dfterm.semantic_uri, dfterm.id_term))

    def insert_update_relations(self,df, table):
        try:
            conn_pg = self.create_pg_connection()
            conn_pg.autocommit = False
            if len(df) > 0:
                df_columns = list(df)
                # create (col1,col2,...)
                columns = ",".join(df_columns)
                # create VALUES('%s', '%s",...) one '%s' per column
                #values = "VALUES({})".format(",".join(["%s" for _ in df_columns]))
                # create INSERT INTO table (columns) VALUES('%s',...)
                insert_stmt = "INSERT INTO {} AS t ({}) VALUES %s ".format(table, columns)
                #on_conflict = "ON CONFLICT DO NOTHING ; "
                on_conflict = "ON CONFLICT " \
                              "DO UPDATE SET id_relation_type = EXCLUDED.id_relation_type , " \
                              "datetime_updated = EXCLUDED.datetime_updated , id_user_updated = EXCLUDED.id_user_updated " \
                              "WHERE (t.id_relation_type) IS DISTINCT FROM (EXCLUDED.id_relation_type); "
                upsert_stmt = insert_stmt + on_conflict
                #print(upsert_stmt)
                cur = conn_pg.cursor()
                #psycopg2.extras.execute_batch(cur, upsert_stmt, df.values)
                psycopg2.extras.execute_values(cur, upsert_stmt, df.values,page_size=10000)
                logger.debug("Relations inserted/updated successfully ")
                conn_pg.commit()
        except psycopg2.DatabaseError as error:
                logger.debug('Failed to insert/update relations to database rollback:  %s' % error)
                conn_pg.rollback()
        finally:
            if conn_pg is not None:
                cur.close()
                conn_pg.close()

    def select_itis_rel(self, columns, table):
        df_res = pd.DataFrame()  # creates a new dataframe that's empty
        cols = ','.join(columns)
        try:
            conn = self.create_sqlite_connection()
            cur = conn.cursor()
            select_stmt = "SELECT {} from {}".format(cols, table)
            df_res = pd.read_sql(select_stmt, conn)
        except sqlite3.Error as error:
            logger.debug(error)
        finally:
            if conn is not None:
                cur.close()
                conn.close()
        return df_res

    def select_sql_pangaea_rank_terms(self,table, columns, rank_types):
        conn_pg = self.create_pg_connection()
        cols = ','.join(columns)
        ranks = (', '.join("'" + item + "'" for item in rank_types))
        pg_sql_select_term = "SELECT {} FROM {} where id_terminology={} and name IN ({}) " \
                             "ORDER BY id_term ASC".format(cols, table,id_terminology,ranks)
        df_pg_term = pd.read_sql(pg_sql_select_term, conn_pg)
        conn_pg.close()
        return df_pg_term

