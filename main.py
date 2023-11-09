#!/usr/bin/env python
#_*_ codig: utf8 _*_
import psycopg2, time, json, re, os
import pandas as pd
from dateutil.relativedelta import relativedelta
from modules.constants import *
from modules.functions import *

if __name__ == '__main__':
    try:
        dict_summary={}
        cdndb_connect=psycopg2.connect(data_base_connect_prod)
        cdndb_cur=cdndb_connect.cursor()
        log_path=input("ingrese file path: ")
        dict_summary['log_name']=log_path.split('/')[-1]
        dtype={
            'View_Minutos': 'Int64',
            'IDEN_VIVIENDA': 'object'
        }
        df=pd.read_csv(log_path, delimiter=',', low_memory=False, dtype=dtype)
        quantity=df.shape[0]
        dict_summary['rows']=quantity
        df=df.drop(['TITULO', 'TITULO_lower', 'DURATION'], axis='columns')
        df.rename(columns={'IDEN_VIVIENDA': 'clientid', 'ID_FECH_COMPRA': 'datetime', 'View_Minutos': 'segmentos', 'ExternalID': 'assetid'}, inplace=True)
        df['device']='N/A'
        df['mso_country']='CL'
        df['datetime']=df['datetime'].apply(lambda x: re.sub(r"\.[^.]*$", "", x))
        df['uri']='NA'
        df['mso_name']='vtr'
        df['manifestid']=df['datetime'].map(id_generate)
        df=df.reindex(columns=['datetime', 'manifestid', 'uri', 'mso_name', 'mso_country', 'clientid', 'assetid', 'device', 'segmentos'])
        df_metadata, xml_notfound=extract_xml_data(df['assetid'].drop_duplicates().dropna())
        dict_summary['xml_notfound']=xml_notfound
        a1=df.shape[0]
        for contentid in xml_notfound:
            df.drop(df[df['assetid']==contentid].index, inplace=True)
        #df=df.drop(xml_nofound, axis=0)
        a2=df.shape[0]
        dict_summary['delete_playbacks']=a1-a2
        df_data=pd.merge(df[['datetime', 'manifestid', 'uri', 'mso_name', 'mso_country', 'clientid', 'assetid', 'device', 'segmentos']], df_metadata[['assetid', 'humanid', 'servicetype', 'contenttype', 'channel', 'title', 'serietitle', 'season', 'episode', 'genre', 'rating', 'releaseyear', 'duration']], on='assetid', how='left')
        df_data=df_data.fillna('None')
        df_data=df_data.reindex(columns=['manifestid','datetime', 'mso_country', 'mso_name','device', 'clientid', 'uri', 'assetid', 'humanid', 'servicetype', 'contenttype', 'channel', 'title', 'serietitle', 'releaseyear', 'season', 'episode', 'genre', 'rating', 'duration', 'segmentos'])
        list_data=df_data.values.tolist()
        cdndb_cur.executemany("INSERT INTO playbacks VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", list_data)
        cdndb_connect.commit()
        time.sleep(2)
        dict_summary['sum_Insert_Playbacks']=cdndb_cur.rowcount
        cdndb_cur.close()
        cdndb_connect.close()       
        os.remove(log_path)
        dict_summary_str=json.dumps(dict_summary, sort_keys=False, indent=4)
        print_log(dict_summary_str)
        SendMail(dict_summary_str, 'Summary VTR Data Playbacks')
    except:
        cdndb_cur.close()
        cdndb_connect.close()
        error=sys.exc_info()[2]
        errorinfo=traceback.format_tb(error)[0]
        dict_summary['Error']={
            'Error': str(sys.exc_info()[1]),
            'error_info': str(errorinfo)
        }
        dict_summary_str=json.dumps(dict_summary, sort_keys=False, indent=4)
        print_log(dict_summary_str)
        mail_subject='FAIL VTR_Data_PROD Execution Error'
        SendMail(dict_summary_str, mail_subject)