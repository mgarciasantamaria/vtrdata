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
        log_path=input("ingrese file path: ")
        df=pd.read_csv(log_path, delimiter=',', low_memory=False)
        quantity=df.shape[0]
        df=df.drop(['TITULO', 'TITULO_lower', 'DURATION'], axis='columns')
        df=df.rename(columns={'IDEN_VIVIENDA': 'clientid', 'ID_FECH_COMPRA': 'datetime', 'View_Minutos': 'segduration', 'DURATION': 'duration', 'ExternalID': 'contentid'}, inplace=True)
        df['device']='N/A'
        df['country']='CL'
        df['datetime']=df['datetime'].apply(lambda x: re.sub(r"\.[^.]*$", "", x))
        df=df.reindex(columns=['datetime', 'country', 'clientid', 'contentid', 'device', 'segduration'])
        print(df)
        cdndb_connect=psycopg2.connect(data_base_connect_prod)
        cdndb_cur=cdndb_connect.cursor()
        cdndb_cur.executemany("INSERT INTO vtrdata VALUES (%s, %s, %s, %s, %s, %s)", df.values.tolist())
        cdndb_connect.commit()
        time.sleep(2)
        cdndb_cur.execute("SELECT DISTINCT vtrdata.contentid FROM vtrdata LEFT JOIN xmldata ON vtrdata.contentid = xmldata.contentid where xmldata.contentid is NULL;")    
        contentid_list=cdndb_cur.fetchall()
        if contentid_list != []:
            xml_nofound, dict_xml_extract = extract_xml_data(contentid_list)
            dict_summary[log_path]=({'extract_xml_data': dict_xml_extract})
            for contentid in xml_nofound:
                cdndb_cur.execute(f"DELETE FROM vtrdata WHERE contentid LIKE '{contentid}';")
                dict_summary['Delete_Playbacks']=cdndb_cur.rowcount
                cdndb_connect.commit()
            sql="""INSERT INTO playbacks
            SELECT 
            vtrdata.datetime,
            vtrdata.country,
            'vtr',
            vtrdata.device,
            vtrdata.clientid,
            vtrdata.contentid,
            xmldata.contenttype,
            xmldata.channel,
            xmldata.title,
            xmldata.serietitle,
            xmldata.releaseyear,
            xmldata.season,
            xmldata.episode,
            xmldata.genre,
            xmldata.rating,
            xmldata.duration,
            vtrdata.segduration
            FROM vtrdata
            LEFT JOIN xmldata ON vtrdata.contentid = xmldata.contentid
            GROUP BY vtrdata.datetime,
            vtrdata.country,
            vtrdata.device,
            vtrdata.clientid,
            vtrdata.contentid,
            xmldata.contenttype,
            xmldata.channel,
            xmldata.title,
            xmldata.serietitle,
            xmldata.releaseyear,
            xmldata.season,
            xmldata.episode,
            xmldata.genre,
            xmldata.rating,
            xmldata.duration,
            vtrdata.segduration;
            """
            cdndb_cur.execute(sql)
            dict_summary[log_path].update({'sum_Insert_Playbacks': cdndb_cur.rowcount})
            cdndb_connect.commit()
            dict_str=json.dumps(dict_summary[log_path], sort_keys=False, indent=4)
            print(dict_str)
            cdndb_cur.execute('DELETE FROM vtrdata;')
            cdndb_connect.commit()
            os.remove(log_path)
            dict_summary_str=json.dumps(dict_summary, sort_keys=False, indent=4)
            print(dict_summary_str)
            print_log(dict_summary_str)
            SendMail(dict_summary_str, 'Summary VTR Data Playbacks')
            cdndb_cur.close()
            cdndb_connect.close()
        else:
            pass        
        
    except:
        cdndb_cur.close()
        cdndb_connect.close()
        error=sys.exc_info()[2]
        errorinfo=traceback.format_tb(error)[0]
        dict_summary['Error']={
            'Error': str(sys.exc_info()[1]),
            'error_info': errorinfo
        }
        dict_summary_str=json.dumps(dict_summary, sort_keys=False, indent=4)
        print_log(dict_summary_str)
        mail_subject='FAIL VTR_Data_PROD Execution Error'
        SendMail(dict_summary_str, mail_subject)