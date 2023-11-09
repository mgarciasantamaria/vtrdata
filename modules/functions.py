#!/usr/bin/env python
#_*_ codig: utf8 _*_
import datetime, psycopg2, smtplib, sys, traceback, boto3, random, string
import xml.etree.ElementTree as ET
import pandas as pd
from email.message import EmailMessage
from modules.constants import *

#*******************--SEND MAIL Function--************************************************************************
#Este código de Python define una función llamada SendMail que toma dos argumentos: text y mail_subject. Esta función se utiliza para enviar correos electrónicos utilizando la librería smtplib de Python.
#La función comienza creando un objeto EmailMessage vacío y luego establece el contenido del correo electrónico utilizando el argumento text. Luego, establece el asunto del correo electrónico utilizando el argumento mail_subject.
#A continuación, se establece la dirección de correo electrónico del remitente en el encabezado From utilizando la cadena 'alarmas-aws@vcmedios.com.co'. La dirección de correo electrónico del destinatario se establece en el encabezado To utilizando el valor de la variable Mail_To.
#Después de establecer los encabezados del correo electrónico, la función crea una conexión SMTP utilizando el host y puerto especificados en la función smtplib.SMTP(). En este caso, el host es 10.10.122.17 y el puerto es 25.
#Luego, la función envía el correo electrónico utilizando el método send_message() del objeto SMTP creado anteriormente. Finalmente, la conexión SMTP se cierra usando el método quit().
#La función no devuelve nada ya que simplemente se utiliza para enviar el correo electrónico y no requiere de una salida específica.
def SendMail(text, mail_subject): #se define la función llamada 'SendMail' que acepta dos argumentos: 'text' y 'mail_subject'.
    msg = EmailMessage() #Se crea un objeto 'EmailMessage' vacío para almacenar los detalles del correo electrónico.
    msg.set_content(text) #Se establece el contenido del correo electrónico utilizando el valor del argumento 'text'.
    msg['Subject'] = mail_subject #Se establece el asunto del correo electrónico utilizando el valor del argumento 'mail_subject'.
    msg['From'] = 'alarmas-aws@vcmedios.com.co' #Se establece la dirección de correo electrónico del remitente en el encabezado 'From' del correo electrónico.
    msg['To'] = Mail_To # Se establece la dirección de correo electrónico del destinatario en el encabezado 'To' del correo electrónico. El valor de la variable 'Mail_To' se utiliza como dirección de correo electrónico del destinatario.
    conexion = smtplib.SMTP(host='10.10.122.17', port=25) #Se crea una conexión SMTP utilizando el host '10.10.130.217' y el puerto '25'.
    conexion.ehlo() #Se inicia la conexión SMTP con el servidor.
    conexion.send_message(msg) #Se envía el correo electrónico utilizando el método 'send_message()' del objeto SMTP creado anteriormente.
    conexion.quit() #Se cierra la conexión SMTP utilizando el método quit() del objeto SMTP.
    return
#***********************************--END--************************************************************************************************

#*************************************--FUNCTION Duration_Transform--***************************************
# Funcion que permite transfomar le dato de duracion del contenido cuando este no cumple el formato HH:MM:SS.
#y convierte el dato en segundos. Toma como argumento la lista de digitos del dato de duracion del contenido
# y devuelve el mismo en segundos
def Duration_Transform(duration):
    while len(duration)>3:
        duration.remove('')
    duration_seconds=str(
        round(
        	datetime.timedelta(
    		    hours=int(duration[0]),
	    	    minutes=int(duration[1]),
		        seconds=int(duration[2])
                ).total_seconds()
            )
	    )
    return duration_seconds
#*****************************************--END--*************************************************************

#*********************--FUNCTION print_log--************************************************************
#Funcion que permite escribir texto en un archivo txt. Toma como argumento la variable TEXT que corresponde al texto a imprimir en el archivo
#y la variable DATE_LOG que corresponde a la fecha de creacion del log. No devuelve.
def print_log(TEXT):
    log_file=open(f"{log_Path}/Sumary_log.txt", "a")
    log_file.write(f"{str(datetime.datetime.strftime(datetime.datetime.now(), '%H:%M:%S'))}\t{TEXT}\n")
    log_file.close()
#************************************--END--************************************************************


#*******************************--FUNCTION extrac_xml_data--****************************************************************************************
# Se define una función llamada 'extract_xml_data' que toma como argumentos una lista de contentid y una fecha de registro. 
# La función se conecta a una base de datos PostgreSQL y a un recurso S3 de AWS usando las bibliotecas psycopg2 y boto3 respectivamente. 
# Luego, para cada contentid en la lista, la función extrae un archivo XML de S3 y lo analiza con la biblioteca ElementTree de Python para 
# extraer información sobre el contenido y almacenarla en la base de datos PostgreSQL. La función devuelve una lista de contentid que 
# fueron procesados exitosamente.
def extract_xml_data(x): #Se define la función llamada extract_xml_data que acepta dos argumentos: contentid_list y DATE_LOG.
    xml_not_found=[]
    d= {
        'assetid': [],      #assetid
        'humanid': [],      #humanid-contetnid
        'servicetype': [],  #servicetype
        'contenttype': [],  #contenttype
        'channel': [],      #channel
        'title': [],        #title
        'serietitle': [],   #serietitle
        'season': [],       #season
        'episode': [],      #episode
        'genre': [],        #genre
        'rating': [],       #rating
        'releaseyear': [],  #releaseyear
        'duration': [],     #duration
    }
    postgresql=psycopg2.connect(data_base_connect_prod) #Se establece conexion con la base de datos.
    curpsql=postgresql.cursor() #Se activa el cursor en la base de datos.
    aws_session=boto3.Session(profile_name=aws_profile) #Se establece una sesión de AWS utilizando el perfil especificado en la variable aws_profile.
    s3_resource=aws_session.resource('s3') #Se crea un recurso s3_resource para acceder a los objetos de Amazon S3.
    for contentid in x: #Se itera a través de cada elemento en la lista contentid_list.
        curpsql.execute(f"SELECT * FROM {xml_table} WHERE contentid LIKE '%{contentid}%';")
        if curpsql.rowcount !=0:
            data=curpsql.fetchone()
            d['assetid'].append(str(contentid))
            d['servicetype'].append('XVOD')
            d['humanid'].append(str(data[0]))
            d['contenttype'].append(str(data[1]))
            d['channel'].append(str(data[2]))
            d['title'].append(str(data[3]))
            d['serietitle'].append(str(data[4]))
            d['releaseyear'].append(int(data[5]))
            d['season'].append(str(data[6]))
            d['episode'].append(str(data[7]))
            d['genre'].append(str(data[8]))
            d['rating'].append(str(data[9]))
            d['duration'].append(int(data[10]))
        else:
            bucket = Buckets[contentid[6:8]][0] # Se extrae el primer elemento del dicionario Buckets segun el key que contenga la variable contentid y se almacena en la variable 'bukect'.
            Folder= Buckets[contentid[6:8]][1] # Se extrae el segundo elemento del dicionario Buckets segun el key que contenga la variable contentid y se almacena en la variable 'Folder'.
            Object_key = Folder+'/'+contentid+'/'+contentid+'.xml' #se utilizan los valores de bucket y la variable Folder para defiir el nombre del bucket y la ruta del objeto XML en Amazon S3.
            try:
                xml_data=s3_resource.Bucket(bucket).Object(Object_key).get()['Body'].read().decode('utf-8') #Se obtiene el objeto XML desde Amazon S3, Se lee el contenido del objeto y se almacena en la variable xml_data.
            except:
                xml_not_found.append(contentid)
                continue
            xml_root = ET.fromstring(xml_data) #Se utiliza la función ET.fromstring del módulo ElementTree para analizar el contenido XML y se almacena en la variable xml_root.
            contentType=xml_root.find("contentType").text
            humanid=xml_root.find("externalId").text
            channel=xml_root.find('channel').text
            for Title in xml_root.iter('title'): #se itera a través de las etiquetas title en el XML.
                title=Title.text #Se agrega el valor a la lista data.
                break
            releaseyear=xml_root.find("release").text
            d['assetid'].append(contentid)
            d['humanid'].append(humanid)
            d['servicetype'].append('XVOD')
            d['contenttype'].append(contentType) #Se extrae el valor de la etiqueta contentType del XML y se almacena en la variable contentType.
            d['channel'].append(channel)
            d['title'].append(title)
            d['releaseyear'].append(int(releaseyear)) #Se agrega el valor de la etiqueta release a la lista data.
            if contentType=="movie": #Se comprueba el valor de contentType.
                serietitle=''
                season=''
                episode=''
                d['serietitle'].append(serietitle) #Si es igual a "movie", se agrega "na" a la lista data como valor para serietitle.
                d['season'].append(season)
                d['episode'].append(episode)
            elif contentType=="episode":
                for serieTitle in xml_root.iter("seriesTitle"):# Si es igual a episode se itera a traves de la etiqueta seriesTitle 
                    serietitle=serieTitle.text
                    d['serietitle'].append(serietitle) #Se agrega el primer valor a la lista data.
                    break
                season=xml_root.find("season").text
                episode=xml_root.find("episode").text
                d['season'].append(season) #season
                d['episode'].append(episode) #episode
            
                
            for Genre in xml_root.iter('genre'): #Se itera a traves de la etiqueta genre y se agrega el primer valor a la lista data 
                genre=Genre.text
                d['genre'].append(genre) #genre
                break
            for Rating in xml_root.iter('rating'): #Se itera a traves de la etiqueta rating y se agrega el primer valor a la lista data 
                rating=Rating.text
                d['rating'].append(rating) #rating
                break
                    
            dur=xml_root.find("duration").text #Se almacena el valor de la etiqueta duration en la variable dur.
            duration_split=dur.split(':') #Separamos en forma de lista los digitos numericos del dato dur.
            duration=Duration_Transform(duration_split) #Se ejecuta la funcion Duration_Transform la cual transforma y entrega el dato en segundos, se almacena el mismo en la variable diration 
            d['duration'].append(int(duration)) #Se agrega el valor de la variable duration a la lista data.
            sql_insert=f"INSERT INTO {xml_table} VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"
            sql_data=(
                humanid, #contentid
                contentType, #contenttype
                channel, #channel
                title, #title
                serietitle, #serietitle
                releaseyear, #releaseyear
                season, #season
                episode, #episode
                genre, #genre
                rating, #rating
                duration #duration
            )
            curpsql.execute(sql_insert, sql_data)
            postgresql.commit()
    return pd.DataFrame(d), xml_not_found #Retorna la lista de contentid a los cuales se extrajo los datos del xml.

#************************************************************--END--*******************************************

def id_generate(x):
    micadena=string.ascii_letters + string.digits + str('-_')       
    id="".join(random.choice(micadena) for j in range(random.randint(20,20)))
    return str(id)