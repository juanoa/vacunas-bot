# -*- coding: utf-8 -*-

import requests
import json
import datetime
import math
import sys
import os

import tweepy
from pyexcel_ods import get_data
from apscheduler.schedulers.blocking import BlockingScheduler
from sklearn import linear_model
import numpy as np

# VARIABLES GLOBALES

URL_PREFIJO = 'https://www.mscbs.gob.es/profesionales/saludPublica/ccayes/alertasActual/nCov/documentos/Informe_Comunicacion_'
URL_SUFIJO = '.ods'
FECHA_INICIO_VACUNACION = datetime.date(2021, 1, 18)
DIAS_ENTRE_DOSIS = 28
TAMANO_BARRA_PROGRESO = 8

RESTDB_URL 'xxx/rest'
RESTDB_TOKEN 'xxx'

TW_CONSUMER_KEY = 'xxx'
TW_CONSUMER_SECRET = 'xxx'
TW_ACCESS_TOKEN = 'xxx'
TW_ACCESS_TOKEN_SECRET = 'xxx'

TELEGRAM_TOKEN = 'xxx:xxx'
TELEGRAM_CHAT_ID = 'xxx'

POBLACION_ESP = 46940000
POBLACION_ANDALUCIA = 8464411
POBLACION_ARAGON = 1329391
POBLACION_ASTURIAS = 1018784
POBLACION_BALEARES = 1171543
POBLACION_CANARIAS = 2175952
POBLACION_CANTABRIA = 582905
POBLACION_CASTILLA_LEON = 2394918
POBLACION_CASTILLA_MANCHA = 2045221
POBLACION_CATALUNA = 7780479
POBLACION_CEUTA = 84202
POBLACION_C_VALENCIANA = 5057353
POBLACION_EXTREMADURA = 1063987
POBLACION_GALICIA = 2701819
POBLACION_LA_RIOJA = 319914
POBLACION_MADRID = 6779888
POBLACION_MELILLA = 87076
POBLACION_MURCIA = 1511251
POBLACION_NAVARRA = 661197
POBLACION_PAIS_VASCO = 2220504


# FUNCIONES AUXILIARES

def procesar_json_array(array_bruto, tipo):
    # Procesamos un array de str para convertirlo a su formato (float o int)
    store_list = []
    for item in array_bruto:
        if tipo == 'float':
            store_list.append(float(item))
        else:
            store_list.append(int(item))
    return store_list

def obtener_dias_porcentajes():
    # Formulamos la URL
    url = RESTDB_URL+'/data'

    # Realizamos la petición
    response = requests.get(url=url, headers={'x-apikey': RESTDB_TOKEN})
    
    # Si hay datos, se devuelve, si no, devolvemos arrays vacíos
    try:
        js = json.loads(response.content)
        id = js[0][u'_id']  
        porcentajes_bruto = js[0][u'porcentajes'] 
        dias_bruto = js[0][u'dias']
        # Procesamos los arrays
        porcentajes = procesar_json_array(porcentajes_bruto, 'float')
        dias = procesar_json_array(dias_bruto, 'int')
        return {'id': id, 'porcentajes': porcentajes, 'dias': dias}
    except:
        return {'id': 0, 'porcentajes': [], 'dias': []}

def actualizar_dias_porcentajes(dia, porcentaje):
    # Obtenemos los arrays que ya hay guardados
    datos = obtener_dias_porcentajes()

    # Extraemos los arrays del diccionario
    id = datos['id']
    porcentajes = datos['porcentajes']
    dias = datos['dias']

    # Añadimos los nuevos valores a los arrays 
    porcentajes.append(porcentaje)
    dias.append(dia)
    
    # Preparamos el cuerpo de la petición
    data_post = {'porcentajes': porcentajes, 'dias': dias}

    # Formulamos la URL
    url_post = RESTDB_URL + '/data' + id

    # Realizamos la petición
    response = requests.put(url=url_post, headers={'x-apikey': RESTDB_TOKEN}, data=data_post)
    

def obtener_ultima_fecha():
    # Formulamos la URL
    url = RESTDB_URL + '/fecha'

    # Realizamos la petición
    response = requests.get(url=url, headers={'x-apikey': RESTDB_TOKEN})

    # Si hay una fecha la devolvemos, sino, devolvemos un str vacío
    try:
        return json.loads(response.content)[0][u'fecha']
    except:
        return ''

def guardar_ultima_fecha(fecha):
    # Obtener la ultima fecha
    ultima_fecha = obtener_ultima_fecha()

    # Si hay alguna guardada, la eliminamos
    if ultima_fecha != '':
        url_delete = RESTDB_URL + '/fecha/*?q={"fecha": "'+ultima_fecha+'"}'
        response = requests.delete(url=url_delete, headers={'x-apikey': RESTDB_TOKEN})

    # Preparamos el cuerpo de la petición
    data_post = {'fecha': fecha}

    # Formulamos la URL
    url_post = RESTDB_URL + '/fecha'

    # Realizamos la petición
    response = requests.post(url=url_post, headers={'x-apikey': RESTDB_TOKEN}, data=data_post)


def obtener_mes_esp(month):
    # Devuelve el equivalente del str en español del mes que se pase
    switcher = {
        1: 'enero',
        2: 'febrero',
        3: 'marzo',
        4: 'abril',
        5: 'mayo',
        6: 'junio',
        7: 'julio',
        8: 'agosto',
        9: 'septiembre',
        10: 'octubre',
        11: 'noviembre',
        12: 'diciembre'
    }
    return switcher.get(month)


def convertidor_fecha(o):
        if isinstance(o, datetime.datetime):
            return o.__str__()


def avanzar_fecha_dias(fecha, dias):
    # Avanza la fecha el número de días que se pase por parámetros
    return datetime.date.fromordinal(fecha.toordinal()+dias)


def sendNotification(notification, emoji):
    # Formulamos el string de la noticicación
    msg = emoji+' *Vacunas COVID Twitter bot*\n\n'+notification

    # Formulamos la URL de la petición
    send_text = 'https://api.telegram.org/bot' + TELEGRAM_TOKEN + '/sendMessage?chat_id=' + TELEGRAM_CHAT_ID + '&parse_mode=Markdown&text=' + msg

    # Realizamos la petición
    response = requests.get(send_text)

    return response.json()

# FUNCIONES DEL BOT

def obtener_datos(today):
    # Obtenemos la fecha del día de ayer para obtener sus datos
    fecha_url = avanzar_fecha_dias(today, 0).strftime('%Y%m%d')

    # Obtenemos la fecha del último tweet para evitar repeticiones
    fecha_ultimo = obtener_ultima_fecha()

    # Si ya hemos publicado, devolvemos el 2
    if (fecha_url <= fecha_ultimo):
        return 2

    # Conformamos la URL de la petición
    url = URL_PREFIJO + fecha_url + URL_SUFIJO

    # Probamos a obtener el fichero, si no puede, devolvemos el error 3
    try: 
        r = requests.get(url, allow_redirects=True)
    except: 
        return 3

    # Probamos a abrir el fichero, si no se puede, quiere decir que no están los datos todavía, devolvemos 1
    try: 
        open('data.ods', 'wb').write(r.content)
    except: 
        return 1

    # En el caso de que si escriba el fichero, obtenemos los datos en ODS
    try: 
        ods = get_data("data.ods")
    except: 
        return 1

    # Obtenemos un diccionario del JSON
    datos = json.loads(json.dumps(ods, default=convertidor_fecha, indent=4))

    return datos


def obtener_fecha_estimada(porc_buscado, today):
    # Obtenemos los datos
    datos = obtener_dias_porcentajes()

    # Declaramos los arrays
    x = np.array(datos['porcentajes']).astype(np.float64).reshape((-1, 1))
    y = np.array(datos['dias']).astype(np.float64)

    # Inicializamos el modelo lineal y añadimos los datos
    model = linear_model.LinearRegression()
    model.fit(x, y)

    # Realizamos la predicción de dias
    x_predict = [[porc_buscado]]
    y_predict = int(model.predict(x_predict)[0])

    # Calculamos la fecha estimada
    fecha_est = FECHA_INICIO_VACUNACION + datetime.timedelta(y_predict)

    # Formulamos el strig con la fecha estimada
    fecha_est_str = str(fecha_est.day) + ' de ' + obtener_mes_esp(fecha_est.month) + ' del ' + str(fecha_est.year)
    
    return fecha_est_str

def obtener_fecha_actual(today):
    return str(today.day) + ' de ' + obtener_mes_esp(today.month)


def obtener_barra_progreso(porc_vac):
    simbolo_vacio = '○'
    simbolo_lleno = '●'
    return (int(porc_vac/TAMANO_BARRA_PROGRESO)*simbolo_lleno) + ( int((100/TAMANO_BARRA_PROGRESO) - int(porc_vac/TAMANO_BARRA_PROGRESO)) *simbolo_vacio)

def obtener_str_comunidad(comunidad, fila, poblacion):
    personas_vacunadas = fila[-2]
    porc_vac = round((personas_vacunadas/float(poblacion)*100), 2)
    barra = obtener_barra_progreso(porc_vac)
    return '{} → {}%\n{}\n\n'.format(comunidad, str(porc_vac).replace('.', ','), barra)

def obtener_tweets_comunidades(datos):
    comunidades = []

    # Andalucia
    comunidades.append(obtener_str_comunidad('Andalucía', datos[1], POBLACION_ANDALUCIA))
    # Aragon
    comunidades.append(obtener_str_comunidad('Aragón', datos[2], POBLACION_ARAGON))
    # Asturias
    comunidades.append(obtener_str_comunidad('Asturias', datos[3], POBLACION_ASTURIAS))
    # Baleares
    comunidades.append(obtener_str_comunidad('Baleares', datos[4], POBLACION_BALEARES))
    # Canarias
    comunidades.append(obtener_str_comunidad('Canarias', datos[5], POBLACION_CANARIAS))
    # Cantabria
    comunidades.append(obtener_str_comunidad('Cantabria', datos[6], POBLACION_CANTABRIA))
    # Castilla y León
    comunidades.append(obtener_str_comunidad('Castilla y león', datos[7], POBLACION_CASTILLA_LEON))
    # Castilla-La Mancha
    comunidades.append(obtener_str_comunidad('Castilla-La Mancha', datos[8], POBLACION_CASTILLA_MANCHA))
    # Cataluña
    comunidades.append(obtener_str_comunidad('Cataluña', datos[9], POBLACION_CATALUNA))
    # C. Valenciana
    comunidades.append(obtener_str_comunidad('C. Valenciana', datos[10], POBLACION_C_VALENCIANA))
    # Extremadura
    comunidades.append(obtener_str_comunidad('Extremadura', datos[11], POBLACION_EXTREMADURA))
    # Galicia
    comunidades.append(obtener_str_comunidad('Galicia', datos[12], POBLACION_GALICIA))
    # La rioja
    comunidades.append(obtener_str_comunidad('La Rioja', datos[13], POBLACION_LA_RIOJA))
    # Madrid
    comunidades.append(obtener_str_comunidad('Madrid', datos[14], POBLACION_MADRID))
    # Murcia
    comunidades.append(obtener_str_comunidad('Murcia', datos[15], POBLACION_MURCIA))
    # Navarra
    comunidades.append(obtener_str_comunidad('Navarra', datos[16], POBLACION_NAVARRA))
    # País Vasco
    comunidades.append(obtener_str_comunidad('País Vasco', datos[17], POBLACION_PAIS_VASCO))
    # Ceuta
    comunidades.append(obtener_str_comunidad('Ceuta', datos[18], POBLACION_CEUTA))
    # Melilla
    comunidades.append(obtener_str_comunidad('Melilla', datos[19], POBLACION_MELILLA))

    return comunidades


def publicar_tweet(tweet, comunidades, today):
    notificacion = tweet + '\n-------\n'

    # Configuramos el bbot
    auth = tweepy.OAuthHandler(TW_CONSUMER_KEY, TW_CONSUMER_SECRET)
    auth.set_access_token(TW_ACCESS_TOKEN, TW_ACCESS_TOKEN_SECRET)

    # Iniciamos el cliente
    api = tweepy.API(auth)

    # Publicamos el tweet
    hilo = api.update_status(tweet)

    # Publicamos el hilo de las comunidades
    aux = 0
    respuesta = ''
    for c in comunidades:
        if (aux <= 4):
            aux += 1
            respuesta = respuesta + c
        else:
            hilo = api.update_status(status=respuesta, in_reply_to_status_id=hilo.id, auto_populate_reply_metadata=True)
            notificacion = notificacion+respuesta+'-------\n'
            aux = 1
            respuesta = c
    hilo = api.update_status(status=respuesta, in_reply_to_status_id=hilo.id, auto_populate_reply_metadata=True)
    notificacion = notificacion+respuesta+'-------\n'

    # Aunque se haya publicado hoy, los datos que se muestran son los del día anterior (por eso el -1)
    guardar_ultima_fecha(avanzar_fecha_dias(today, 0).strftime('%Y%m%d'))

    # Mandamos notificación informando del Tweet
    sendNotification(notificacion, 'ℹ️')
    
    return 0


def main(today):
    # Obtenemos los datos y si hay fallo lo devolvemos
    datos = obtener_datos(today)
    if (isinstance(datos, int)):
        return datos

    # Extraemos los datos que nos pueden interesar
    try:
        array_totales = datos[u'Comunicación'][-2]
        dosis_administradas = array_totales[-4]
        porc_admin = round((dosis_administradas/float(POBLACION_ESP)*100), 2)
        personas_una_dosis = array_totales[-2]
        personas_una_dosis_porc = round((personas_una_dosis/float(POBLACION_ESP)*100), 2)
        personas_vacunadas = array_totales[-1]
        porc_vac = round((personas_vacunadas/float(POBLACION_ESP)*100), 2)
    except:
        return 4

    # Actualizamos los arrays de días y porcentajes
    actualizar_dias_porcentajes((today-FECHA_INICIO_VACUNACION).days, porc_vac)

    # Obtenemos el string de la fecha de hoy
    fecha_actual_str = obtener_fecha_actual(today)

    # Obtenemos el string de la fecha estimada
    fecha_estimada_str = obtener_fecha_estimada(50, today)

    # Obtenemos la barra de progreso
    barra_progreso = obtener_barra_progreso(porc_vac)

    # Formulamos el tweet
    tweet = """{} \n\n💉 Dosis administradas: {} \n👤 Personas 1 dosis: {} ({}%) \n\n💃 Personas vacunadas: {} \nPOBLACIÓN VACUNADA: {}% ✌️\n\n{}\n\n📆 Est. 50%: {}""".format(fecha_actual_str, "{:,}".format(dosis_administradas).replace(',','.'), "{:,}".format(personas_una_dosis).replace(',','.'), str(personas_una_dosis_porc).replace('.', ','), "{:,}".format(personas_vacunadas).replace(',','.'), str(porc_vac).replace('.', ','), barra_progreso, fecha_estimada_str)

    # Ontenemos los tweets de las comunidades
    comunidades = obtener_tweets_comunidades(datos[u'Comunicación'])

    # Publicamos el tweet
    publicar_tweet(tweet, comunidades, today)

    # Devolvemos 0 sino hay problemas
    return 0


def init():

    today = datetime.date.today()

    # Tratar caso en el que se pide un día anterior
    try:
        dias = int(sys.argv[1])*-1
    except:
        dias = 0
    today = avanzar_fecha_dias (today, dias)

    # Lanzar el programa principal
    resultado = main(today)

    # Tratamiento de errores
    if (resultado == 0):
        msg = 'El tweet se ha publicado correctamente'
        print(msg)
        sendNotification(msg, '✅')
    elif (resultado == 1):
        msg = 'Todavía no hay datos para mostrar'
        print(msg)
        sendNotification(msg, '⚠️')
    elif (resultado == 2):
        msg = 'Ya se han publicado los datos de {}'.format(avanzar_fecha_dias(today, 0).strftime('%d-%m-%Y'))
        print(msg)
        sendNotification(msg, '⚠️')
    elif (resultado == 3):
        msg = 'Error al obtener los datos'
        print(msg)
        sendNotification(msg, '❌')
    elif (resultado == 4):
        msg = 'El Excel ha cambiado de formato'
        print(msg)
        sendNotification(msg, '❌')


if __name__ == "__main__":
    init()