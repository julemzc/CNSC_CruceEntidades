#Librerias
import pandas as pd
import numpy as np
import math
import re
import yaml
import psycopg2
import xlsxwriter
from datetime import datetime
from unidecode import unidecode


# import datos de configuración
def load_config(config_file='base/config.yaml'):
    with open(config_file, 'r', encoding='utf-8') as file:
        return yaml.safe_load(file)

# imprimir en el log
def lprint(mensaje):
    log_file = load_config()['log_dir'] + "log_" + datetime.now().strftime('%Y%m%d') + ".txt"
    log = datetime.now().strftime('%Y-%h-%d-%H:%M:%S') + ', ' + mensaje + '\n'
    print(log)
    with open(log_file, "a") as f:
        f.write(log)


## Abrir y cerrar conexiones
def abrirConexion(conn_name):
    config = load_config().get(conn_name, {})
    try:
        lprint(f"Conectando a: {config.get('host', 'host no especificado')}")
#        return create_engine('postgresql+psycopg2://'+miUser+':'+miPass+'@'+miHost+':'+miPort+'/'+miDB)
        conn = psycopg2.connect(
            user=config['user'],
            password=config['pass'],
            host=config['host'],
            port=config['port'],
            database=config['db']
        )
        return conn, config['schema']
    except psycopg2.Error as e:
        lprint(f" Error al conectar con PostgreSQL ({conn_name}): {e}")
        return None, None

def cerrarConexion(conexion):
    try:
        if(conexion):
            conexion.close()
    except Exception as e:
        print(e)

## Conexión a Bases de Datos
def openWayu():
    return abrirConexion('conn_wayu')

def fConsultaScript(conx, query):
  dfS = None
  try:
    dfS = pd.read_sql(query, conx)
    print(datetime.now(),"DF Query Consultado from ",re.search(r'\bFROM\s+(\w+)', query, re.IGNORECASE).group(1))
  except Exception as e:
    res = str(e)
 # finally:
#    cerrarConexion(conx)
  return dfS

# Consultas
def FcdEntidad(conx):
  query =r"""
  SELECT id, nit, nombre
  FROM fcd2.entidad;
  """
  df = fConsultaScript(conx()[0],query)
  return df

def RpcaEntidad(conx):
  query =r"""
    SELECT fuente, nit, nombre_entidad, 
    REPLACE(REPLACE(nombre_seccional, 'COMISION SECCIONAL DE ',''),'SIN INFORMACIÓN','') seccional,
    count(*) conteo
    FROM rpca.vista_rpca_completa
    WHERE fcd_entidad_id IS NULL
    GROUP BY 1,2,3,4
    ORDER BY 3
    """
  query =r"""
    SELECT fuente, btrim(nit) nit, nombre_entidad, REPLACE(REPLACE(nombre_seccional, 'COMISION SECCIONAL DE ',''),'SIN INFORMACIÓN','') seccional,
    btrim( upper( fcd2.elimina_caraespe( regexp_replace( regexp_replace(
    CASE WHEN nombre_entidad ILIKE '%)' THEN 'ALCALDIA '||split_part(nombre_entidad,'(',1) ELSE nombre_entidad END,
    '\y(MPAL|M/PAL|MPIO|DE|DEL|MUNICIPAL|MUNICIPIO)\y', 'ALCALDIA', 'gi' ), E'[\\n\\r\\t\\s,;|]+', ' ', 'g' ) ) ) ) AS ventidad,
    CASE WHEN nombre_entidad ILIKE '%alcald%' OR nombre_entidad ILIKE '%(%' THEN 'AL'
        WHEN nombre_entidad ILIKE '%conce%' THEN 'CO'
        WHEN nombre_entidad ILIKE '%perso%' THEN 'PE'
        WHEN nombre_entidad ILIKE '%gober%' THEN 'GO'
        WHEN nombre_entidad ILIKE '%educa%' THEN 'SE'
        WHEN nombre_entidad ILIKE '%asamb%' THEN 'AS'
    ELSE '-' END tipo, count(*) conteo
    FROM rpca.vista_rpca_completa
    WHERE fcd_entidad_id IS NULL
    GROUP BY 1,2,3,4,5,6
  """
  df = fConsultaScript(conx()[0],query)
  return df

## Función que lee el Excel y lo convierte a un dataframe
def fLeerExcel(ruta, libro, hoja, fila1):
    return pd.read_excel(ruta + libro, sheet_name=hoja, header=fila1, dtype=str)

def fResultadosExcel(df, nombre):
    with pd.ExcelWriter(nombre+".xlsx", engine="xlsxwriter") as writer:
        df.to_excel(writer, sheet_name="BASE", index=False)
    lprint("Excel con los datos creado "+nombre+".xlsx")

# retirar tildes
def rRetirar_tildes(valor):
    return unidecode(str(valor).strip().upper())

# limpiar texto
def rLimpiar_texto(valor):
    valor = str(valor).strip()
    if valor == '' or valor == str(np.nan):
        valor = None
    valor = str(valor).upper()
    return valor

# función de control de nulidades
def rDatoNulo(valor):
    if pd.isna(valor):
        return None
    return valor

def rLimpiaSeccional(valor):
    if isinstance(valor, str):
        valor = valor.replace('COMISION SECCIONAL DE ', ' ').replace('SIN INFORMACIÓN', ' ').strip()
        return None if valor == '' else valor
    return valor

# Pasar los vacios a nulos, se usa con df[col] = df[col].map(rVaciosNulos)
# cols = [col1,col2] / df[[cols]] = df[[cols]].apply(lambda col: col.map(rVaciosNulos))
def rVaciosNulos(valor):
    if isinstance(valor, str):
        valor = re.sub(r'[\n\r\t,]+', ' ', valor).strip()
        return None if valor == '' else valor
    return valor

## Calcular la longitud del campo
def rTecho(maximo):
    return 2 ** math.ceil(math.log2(maximo*2))

## Corregir columna de int
def fCorregirInt(columna,es_nulo=False):
    col = columna.astype(str).str.strip().apply(
      lambda x: int(float(re.sub(r'[^\d.-]', '', x))) if re.sub(r'[^\d.-]', '', x) != '' else (pd.NA if es_nulo else -1))
    if es_nulo:
        col = col.astype('Int64')
    else:
        col = col.astype('int32' if col.max() < pow(2, 30) else 'int64')
    return col

# Corregir columna de date
def fCorregirDate(columna):
    columna = columna.astype(str).str[:10]
    col = pd.to_datetime(columna, format='%d/%m/%Y', errors='coerce')
    mask = col.isna()
    if mask.any():
        col[mask] = pd.to_datetime(columna[mask], format='%Y-%m-%d', errors='coerce')
    return col

# Convertir a string
def fCorregirString(columna):
    try:
        col = pd.to_datetime(columna).dt.strftime('%Y-%m-%d')
    except (ValueError, TypeError):
        col = columna.astype(str)
    return col

#Rango de calificación
def rangoCalificacion(col):
    condiciones = [ col >= 90, (col >= 70) & (col < 90), (col >= 50) & (col < 70), (col >= 30) & (col < 50), col < 30]
    valores = ['1.Idéntico','2.Coincide','3.Revisar','4.Dudoso','5.NO coincide']
    return np.select(condiciones, valores, default='-1.NO Dato')
