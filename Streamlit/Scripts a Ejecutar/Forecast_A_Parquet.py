#!/usr/bin/env python
# coding: utf-8

# Crear Carpeta Mes ACTUAL

# COPIAR FORECAST AUNQUE ESTE ABIERTO (FORECAST ACTUAL, editar ruta de origen para el pasado si es necesario)

# In[1]:


import pandas as pd
import os
from datetime import datetime
import shutil
import openpyxl

ruta_origen = r"C:\Users\etorres.DERCOPARTS\DERCO CHILE REPUESTOS SpA\Planificación y abastecimiento - Documentos\Planificación y Compras AFM\Forecast Inbound\2024-12 Diciembre\12.2024 S&OP Demanda Sin restricciones Inbound Ciclo Enero 25_Inbound.xlsx"

def find_base_sheet(ruta_excel):
    xls = pd.ExcelFile(ruta_excel)
    for sheet in xls.sheet_names:
        if sheet.lower() == 'base':
            return sheet
    return None

def clean_dataframe(df):
    df.columns = df.columns.astype(str)
    for column in df.columns:
        if df[column].dtype == 'object':
            try:
                df[column] = df[column].astype(str)
            except:
                pass
    return df

def excel_to_parquet(ruta_excel, ruta_parquet):
    base_sheet = find_base_sheet(ruta_excel)
    if base_sheet:
        df = pd.read_excel(ruta_excel, sheet_name=base_sheet, skiprows=2, engine='openpyxl')
        df = clean_dataframe(df)
        df.to_parquet(ruta_parquet)
    else:
        print(f"No se encontró la hoja 'BASE' en el archivo {ruta_excel}")

usuario = os.getlogin()
anio_actual = datetime.now().year
mes_actual = datetime.now().month

ruta_guardado_excel = r"C:\Users\etorres.DERCOPARTS\DERCO CHILE REPUESTOS SpA\Planificación y abastecimiento - Documentos\Planificación y Compras AFM\S&OP Demanda\Codigos Demanda\PWBI Forecast\Evolutivo"
ruta_guardado_parquet = r"C:\Users\etorres.DERCOPARTS\DERCO CHILE REPUESTOS SpA\Planificación y abastecimiento - Documentos\Planificación y Compras AFM\S&OP Demanda\Codigos Demanda\PWBI Forecast"

# Crear las carpetas si no existen
if not os.path.exists(ruta_guardado_excel):
    os.makedirs(ruta_guardado_excel)

if not os.path.exists(ruta_guardado_parquet):
    os.makedirs(ruta_guardado_parquet)

nombre_archivo = os.path.basename(ruta_origen)
ruta_copiada = os.path.join(ruta_guardado_excel, nombre_archivo)

# Copiar el archivo utilizando shutil, aunque esté abierto
shutil.copy2(ruta_origen, ruta_copiada)

nombre_parquet_actual = os.path.splitext(nombre_archivo)[0] + "_Forecast.parquet"
ruta_parquet_actual = os.path.join(ruta_guardado_parquet, nombre_parquet_actual)
excel_to_parquet(ruta_copiada, ruta_parquet_actual)