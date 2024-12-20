import streamlit as st
import subprocess
import os
import time
import requests

st.set_page_config(page_title="Menu Scripts", page_icon="⏱️")
st.title("Menú de Ejecución de Scripts")

opcion = st.radio("Selecciona el Script a Ejecutar:", ("Disponibilidad Futura por Canales (GitHub)", "Forecast a Parquet (GitHub)"))

def limpiar_conflictos(contenido):
    lineas = contenido.split("\n")
    filtradas = []
    for linea in lineas:
        if not (linea.startswith("<<<<<<<") or linea.startswith(">>>>>>>") or linea.startswith("=======")):
            filtradas.append(linea)
    return "\n".join(filtradas)

def instalar_dependencias():
    subprocess.run(["python", "-m", "pip", "install", "polars"], capture_output=True, text=True, encoding="utf-8")

if opcion == "Disponibilidad Futura por Canales (GitHub)":
    semana = st.number_input("Semana (1-4):", min_value=1, max_value=4, step=1)
    tienda = st.selectbox("Incluir tiendas:", options=[0, 1], format_func=lambda x: "No" if x == 0 else "Sí")
    faltan = st.selectbox("Incluir faltantes:", options=[0, 1], format_func=lambda x: "No" if x == 0 else "Sí")
    url_script = "https://raw.githubusercontent.com/KikeTG/Dercoparts_AP/main/Dispo/Dispo-Canal2.py"
    if st.button("Descargar, Instalar Dependencias y Ejecutar"):
        with st.spinner("Descargando..."):
            r = requests.get(url_script)
            contenido_limpio = limpiar_conflictos(r.text)
            script_path = "Dispo-Canal2_temp.py"
            with open(script_path, "w", encoding="utf-8") as f:
                f.write(contenido_limpio)
        with st.spinner("Instalando dependencias..."):
            instalar_dependencias()
        start_time = time.time()
        with st.spinner("Ejecutando..."):
            result = subprocess.run(["python", script_path, str(semana), str(tienda), str(faltan)], capture_output=True, text=True, encoding="utf-8")
        total_time = time.time() - start_time
        st.success(f"Ejecución completada en {total_time:.2f} segundos.")
        st.code(result.stdout, language='text')
        if result.stderr:
            st.error("Errores detectados:")
            st.code(result.stderr, language='text')
        os.remove(script_path)

elif opcion == "Forecast a Parquet (GitHub)":
    url_script = "https://raw.githubusercontent.com/KikeTG/Dercoparts_AP/main/Streamlit/Scripts%20a%20Ejecutar/Forecast_A_Parquet.py"
    if st.button("Descargar, Instalar Dependencias y Ejecutar"):
        with st.spinner("Descargando..."):
            r = requests.get(url_script)
            contenido_limpio = limpiar_conflictos(r.text)
            script_path = "Forecast_A_Parquet_temp.py"
            with open(script_path, "w", encoding="utf-8") as f:
                f.write(contenido_limpio)
        with st.spinner("Instalando dependencias..."):
            instalar_dependencias()
        start_time = time.time()
        with st.spinner("Ejecutando..."):
            result = subprocess.run(["python", script_path], capture_output=True, text=True, encoding="utf-8")
        total_time = time.time() - start_time
        st.success(f"Ejecución completada en {total_time:.2f} segundos.")
        st.code(result.stdout, language='text')
        if result.stderr:
            st.error("Errores detectados:")
            st.code(result.stderr, language='text')
        os.remove(script_path)
