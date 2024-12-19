import streamlit as st
import subprocess
import os

def download_and_run_notebook():
    repo_url = "https://github.com/KikeTG/Dercoparts_AP.git"
    notebook_path = "KPIs/Forecast_A_Parquet.ipynb"
    repo_name = "Dercoparts_AP"

    try:
        # Clonar el repositorio si no está clonado
        if not os.path.exists(repo_name):
            subprocess.run(["git", "clone", repo_url], check=True)
        
        # Cambiar al directorio del repositorio
        os.chdir(repo_name)

        # Abrir el notebook utilizando Jupyter
        subprocess.run(["jupyter", "notebook", notebook_path], check=True)

    except Exception as e:
        st.error(f"Error al ejecutar el notebook: {e}")

    finally:
        # Volver al directorio original
        os.chdir("..")

st.title("Ejecutor de Notebook desde GitHub")

if st.button("Ejecutar Notebook"):
    download_and_run_notebook()
