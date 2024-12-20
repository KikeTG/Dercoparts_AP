import streamlit as st
import subprocess
import os
import time

# Configuración de la página
st.set_page_config(page_title="Disponibilidad Futura por Canales", page_icon="⏱️")

# Título de la aplicación
st.title("📊 Disponibilidad Futura por Canales")

# Descripción breve
st.markdown("""
Bienvenido a la aplicación de **Disponibilidad Futura por Canales**. Aquí puedes configurar las variables necesarias y ejecutar el código automáticamente.
""")

# Línea divisoria
st.markdown("---")

# Inputs organizados en columnas
col1, col2, col3 = st.columns(3)

with col1:
    semana = st.number_input("**Semana (1-4):**", min_value=1, max_value=4, step=1)
with col2:
    tienda = st.selectbox("**Incluir tiendas:**", options=[0, 1], format_func=lambda x: "No" if x == 0 else "Sí")
with col3:
    faltan = st.selectbox("**Incluir faltantes:**", options=[0, 1], format_func=lambda x: "No" if x == 0 else "Sí")
ruta_script = os.path.join(
    os.path.expanduser("~"),
    r"DERCO CHILE REPUESTOS SpA\Planificación y abastecimiento - Documentos\Planificación y Compras AFM\S&OP Demanda\Codigos Demanda\Dispo-Canal2.py"
)


# Botón para ejecutar el código
if st.button("🚀 Ejecutar Código"):
    # Verifica si el archivo existe
    if not os.path.exists(ruta_script):
        st.error(f"⚠️ No se encontró el archivo `{ruta_script}`. Verifica la ruta.")
    else:
        # Inicia el cronómetro
        start_time = time.time()
        
        # Ejecuta el script
        with st.spinner("⏳ Ejecutando el código, por favor espera..."):
            result = subprocess.run(
                ["python", ruta_script, str(semana), str(tienda), str(faltan)],
                capture_output=True,
                text=True,
                encoding="utf-8"
            )
        
        # Calcular tiempo total
        total_time = time.time() - start_time

        # Mostrar la salida del script
        st.success(f"✅ **Ejecución completada en {total_time:.2f} segundos.**")
        st.markdown("### 📋 Salida del Script:")
        st.code(result.stdout, language='text')
        
        # Mostrar errores, si los hay
        if result.stderr:
            st.error("❌ **Errores detectados:**")
            st.code(result.stderr, language='text')
        
# Línea divisoria final
st.markdown("---")
st.markdown("Desarrollado por [P&A] © 2024")