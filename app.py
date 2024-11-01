import streamlit as st
import pandas as pd
import mysql.connector
import os
import matplotlib.pyplot as plt
import requests

def get_db_connection():
    try:
        conn = mysql.connector.connect(
            host='localhost',        
            database='appweb',       
            user='root',             
            password='',             
            port=3307
        )
        return conn
    except mysql.connector.Error as err:
        st.error(f"Error al conectar a MySQL: {err}")
        return None

def eliminar_tabla(nombre_tabla):
    conn = get_db_connection()
    if conn:
        cursor = conn.cursor()
        try:
            cursor.execute(f"DROP TABLE IF EXISTS {nombre_tabla}")
            conn.commit()
            st.success(f"Tabla '{nombre_tabla}' eliminada exitosamente.")
        except Exception as e:
            st.error(f"Error al eliminar la tabla '{nombre_tabla}': {e}")
        finally:
            cursor.close()
            conn.close()

def bulk(df, nombre_tabla):
    conn = get_db_connection()
    if conn:
        cursor = conn.cursor()

        columnas = []
        foreign_keys = []  
        primary_key = df.columns[0]

        for column, dtype in df.dtypes.items():
            if pd.api.types.is_integer_dtype(dtype):
                columnas.append(f"{column} INT")
            elif pd.api.types.is_float_dtype(dtype):
                columnas.append(f"{column} FLOAT")
            elif pd.api.types.is_datetime64_any_dtype(dtype):
                columnas.append(f"{column} DATETIME")
            else:
                columnas.append(f"{column} VARCHAR(255)")
            
            if column.startswith("id_") and column != primary_key:
                referenced_table = column.split("_")[1] + "s"  
                foreign_keys.append(f"FOREIGN KEY ({column}) REFERENCES {referenced_table}({column})")

        columnas_str = ", ".join(columnas)
        primary_key_str = f"PRIMARY KEY ({primary_key})"
        foreign_keys_str = ", ".join(foreign_keys)
        table_definition = f"CREATE TABLE IF NOT EXISTS {nombre_tabla} ({columnas_str}, {primary_key_str}"

        if foreign_keys_str:
            table_definition += f", {foreign_keys_str}"

        table_definition += ")"

        try:
            cursor.execute(table_definition)
            st.write(f"Tabla '{nombre_tabla}' creada con estructura:\n{table_definition}")
        except Exception as e:
            st.error(f"Error al crear la tabla '{nombre_tabla}': {e}")

        try:
            for _, row in df.iterrows():
                cursor.execute(
                    f"INSERT INTO {nombre_tabla} ({', '.join(df.columns)}) VALUES ({', '.join(['%s'] * len(row))})",
                    tuple(row)
                )
            conn.commit()
            st.success(f"Datos de '{nombre_tabla}' cargados exitosamente con relaciones.")
        except Exception as e:
            st.error(f"Error al insertar los datos en '{nombre_tabla}': {e}")
        
        cursor.close()
        conn.close()

def contar_registros():
    conn = get_db_connection()
    if conn:
        cursor = conn.cursor()
        
        cursor.execute("SHOW TABLES;")
        tablas = cursor.fetchall()
        
        conteo = {}
        for tabla in tablas:
            cursor.execute(f"SELECT COUNT(*) FROM {tabla[0]}")
            conteo[tabla[0]] = cursor.fetchone()[0]
        
        cursor.close()
        conn.close()
        return conteo

def obtener_muestra_tabla(nombre_tabla, limite=5):
    conn = get_db_connection()
    if conn:
        query = f"SELECT * FROM {nombre_tabla} LIMIT {limite}"
        df = pd.read_sql(query, conn)
        conn.close()
        return df
    return None

st.sidebar.title("Menú")
menu = st.sidebar.radio("Navega entre las páginas:", ["Inicio", "Subir Excel", "Consultas API", "Eliminar Tabla"])

if menu == "Inicio":
    st.title("Bienvenido a la Plataforma de Gestión de Datos")
    st.write("Utiliza el menú para navegar entre las distintas secciones.")
    
    conteo_tablas = contar_registros()
    if conteo_tablas:
        st.subheader("Conteo de Registros por Tabla:")
        for tabla, cantidad in conteo_tablas.items():
            st.write(f"{tabla}: {cantidad} registros")
        
        st.subheader("Gráfico de Registros por Tabla")
        plt.bar(conteo_tablas.keys(), conteo_tablas.values(), color='skyblue')
        plt.xlabel("Tablas")
        plt.ylabel("Número de Registros")
        plt.title("Conteo de Registros en Cada Tabla")
        st.pyplot(plt)

        st.subheader("Contenido de las Tablas")
        for tabla in conteo_tablas.keys():
            st.markdown(f"### {tabla}")
            df_muestra = obtener_muestra_tabla(tabla)
            if df_muestra is not None:
                st.dataframe(df_muestra)
            else:
                st.write("No se pudo obtener una muestra de esta tabla.")
    else:
        st.write("No hay tablas en la base de datos.")

elif menu == "Subir Excel":
    st.title("Subir Archivos Excel")
    st.write("Sube tus archivos Excel para cargarlos a la base de datos.")

    nombre_tabla = st.text_input("Nombre de la tabla a crear en la base de datos:")

    archivo = st.file_uploader("Selecciona un archivo Excel", type=["xls", "xlsx"])
    if archivo and nombre_tabla: 
        df = pd.read_excel(archivo, engine="openpyxl")
        st.write(df)
        if st.button("Cargar a MySQL"):
            bulk(df, nombre_tabla)

elif menu == "Consultas API":
    st.title("Consultas API")

    st.subheader("Insertar Registro")
    nombre = st.text_input("Nombre:")
    valor = st.number_input("Valor:", format="%f")
    if st.button("Insertar Registro"):
        response = requests.post("http://127.0.0.1:8000/api/insertar", json={"nombre": nombre, "valor": valor})
        if response.status_code == 200:
            st.success("Registro insertado exitosamente.")
        else:
            st.error("Error al insertar el registro.")

    st.subheader("Consultar Registros")
    if st.button("Consultar"):
        response = requests.get("http://127.0.0.1:8000/api/consultar")
        if response.status_code == 200:
            registros = response.json()
            st.write(pd.DataFrame(registros))
        else:
            st.error("Error al consultar los registros.")

    st.subheader("Estadísticas de los Valores")
    if st.button("Obtener Estadísticas"):
        response = requests.get("http://127.0.0.1:8000/api/estadisticas")
        if response.status_code == 200:
            estadisticas = response.json()
            st.write(f"Máximo: {estadisticas['max']}")
            st.write(f"Mínimo: {estadisticas['min']}")
            st.write(f"Promedio: {estadisticas['avg']}")
        else:
            st.error("Error al obtener las estadísticas.")

elif menu == "Eliminar Tabla":
    st.title("Eliminar Tabla")
    tabla_a_eliminar = st.text_input("Ingrese el nombre de la tabla que desea eliminar:")
    
    if st.button("Eliminar Tabla"):
        if tabla_a_eliminar:
            eliminar_tabla(tabla_a_eliminar)
        else:
            st.error("Por favor, ingrese un nombre de tabla.")

st.sidebar.markdown("**© 2024 Plataforma de Gestión de Datos**")
