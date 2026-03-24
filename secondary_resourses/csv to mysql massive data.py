import streamlit as st
import mysql.connector
import pandas as pd
import io

BATCH_SIZE = 100


def connect_db(host, port, user, password, database):
    return mysql.connector.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        database=database,
        connection_timeout=25000,
        command_timeout=25000

    )


st.title("CSV → MySQL Loader")

# -----------------------------
# DATABASE CREDENTIALS
# -----------------------------

host = st.text_input("Host")
port = st.number_input("Port", value=4000)
user = st.text_input("Username")
password = st.text_input("Password", type="password")
database = st.text_input("Database Name")

if st.button("Test Connection"):

    try:
        conn = connect_db(host, port, user, password, database)
        st.success("Connection Successful")
        conn.close()
    except Exception as e:
        st.error(e)

# -----------------------------
# CSV UPLOAD
# -----------------------------

uploaded_files = st.file_uploader(
    "Upload CSV Files",
    type=["csv"],
    accept_multiple_files=True
)

if uploaded_files:

    if st.button("Import Files"):

        conn = connect_db(host, port, user, password, database)
        cursor = conn.cursor()

        for file in uploaded_files:

            table_name = file.name.replace(".csv", "")

            st.write(f"Processing {table_name}")

            df = pd.read_csv(file)

            columns = ", ".join([f"`{c}` TEXT" for c in df.columns])

            cursor.execute(
                f"CREATE TABLE IF NOT EXISTS `{table_name}` ({columns})"
            )

            cols = ", ".join([f"`{c}`" for c in df.columns])
            placeholders = ", ".join(["%s"] * len(df.columns))

            insert_query = f"""
            INSERT INTO `{table_name}` ({cols})
            VALUES ({placeholders})
            """

            data = df.values.tolist()

            progress = st.progress(0)

            total = len(data)

            for i in range(0, total, BATCH_SIZE):

                batch = data[i:i + BATCH_SIZE]

                try:
                    cursor.executemany(insert_query, batch)
                    conn.commit()
                except Exception as e:
                    st.error(e)
                    conn.rollback()
                    break

                progress.progress(min((i + BATCH_SIZE) / total, 1.0))

            st.success(f"{table_name} imported")

        cursor.close()
        conn.close()