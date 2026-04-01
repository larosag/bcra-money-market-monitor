
import requests
import pandas as pd
import sqlite3
from datetime import date 

DB_PATH = "tasas_bancos.db"

# 1. Extraer promedio ponderado del sistema (BCRA)

url_bcra = "https://api.bcra.gob.ar/estadisticas/v4.0/Monetarias/12"
response = requests.get(url_bcra, timeout=10)
response.raise_for_status()
data_monetaria = response.json()

valor_promedio = data_monetaria["results"][0]["detalle"][0]["valor"]
 

# 2. Extraer tasas por banco 
url_bancos = "https://www.bcra.gob.ar/api-plazos-fijos.php"
response_bancos = requests.get(url_bancos, timeout=10)
response_bancos.raise_for_status()
data_bancos = response_bancos.json()
 
df_top10 = pd.DataFrame(data_bancos["top10"])
df_otros = pd.DataFrame(data_bancos["otros"])
df_bancos = pd.concat([df_top10, df_otros], ignore_index=True) 

# 3. Transformar

df_bancos = df_bancos[["codigo", "entidad", "tasa_con_relacion"]]
df_bancos = df_bancos.rename(columns={
    "entidad": "banco",
    "tasa_con_relacion": "tasa_pf_30d"
})
 
fecha_hoy = date.today()
df_bancos["fecha"] = fecha_hoy
df_bancos["promedio_bcra"] = valor_promedio
df_bancos["spread"] = df_bancos["tasa_pf_30d"] - df_bancos["promedio_bcra"] 

# 4. Normalizar nombres 

df_bancos["banco"] = df_bancos["banco"].str.strip().str.upper()

# 5. Carga a SQLite

conn = sqlite3.connect(DB_PATH)

tabla_existe = conn.execute("""
    SELECT name FROM sqlite_master 
    WHERE type='table' AND name='tasas_bancos'
""").fetchone()
 
if tabla_existe:
    eliminados = conn.execute(
        "DELETE FROM tasas_bancos WHERE fecha = ?",
        (fecha_hoy.isoformat(),)
    ).rowcount
    conn.commit()
    if eliminados > 0:
        print(f"Se eliminaron {eliminados} registros previos de {fecha_hoy} (re-ejecución)")
 
df_bancos.to_sql(
    "tasas_bancos",
    conn,
    if_exists="append",
    index=False
)
 
conn.commit()

# Verificación
total = conn.execute("SELECT COUNT(*) FROM tasas_bancos").fetchone()[0]
fechas = conn.execute("SELECT COUNT(DISTINCT fecha) FROM tasas_bancos").fetchone()[0]
conn.close()
 
print(f"Carga exitosa: {len(df_bancos)} registros insertados")
print(f" Base total: {total} registros | {fechas} fecha(s) distintas")