import pandas as pd

# Cargar archivos
breq = pd.read_csv("breq.csv")
panas = pd.read_csv("panas.csv")
personality = pd.read_csv("personality.csv")
stai = pd.read_csv("stai.csv")
ttm = pd.read_csv("ttm.csv")
daily = pd.read_csv("daily_fitbit_sema_df_unprocessed.csv")

# Renombrar columna de fecha y convertir a datetime
for df in [breq, panas, personality, stai, ttm]:
    df.rename(columns={"submitdate": "date"}, inplace=True)
    df["date"] = pd.to_datetime(df["date"])
daily["date"] = pd.to_datetime(daily["date"])

# Eliminar columnas innecesarias
for df in [breq, panas, personality, stai, ttm]:
    df.drop(columns=["Unnamed: 0", "type"], inplace=True, errors="ignore")
daily.drop(columns=["Unnamed: 0"], inplace=True, errors="ignore")

# Merge INNER para conservar solo fechas comunes
merged_inner = breq.merge(panas, on=["user_id", "date"], how="inner") \
                   .merge(personality, on=["user_id", "date"], how="inner") \
                   .merge(stai, on=["user_id", "date"], how="inner") \
                   .merge(ttm, on=["user_id", "date"], how="inner") \
                   .merge(daily, left_on=["user_id", "date"], right_on=["id", "date"], how="left")

# Guardar archivo intermedio
merged_inner.to_csv("datos_procesados_completos.csv", index=False)

# Columnas de motivación a pivotar
motivation_cols = [
    'breq_amotivation',
    'breq_external_regulation',
    'breq_introjected_regulation',
    'breq_identified_regulation',
    'breq_intrinsic_regulation'
]

# Columnas que se mantienen
id_vars = [col for col in merged_inner.columns if col not in motivation_cols]

# Pivotar
df_pivot = merged_inner.melt(
    id_vars=id_vars,
    value_vars=motivation_cols,
    var_name="Tipo de Motivación",
    value_name="Valor de Motivación"
)

# Mejorar nombres
df_pivot["Tipo de Motivación"] = (
    df_pivot["Tipo de Motivación"]
    .str.replace("breq_", "", regex=False)
    .str.replace("_", " ")
    .str.title()
)

# Guardar archivo para Tableau
df_pivot.to_csv("datos_pivotados_para_tableau.csv", index=False)