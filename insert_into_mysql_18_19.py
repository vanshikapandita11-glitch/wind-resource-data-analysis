import pandas as pd
import mysql.connector
import numpy as np

# -----------------------------
# Load cleaned 2018–19 data
# -----------------------------
df = pd.read_csv(
    "wind_data_cleaned_18_19.csv",
    parse_dates=["measurement_datetime"]
)

# Replace NaN with None for MySQL
df = df.replace({np.nan: None, "nan": None})

# -----------------------------
# Connect to MySQL
# -----------------------------
conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="chogal123",
    database="wind_resource_data_management"
)

cursor = conn.cursor()

# -----------------------------
# Insert query
# -----------------------------
insert_query = """
INSERT INTO wind_measurements (
    measurement_datetime,
    wind_100m_n_avg,
    wind_100m_s_avg,
    wind_80m_avg,
    wind_50m_avg,
    wind_20m_avg,
    wind_10m_avg,
    pressure_mbar,
    wind_direction_98m,
    wind_direction_78m,
    wind_direction_48m,
    temperature_5m,
    humidity_5m,
    data_source,
    data_quality,
    quality_notes
)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

# -----------------------------
# Insert rows
# -----------------------------
for _, row in df.iterrows():
    values = tuple(
        None if pd.isna(x) else x
        for x in (
            row["measurement_datetime"],
            row["wind_100m_n_avg"],
            row["wind_100m_s_avg"],
            row["wind_80m_avg"],
            row["wind_50m_avg"],
            row["wind_20m_avg"],
            row["wind_10m_avg"],
            row["pressure_mbar"],
            row["wind_direction_98m"],
            row["wind_direction_78m"],
            row["wind_direction_48m"],
            row["temperature_5m"],
            row["humidity_5m"],
            row["data_source"],
            row["data_quality"],
            row["quality_notes"]
        )
    )
    cursor.execute(insert_query, values)

# -----------------------------
# Commit and close
# -----------------------------
conn.commit()
cursor.close()
conn.close()

print("2018–19 data inserted into MySQL successfully.")

