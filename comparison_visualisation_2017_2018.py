import pandas as pd
import mysql.connector
import matplotlib.pyplot as plt
import os

# -----------------------------
# Create output folder
# -----------------------------
os.makedirs("plots/2017_2018", exist_ok=True)

# -----------------------------
# MySQL connection
# -----------------------------
conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="chogal123",
    database="wind_resource_data_management"
)

# -----------------------------
# Wind year 2017–18
# -----------------------------
start_date = "2017-11-01 00:00:00"
end_date   = "2018-11-30 23:59:59"

# -----------------------------
# Heights and database columns
# -----------------------------
heights = {
    "100 m": "wind_100m_n_avg",
    "80 m":  "wind_80m_avg",
    "50 m":  "wind_50m_avg",
    "20 m":  "wind_20m_avg"
}

# -----------------------------
# Create plot
# -----------------------------
plt.figure(figsize=(12, 5))

for label, column in heights.items():

    query = f"""
    SELECT measurement_datetime, {column}
    FROM wind_measurements
    WHERE measurement_datetime BETWEEN
          '{start_date}' AND '{end_date}'
    ORDER BY measurement_datetime;
    """

    df = pd.read_sql(query, conn)

    df["measurement_datetime"] = pd.to_datetime(df["measurement_datetime"])
    df[column] = pd.to_numeric(df[column], errors="coerce")
    df.set_index("measurement_datetime", inplace=True)

    daily_avg = df.resample("D").mean().dropna()

    plt.plot(daily_avg.index, daily_avg[column], label=label, linewidth=1.5)

# -----------------------------
# Final formatting
# -----------------------------
plt.xlabel("Date")
plt.ylabel("Daily Average Wind Speed (m/s)")
plt.title("Comparison of Daily Average Wind Speed at Different Heights (2017–18)")
plt.legend()
plt.grid(True)
plt.tight_layout()

# -----------------------------
# Save and show
# -----------------------------
plt.savefig("plots/2017_2018/comparison_all_heights.png", dpi=300)
plt.show()

# -----------------------------
# Close connection
# -----------------------------
conn.close()

