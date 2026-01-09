import pandas as pd
import mysql.connector
import matplotlib.pyplot as plt
import os

# -----------------------------
# Create output folder
# -----------------------------
os.makedirs("plots/2018_2019", exist_ok=True)

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
# Wind year date range
# -----------------------------
start_date = "2018-12-01 00:00:00"
end_date   = "2019-11-30 23:59:59"

# -----------------------------
# Heights and database columns
# -----------------------------
heights = {
    "100m": "wind_100m_n_avg",
    "80m":  "wind_80m_avg",
    "50m":  "wind_50m_avg",
    "20m":  "wind_20m_avg"
}

# -----------------------------
# Loop through each height
# -----------------------------
for height_label, column_name in heights.items():

    print(f"Processing {height_label} data...")

    query = f"""
    SELECT
        measurement_datetime,
        {column_name}
    FROM wind_measurements
    WHERE measurement_datetime BETWEEN
          '{start_date}' AND '{end_date}'
    ORDER BY measurement_datetime;
    """

    df = pd.read_sql(query, conn)

    # -----------------------------
    # Data preparation
    # -----------------------------
    df["measurement_datetime"] = pd.to_datetime(df["measurement_datetime"])
    df[column_name] = pd.to_numeric(df[column_name], errors="coerce")

    df.set_index("measurement_datetime", inplace=True)

    # -----------------------------
    # Daily averaging
    # -----------------------------
    daily_avg = df.resample("D").mean()
    daily_avg = daily_avg.dropna()

    print(f"  Days plotted: {len(daily_avg)}")

    # -----------------------------
    # Plot
    # -----------------------------
    plt.figure(figsize=(12, 5))
    plt.plot(daily_avg.index, daily_avg[column_name], linewidth=1.5)

    plt.xlabel("Date")
    plt.ylabel("Daily Average Wind Speed (m/s)")
    plt.title(f"Daily Average Wind Speed at {height_label} (2018–19)")
    plt.grid(True)
    plt.tight_layout()

    # -----------------------------
    # Save plot
    # -----------------------------
    filename = f"plots/2018_2019/daily_{height_label}.png"
    plt.savefig(filename, dpi=300)
    plt.close()

    print(f"  Saved: {filename}")

# -----------------------------
# Close database connection
# -----------------------------
conn.close()

print("All plots generated successfully.")

