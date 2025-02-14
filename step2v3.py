import psycopg2
import pandas as pd
import yaml
from psycopg2.extras import execute_values
import numpy as np

class WeeklyVolumeAnalyzer:
    """Analyzes weekly stock volume and stores high-volume weeks in the database."""

    def __init__(self, config_file="config.yaml"):
        with open(config_file, "r") as file:
            self.config = yaml.safe_load(file)
        
        self.db_config = self.config["database"]
        self.conn = psycopg2.connect(
            dbname=self.db_config["dbname"],
            user=self.db_config["user"],
            password=self.db_config["password"],
            host=self.db_config["host"],
            port=self.db_config["port"]
        )
        self.conn.autocommit = True  # Ensure immediate commit of changes

    def clear_data(self):
        """Deletes all existing data from high_volume_weeks table."""
        with self.conn.cursor() as cursor:
            cursor.execute("DELETE FROM high_volume_weeks;")
        print("Deleted previous high-volume week data.")

    def fetch_data(self):
        """Fetches stock data including volume, open, close, high, and low prices."""
        query = """
        SELECT stock_symbol, date, volume, open_price, close_price, high_price, low_price
        FROM stock_data 
        ORDER BY stock_symbol, date;
        """
        with self.conn.cursor() as cursor:
            cursor.execute(query)
            rows = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
        return pd.DataFrame(rows, columns=columns)

    def calculate_rsi(self, prices, window=14):
        """Computes RSI for a given price series."""
        delta = prices.diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=window).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=window).mean()
        rs = gain / loss
        rsi = 100 - (100 / (1 + rs))
        return rsi

    def analyze_and_store(self):
        """Analyzes weekly data and stores high-volume weeks with RSI & Marubozu condition."""
        df = self.fetch_data()
        if df.empty:
            print("No data found in stock_data table.")
            return

        df["date"] = pd.to_datetime(df["date"])
        df.set_index("date", inplace=True)

        results = []
        for symbol, group in df.groupby("stock_symbol"):
            # Resample weekly, summing volume and checking for green candles
            weekly_df = group.resample("W").agg({
                "volume": "sum",
                "open_price": "first",  # First day's open
                "close_price": "last",  # Last day's close
                "high_price": "max",
                "low_price": "min"
            }).reset_index()
            
            weekly_df["week_start"] = weekly_df["date"] - pd.Timedelta(days=6)

            # Marubozu Condition: Open = Low, Close = High
            # Allow a small wick: Open should be near Low, Close should be near High
            weekly_df["is_marubozu"] = (
                (weekly_df["open_price"] <= weekly_df["low_price"] * 1.03)& # Open close to Low (0.2% tolerance)
                (weekly_df["close_price"] >= weekly_df["high_price"] * 0.97) # Close close to High (0.2% tolerance)
            )


            # Calculate rolling 6-week average volume
            rolling_avg = weekly_df["volume"].rolling(window=6).mean().shift(1)
            volume_multiple = weekly_df["volume"] / rolling_avg

            # Compute RSI
            weekly_df["rsi"] = self.calculate_rsi(weekly_df["close_price"])
            # Ensure previous week was green (Close > Open) & Volume < 50% of current week
            weekly_df["prev_close"] = weekly_df["close_price"].shift(1)
            weekly_df["prev_open"] = weekly_df["open_price"].shift(1)
            weekly_df["prev_volume"] = weekly_df["volume"].shift(1)

            weekly_df["prev_week_green"] = weekly_df["prev_close"] > weekly_df["prev_open"]
            weekly_df["prev_week_low_volume"] = weekly_df["prev_volume"] < (weekly_df["volume"] * 0.5)

            # Filter for valid high-volume weeks (Volume Multiple 4-15, Marubozu, RSI 50-70)
            high_vol_weeks = weekly_df[
                (volume_multiple >= 3) &
                (weekly_df["volume"] >500000) &
                (volume_multiple <= 15) &
                (weekly_df["is_marubozu"]) & 
                (weekly_df["rsi"] >= 40) & 
                (weekly_df["rsi"] <= 95)&
                (weekly_df["prev_week_green"]) &  # Previous week must be green
                (weekly_df["prev_week_low_volume"])  # Previous volume must be < 50% of current week
            ].copy()

            high_vol_weeks["volume_multiple"] = volume_multiple[high_vol_weeks.index]

            for _, row in high_vol_weeks.iterrows():
                results.append((
                    symbol,
                    row["week_start"].date(),
                    row["date"].date(),
                    row["volume"],
                    row["volume_multiple"],
                    row["rsi"]
                ))

        if results:
            self.store_results(results)
            self.export_to_csv()
        else:
            print("No high-volume weeks found.")

    def store_results(self, results):
        """Stores high-volume weeks in PostgreSQL."""
        if not results:
            print("No results to insert.")
            return

        print(f"Inserting {len(results)} rows into high_volume_weeks...")
        print(results[:5])  # Print first 5 rows for debugging

        query = """
        INSERT INTO high_volume_weeks (stock_symbol, week_start_date, week_end_date, weekly_volume, volume_multiple, rsi_value)
        VALUES %s
        ON CONFLICT (stock_symbol, week_start_date) DO NOTHING;
        """
        with self.conn.cursor() as cursor:
            execute_values(cursor, query, results)
        print(f"Inserted {len(results)} high-volume weeks.")

    def export_to_csv(self):
        """Exports the high_volume_weeks table to a CSV file."""
        query = "SELECT * FROM high_volume_weeks ORDER BY stock_symbol, week_start_date;"
        df = pd.read_sql(query, self.conn)

        file_path = "high_volume_weeks.csv"
        df.to_csv(file_path, index=False)
        print(f"Exported high-volume weeks to {file_path}")

    def close_connection(self):
        """Closes the database connection."""
        self.conn.close()

if __name__ == "__main__":
    analyzer = WeeklyVolumeAnalyzer()
    analyzer.clear_data()
    analyzer.analyze_and_store()
    analyzer.close_connection()
