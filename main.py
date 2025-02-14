import pandas as pd
import yfinance as yf
import psycopg2
import yaml
from psycopg2.extras import execute_values
from concurrent.futures import ThreadPoolExecutor

# Load configuration from YAML file
with open("config.yaml", "r") as file:
    config = yaml.safe_load(file)

class DatabaseManager:
    """Handles PostgreSQL database operations."""
    
    def __init__(self):
        self.db_config = config["database"]
        self.conn = psycopg2.connect(**self.db_config)
        self.create_table()

    def create_table(self):
        """Creates the stock_data table if it doesn't exist."""
        query = """
        CREATE TABLE IF NOT EXISTS stock_data (
            stock_symbol TEXT,
            date DATE,
            open_price FLOAT,
            high_price FLOAT,
            low_price FLOAT,
            close_price FLOAT,
            volume BIGINT,
            PRIMARY KEY (stock_symbol, date)
        );
        """
        with self.conn.cursor() as cursor:
            cursor.execute(query)
            self.conn.commit()
        print("Table is ready.")

    def clear_data(self):
        """Deletes all data from stock_data before inserting new data."""
        with self.conn.cursor() as cursor:
            cursor.execute("DELETE FROM stock_data;")
            self.conn.commit()
        print("Deleted all existing stock data.")

    def insert_data(self, df):
        """Inserts stock data into PostgreSQL."""
        if df is None or df.empty:
            return
        
        query = """
        INSERT INTO stock_data (date, stock_symbol, open_price, high_price, low_price, close_price, volume)
        VALUES %s ON CONFLICT (stock_symbol, date) DO NOTHING;
        """
        
        data_tuples = list(df.itertuples(index=False, name=None))
        with self.conn.cursor() as cursor:
            execute_values(cursor, query, data_tuples)
            self.conn.commit()
        
        print(f"Inserted {len(df)} records for {df['Symbol'][0]}.")

    def close_connection(self):
        """Closes the database connection."""
        self.conn.close()


class StockFetcher:
    """Fetches historical stock data from Yahoo Finance."""

    def __init__(self):
        self.period = config["fetch_settings"]["period"]

    def fetch_data(self, symbol):
        """Fetches stock data for a given symbol."""
        try:
            stock = yf.Ticker(symbol+".NS")
            df = stock.history(period=self.period)
            
            if df.empty:
                print(f"No data found for {symbol}. Skipping...")
                return None
            
            df.reset_index(inplace=True)
            df["Symbol"] = symbol  # Add stock symbol column
            
            return df[["Date", "Symbol", "Open", "High", "Low", "Close", "Volume"]]
        
        except Exception as e:
            print(f"Error fetching data for {symbol}: {e}")
            return None



class StockDataPipeline:
    """Orchestrates the data fetching and storage process."""

    def __init__(self):
        self.db_manager = DatabaseManager()
        self.fetcher = StockFetcher()
        self.stock_symbols = self.load_stock_symbols()
        self.db_manager.clear_data()  # Delete existing data before inserting new

    def load_stock_symbols(self):
        """Loads stock symbols from CSV file."""
        csv_file = config["fetch_settings"]["csv_file"]
        df = pd.read_csv(csv_file)
        return df["Symbol"].tolist()

    def fetch_and_store(self, symbol):
        """Fetch and store data for a single stock."""
        print(f"Fetching data for {symbol}...")
        stock_df = self.fetcher.fetch_data(symbol)
        self.db_manager.insert_data(stock_df)

    def run(self):
        """Runs the full pipeline with multithreading."""
        with ThreadPoolExecutor(max_workers=10) as executor:
            executor.map(self.fetch_and_store, self.stock_symbols)

        self.db_manager.close_connection()
        print("Data fetching and storing complete.")

# Run the pipeline
if __name__ == "__main__":
    pipeline = StockDataPipeline()
    pipeline.run()
