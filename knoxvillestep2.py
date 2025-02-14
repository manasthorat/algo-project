import pandas as pd
import psycopg2
import yaml
import ta

class KnoxvilleDivergenceAnalyzer:
    """Analyzes Knoxville Divergence and stores bullish signals in the database."""
    
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
        self.conn.autocommit = True
        self.create_table()
        self.clear_table()
    
    def create_table(self):
        query = """
        CREATE TABLE IF NOT EXISTS bullish_knoxville_divergence (
            stock_symbol TEXT,
            date DATE,
            close_price DOUBLE PRECISION,
            PRIMARY KEY (stock_symbol, date)
        );
        """
        with self.conn.cursor() as cursor:
            cursor.execute(query)
    
    def clear_table(self):
        query = "DELETE FROM bullish_knoxville_divergence;"
        with self.conn.cursor() as cursor:
            cursor.execute(query)
    
    def fetch_stock_data(self, symbol):
        query = f"SELECT date, close_price FROM stock_data WHERE stock_symbol = '{symbol}' ORDER BY date ASC;"
        with self.conn.cursor() as cursor:
            cursor.execute(query)
            rows = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
        return pd.DataFrame(rows, columns=columns)
    
    def knoxville_divergence(self, df, rsi_period=14, ema_period=200):
        df['RSI'] = ta.momentum.RSIIndicator(df['close_price'], window=rsi_period).rsi()
        df['EMA'] = ta.trend.EMAIndicator(df['close_price'], window=ema_period).ema_indicator()
        df['RSI_EMA'] = ta.trend.EMAIndicator(df['RSI'], window=ema_period).ema_indicator()
        df['Momentum'] = df['RSI'] - df['RSI_EMA']
        df['Bullish_Divergence'] = (df['Momentum'] > 0) & (df['Momentum'].shift(1) < 0)
        return df[df['Bullish_Divergence']][['date', 'close_price']]
    
    def save_bullish_divergence(self, symbol, df):
        query = """
        INSERT INTO bullish_knoxville_divergence (stock_symbol, date, close_price)
        VALUES (%s, %s, %s) 
        ON CONFLICT DO NOTHING;
        """
        with self.conn.cursor() as cursor:
            for _, row in df.iterrows():
                cursor.execute(query, (symbol, row['date'], row['close_price']))
    
    def analyze_and_store(self):
        stock_list = pd.read_csv('knoxville_stock_list.csv')
        results = []
        for symbol in stock_list['Symbol']:
            df = self.fetch_stock_data(symbol)
            if not df.empty:
                bullish_divergence = self.knoxville_divergence(df)
                if not bullish_divergence.empty:
                    self.save_bullish_divergence(symbol, bullish_divergence)
                    bullish_divergence['stock_symbol'] = symbol
                    results.append(bullish_divergence)
        
        if results:
            final_df = pd.concat(results)
            final_df.to_csv('bullish_knoxville_divergence.csv', index=False)
            print("Exported results to bullish_knoxville_divergence.csv")
    
    def close_connection(self):
        self.conn.close()
    
if __name__ == "__main__":
    analyzer = KnoxvilleDivergenceAnalyzer()
    analyzer.analyze_and_store()
    analyzer.close_connection()
