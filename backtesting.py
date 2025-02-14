import pandas as pd
import psycopg2
import yaml
import concurrent.futures

# Load database config
with open("config.yaml", "r") as file:
    config = yaml.safe_load(file)

# Database connection
def connect_db():
    """Connects to the PostgreSQL database."""
    try:
        return psycopg2.connect(
            dbname=config["database"]["dbname"],
            user=config["database"]["user"],
            password=config["database"]["password"],
            host=config["database"]["host"],
            port=config["database"]["port"]
        )
    except Exception as e:
        print(f"Database connection error: {e}")
        return None

# Fetch stock data
def fetch_all_stock_data():
    """Fetches all stock data once to avoid redundant queries."""
    conn = connect_db()
    if not conn:
        return None
    
    query = "SELECT stock_symbol, date, close_price FROM stock_data ORDER BY stock_symbol, date;"
    
    try:
        df = pd.read_sql(query, conn)
        conn.close()
    except Exception as e:
        print(f"Error fetching stock data: {e}")
        return None
    
    df["date"] = pd.to_datetime(df["date"])
    df.set_index(["stock_symbol", "date"], inplace=True)
    
    stock_data_dict = {symbol: df.xs(symbol, level="stock_symbol") for symbol in df.index.get_level_values("stock_symbol").unique()}
    
    print("Stock data loaded successfully.")
    return stock_data_dict

# Fetch high volume weeks with RSI, Volume Multiple, and Volume
def fetch_high_volume_weeks():
    """Fetches stocks with high volume weeks along with RSI, Volume Multiple, and Volume."""
    conn = connect_db()
    if not conn:
        return None
    
    query = """
    SELECT stock_symbol, week_end_date, volume_multiple, rsi_value, weekly_volume
    FROM high_volume_weeks;
    """
    
    try:
        df = pd.read_sql(query, conn)
        conn.close()
    except Exception as e:
        print(f"Error fetching high volume weeks: {e}")
        return None
    
    df["week_end_date"] = pd.to_datetime(df["week_end_date"])
    return df

# Find the closest previous Friday's closing price
def get_valid_entry_date(df_stock, week_end_date):
    """Finds the closest trading day before Sunday (preferably Friday)."""
    possible_days = [week_end_date - pd.Timedelta(days=i) for i in range(3)]
    for date in possible_days:
        if date in df_stock.index:
            return date, df_stock.loc[date, "close_price"]
    return None, None

# Process each stock in parallel
def process_stock(row, stock_data_dict):
    """Processes a single stock for backtesting."""
    stock = row["stock_symbol"]
    week_end_date = row["week_end_date"]
    volume_multiple = row["volume_multiple"]
    rsi_value = row["rsi_value"]
    weekly_volume = row["weekly_volume"]

    df_stock = stock_data_dict.get(stock)
    if df_stock is None:
        return None
    
    entry_date, entry_price = get_valid_entry_date(df_stock, week_end_date)
    if entry_date is None:
        return None
    
    # Define targets and stop-loss
    target1 = entry_price * 1.15# 10%
    target2 = entry_price * 1.25# 15%
    target3 = entry_price * 1.35  # 20%
    stop_loss = entry_price * 0.7 # 15% stop loss

    # Start scanning for exits
    future_data = df_stock[df_stock.index > entry_date]
    exit_date, exit_price = None, None

    for date, row in future_data.iterrows():
        price = row["close_price"]

        # Update trailing stop-loss
        if price >= target1:
            stop_loss = entry_price*1.01                     
        if price >= target2:
            stop_loss = target1  # Move SL to Target 1
        if price >= target3:
            stop_loss = target2  # Move SL to Target 2

        if price <= stop_loss or price >= target3:
            exit_date, exit_price = date, price
            break

    if exit_date is None:
        return None

    # Calculate profit/loss
    profit_loss = exit_price - entry_price
    profit_loss_pct = ((exit_price / entry_price) - 1) * 100
    profit_or_loss = "Profit" if profit_loss > 0 else "Loss"
    days_in_trade = (exit_date - entry_date).days

    return [
        stock, entry_date, entry_price, exit_date, exit_price, 
        profit_loss, profit_loss_pct, profit_or_loss, days_in_trade,
        volume_multiple, rsi_value, weekly_volume
    ]

# Run backtest with multithreading
def run_backtest():
    print("Starting backtest process...")

    stock_data_dict = fetch_all_stock_data()
    if stock_data_dict is None:
        return

    high_vol_weeks = fetch_high_volume_weeks()
    if high_vol_weeks is None:
        return

    results = []

    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = {executor.submit(process_stock, row, stock_data_dict): row for _, row in high_vol_weeks.iterrows()}
        
        for future in concurrent.futures.as_completed(futures):
            result = future.result()
            if result:
                results.append(result)

    df_results = pd.DataFrame(results, columns=[
        "Stock Symbol", "Entry Date", "Entry Price", "Exit Date", "Exit Price", 
        "Profit/Loss", "Profit/Loss %", "Profit or Loss", "Days in Trade",
        "Volume Multiple", "RSI Value", "Weekly Volume"
    ])
    
    df_results.to_csv("detailed_backtest_results.csv", index=False)
    print("Backtest completed. Results saved to detailed_backtest_results.csv")

    generate_backtest_summary(df_results)

# Generate Backtest Summary
def generate_backtest_summary(df):
    """Generates a detailed performance summary."""
    total_trades = len(df)
    wins = df[df["Profit or Loss"] == "Profit"]
    losses = df[df["Profit or Loss"] == "Loss"]

    win_ratio = len(wins) / total_trades * 100 if total_trades else 0
    avg_profit_pct = wins["Profit/Loss %"].mean()
    avg_loss_pct = losses["Profit/Loss %"].mean()
    avg_days_profit = wins["Days in Trade"].mean()
    avg_days_loss = losses["Days in Trade"].mean()
    risk_reward_ratio = abs(avg_profit_pct / avg_loss_pct) if avg_loss_pct else None
    max_drawdown = df["Profit/Loss %"].min()
    profit_factor = wins["Profit/Loss"].sum() / abs(losses["Profit/Loss"].sum()) if not losses.empty else "N/A"

    summary = {
        "Total Trades": total_trades,
        "Win Ratio (%)": round(win_ratio, 2),
        "Avg Profit %": round(avg_profit_pct, 2),
        "Avg Loss %": round(avg_loss_pct, 2),
        "Max Drawdown %": round(max_drawdown, 2),
        "Profit Factor": round(profit_factor, 2),
        "Risk-Reward Ratio": round(risk_reward_ratio, 2)
    }

    pd.DataFrame([summary]).to_csv("backtest_summary.csv", index=False)
    print(summary)

run_backtest()
