import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import statsmodels.api as sm

# Load CSV File
file_path = "detailed_backtest_results.csv"  # Update the path if needed
df = pd.read_csv(file_path)

# Convert Entry Date to datetime & extract Year
df["Entry Date"] = pd.to_datetime(df["Entry Date"])
df["Year"] = df["Entry Date"].dt.year

# Group data by Year
yearly_stats = df.groupby("Year").agg(
    Total_Profit=("Profit/Loss", lambda x: x[x > 0].sum()),
    Total_Loss=("Profit/Loss", lambda x: x[x < 0].sum()),
    Win_Percentage=("Profit/Loss", lambda x: (x > 0).mean() * 100),
    Total_Trades=("Profit/Loss", "count"),
).reset_index()

# Plot Profit & Loss Per Year
plt.figure(figsize=(10, 5))
sns.barplot(x="Year", y="Total_Profit", data=yearly_stats, color="green", label="Total Profit")
sns.barplot(x="Year", y="Total_Loss", data=yearly_stats, color="red", label="Total Loss")
plt.title("Total Profit & Loss by Year")
plt.xlabel("Year")
plt.ylabel("Profit/Loss")
plt.legend()
plt.show()

# Plot Win % Per Year
plt.figure(figsize=(8, 5))
sns.barplot(x="Year", y="Win_Percentage", data=yearly_stats, palette="Blues")
plt.title("Winning Percentage by Year")
plt.xlabel("Year")
plt.ylabel("Win %")
plt.ylim(0, 100)
plt.show()

# Save Yearly Performance Data
yearly_stats.to_csv("yearly_performance.csv", index=False)
print("\n✅ Analysis Completed! Yearly performance saved in 'yearly_performance.csv'.")
