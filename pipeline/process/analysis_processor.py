# from __future__ import annotations
# import pandas as pd
# import matplotlib.pyplot as plt
# import seaborn as sns
# import os
# from pipeline.process.processor import Processor
#
# class AnalysisProcessor(Processor):
#     """Performs exploratory data analysis: distributions, outliers, and key insights."""
#
#     def process(self, df: pd.DataFrame) -> pd.DataFrame:
#         output_dir = "reports/graphs"
#         os.makedirs(output_dir, exist_ok=True)
#
#         # 1. Distribution of time spent
#         self.log("Plotting distribution of time_spent_seconds")
#         plt.figure(figsize=(8, 5))
#         sns.histplot(df["time_spent_seconds"], kde=True)
#         plt.title("Distribution of Time Spent (seconds)")
#         plt.savefig(f"{output_dir}/time_spent_distribution.png")
#         plt.close()
#
#         # 2. Distribution of purchases (non-null only)
#         self.log("Plotting purchase distribution")
#         plt.figure(figsize=(8, 5))
#         sns.boxplot(x=df["purchase"])
#         plt.title("Purchase Distribution (Outlier Visualization)")
#         plt.savefig(f"{output_dir}/purchase_boxplot.png")
#         plt.close()
#
#         # 3. Outlier detection using IQR
#         q1 = df["purchase"].quantile(0.25)
#         q3 = df["purchase"].quantile(0.75)
#         iqr = q3 - q1
#         lower_bound = q1 - 1.5 * iqr
#         upper_bound = q3 + 1.5 * iqr
#         outliers = df[(df["purchase"] < lower_bound) | (df["purchase"] > upper_bound)]
#         self.log(f"Detected {len(outliers)} outliers in 'purchase' column.")
#         outliers.to_csv(f"{output_dir}/outliers.csv", index=False)
#
#
#         # 5. State-level insights
#         state_sales = df.groupby("state")["purchase"].sum().sort_values(ascending=False)
#         highest_state = state_sales.idxmax()
#         lowest_state = state_sales.idxmin()
#         self.log(f"Highest sales in: {highest_state}, Lowest sales in: {lowest_state}")
#
#         # Save barplot
#         plt.figure(figsize=(12, 6))
#         sns.barplot(x=state_sales.index, y=state_sales.values)
#         plt.xticks(rotation=90)
#         plt.title("Total Sales by State")
#         plt.tight_layout()
#         plt.savefig(f"{output_dir}/state_sales.png")
#         plt.close()
#
#         # Optionally return insights as new columns or metadata
#         # df["conversion_rate"] = conversion_rate
#         return df
