import sys
import os
import logging
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from database.db_handler import DatabaseHandler

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class PerformanceAnalyzer:
    def __init__(self):
        self.db_handler = DatabaseHandler()

    def analyze_performance(self):
        """Analyze and compare performance between streaming and batch processing."""
        try:
            results = self.db_handler.compare_results()
            if not results:
                logger.error("Could not retrieve results from database")
                return False

            streaming_df = results['streaming']
            batch_df = results['batch']
            perf_df = results['performance']
            category_df = results.get('category_sales', pd.DataFrame())
            state_df = results.get('shipping_state_orders', pd.DataFrame())

            logger.info("\n=== Performance Comparison ===")

            # Compare top SKUs by revenue
            logger.info("\nTop 10 SKU Revenue Comparison:")
            combined_df = pd.merge(
                streaming_df, batch_df,
                on='sku',
                how='outer',
                suffixes=('_streaming', '_batch')
            ).fillna(0)
            logger.info("\n" + combined_df.to_string())

            # Execution time comparison
            if not perf_df.empty:
                logger.info("\nExecution Time Comparison:")
                for _, row in perf_df.iterrows():
                    logger.info(f"{row['processing_type'].title()} processing: {row['avg_time']:.2f} seconds")

            # New: Top category by sales
            if not category_df.empty:
                top_category = category_df.sort_values("total_sales", ascending=False).iloc[0]
                logger.info(f"\nTop Category by Total Sales: {top_category['category']} (${top_category['total_sales']:.2f})")

            # New: Shipping state ranking
            if not state_df.empty:
                logger.info("\nTop 10 Shipping States by Order Count:")
                logger.info("\n" + state_df.sort_values("order_count", ascending=False).head(10).to_string(index=False))

            # Charts
            self.generate_charts(streaming_df, batch_df, perf_df, category_df, state_df)
            return True

        except Exception as e:
            logger.error(f"Error analyzing performance: {e}")
            return False

    def generate_charts(self, streaming_df, batch_df, perf_df, category_df, state_df):
        """Generate comparison charts."""
        try:
            os.makedirs('output', exist_ok=True)

            # 1. Revenue Comparison by SKU
            plt.figure(figsize=(12, 6))
            combined_df = pd.merge(
                streaming_df, batch_df,
                on='sku',
                how='outer',
                suffixes=('_streaming', '_batch')
            ).fillna(0)

            combined_df = combined_df.sort_values('total_revenue_streaming', ascending=False).head(10)
            x = range(len(combined_df))
            width = 0.35

            plt.bar([i - width/2 for i in x], combined_df['total_revenue_streaming'], width, label='Streaming')
            plt.bar([i + width/2 for i in x], combined_df['total_revenue_batch'], width, label='Batch')

            plt.xlabel('SKU')
            plt.ylabel('Total Revenue')
            plt.title('Top SKUs by Revenue: Streaming vs Batch')
            plt.xticks(x, combined_df['sku'], rotation=45, ha='right')
            plt.legend()
            plt.tight_layout()
            plt.savefig('output/sku_revenue_comparison.png')
            logger.info("Created SKU revenue comparison chart: output/sku_revenue_comparison.png")

            # 2. Execution Time Comparison
            if not perf_df.empty:
                plt.figure(figsize=(8, 5))
                ax = perf_df.plot(kind='bar', x='processing_type', y='avg_time', legend=False)
                ax.yaxis.set_major_formatter(FuncFormatter(lambda x, _: f'{x:.2f}s'))
                plt.title('Average Execution Time: Streaming vs Batch')
                plt.xlabel('')
                plt.ylabel('Execution Time')
                plt.tight_layout()
                plt.savefig('output/performance_comparison.png')
                logger.info("Created performance comparison chart: output/performance_comparison.png")

            # 3. Category Sales Chart
            if not category_df.empty:
                top_categories = category_df.sort_values("total_sales", ascending=False).head(10)
                plt.figure(figsize=(10, 6))
                plt.bar(top_categories['category'], top_categories['total_sales'], color='skyblue')
                plt.title("Top 10 Categories by Total Sales")
                plt.xticks(rotation=45, ha='right')
                plt.ylabel("Total Sales")
                plt.tight_layout()
                plt.savefig("output/category_sales.png")
                logger.info("Created category sales chart: output/category_sales.png")

            # 4. Shipping State Orders Chart
            if not state_df.empty:
                top_states = state_df.sort_values("order_count", ascending=False).head(10)
                plt.figure(figsize=(10, 6))
                plt.bar(top_states['shipping_state'], top_states['order_count'], color='salmon')
                plt.title("Top 10 States by Order Count")
                plt.xticks(rotation=45, ha='right')
                plt.ylabel("Order Count")
                plt.tight_layout()
                plt.savefig("output/shipping_state_orders.png")
                logger.info("Created shipping state orders chart: output/shipping_state_orders.png")

            return True

        except Exception as e:
            logger.error(f"Error generating charts: {e}")
            return False

if __name__ == "__main__":
    analyzer = PerformanceAnalyzer()
    analyzer.analyze_performance()
