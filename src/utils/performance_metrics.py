# src/performance/performance_analyzer.py
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

            perf_df = results.get('performance', pd.DataFrame())

            logger.info("\n=== Execution Time Comparison ===")
            if perf_df.empty:
                logger.warning("No performance data available.")
                return False

            for _, row in perf_df.iterrows():
                logger.info(f"{row['processing_type'].title()} processing: {row['avg_time']:.2f} seconds")

            self.generate_chart(perf_df)
            return True

        except Exception as e:
            logger.error(f"Error analyzing performance: {e}")
            return False

    def generate_chart(self, perf_df):
        """Generate and save execution time comparison chart."""
        try:
            os.makedirs('output', exist_ok=True)

            plt.figure(figsize=(8, 5))
            ax = perf_df.plot(kind='bar', x='processing_type', y='avg_time', legend=False)
            ax.yaxis.set_major_formatter(FuncFormatter(lambda x, _: f'{x:.2f}s'))
            plt.title('Average Execution Time: Streaming vs Batch')
            plt.xlabel('')
            plt.ylabel('Execution Time')
            plt.tight_layout()
            plt.savefig('output/performance_comparison.png')

            logger.info("Created performance comparison chart: output/performance_comparison.png")
            return True

        except Exception as e:
            logger.error(f"Error generating performance chart: {e}")
            return False

if __name__ == "__main__":
    analyzer = PerformanceAnalyzer()
    analyzer.analyze_performance()
