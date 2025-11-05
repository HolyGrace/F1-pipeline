"""
Bronze Layer Ingestion

Reads raw CSV files and converts them to Parquet format.
This is the first stage of the medallion architecture.

Bronze = Raw data in optimized format (no transformations yet)
"""

import polars as pl
from pathlib import Path
from typing import List, Dict
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
)
logger = logging.getLogger(__name__)

class BronzeIngestion:
    """Handles ingestion of raw CSV files into Bronze layer (Parquet)"""
    
    def __init__(self, raw_path: str = "data/raw", bronze_path: str = "data/bronze"):
        self.raw_path = Path(raw_path)
        self.bronze_path = Path(bronze_path)
        
        # Create bronze directory if it doesn't exist
        self.bronze_path.mkdir(parents=True, exist_ok=True)
        
        # Define schema overrides for problematic files
        # Bronze layer: preserve raw data as strings when ambiguous
        self.schema_overrides = {
            'constructor_results': {
                'points': pl.Utf8
            },
            'constructor_standings': {
                'points': pl.Utf8,
                'positionText': pl.Utf8
            },
            'driver_standings': {
                'points': pl.Utf8,
                'positionText': pl.Utf8
            },
            'pit_stops': {
                'duration': pl.Utf8
            },
            'results': {
                'points': pl.Utf8,
                'number': pl.Utf8,
                'positionText': pl.Utf8,
                'position': pl.Utf8
            }
        }
        
        logger.info(f"Initialized BronzeIngestion")
        logger.info(f"Raw path: {self.raw_path}")
        logger.info(f"Bronze path: {self.bronze_path}")
        logger.info(f"Schema overrides defined for {len(self.schema_overrides)} files")
        
    def get_csv_files(self) -> List[Path]:
        """Get all CSV files from raw directory"""
        csv_files = list(self.raw_path.glob("*.csv"))
        logger.info(f"Found {len(csv_files)} CSV files")
        return csv_files
    
    def ingest_file(self, csv_file: Path) -> Dict[str, any]:
        """
        Ingest a single CSV file to Parquet format
        
        Args:
            csv_file: Path to CSV file
            
        Returns:
            Dictionary with ingestion statistics
        """
        file_name = csv_file.stem
        logger.info(f"Processing {file_name}.csv...")
        
        try:
            # Read CSV with Polars
            start_time = datetime.now()
            
            schema_override = self.schema_overrides.get(file_name, None)

            if schema_override:
                logger.info(f"Applying schema override for {file_name}")
                df = pl.read_csv(csv_file, schema_overrides=schema_override)
            else:
                df = pl.read_csv(csv_file)
            
            read_time = (datetime.now() - start_time).total_seconds()
            
            # Get file sizes
            csv_size = csv_file.stat().st_size / (1024**2)
            
            # Define output path
            parquet_file = self.bronze_path / f"{file_name}.parquet"
            
            # Write to Parquet with compression
            write_start = datetime.now()
            df.write_parquet(parquet_file, compression="snappy")
            write_time = (datetime.now() - write_start).total_seconds()
            
            parquet_size = parquet_file.stat().st_size / (1024**2)
        
            # Calculate statistics
            stats = {
                'file_name': file_name,
                'rows': df.shape[0],
                'columns': df.shape[1],
                'csv_size_mb': round(csv_size, 2),
                'parquet_size_mb': round(parquet_size, 2),
                'size_reduction_pct': round((1 - parquet_size / csv_size) * 100, 1) if csv_size > 0 else 0,
                'read_time_sec': round(read_time, 4),
                'write_time_sec': round(write_time, 4),
                'status': 'success'
            }
            
            logger.info(
                f"{file_name}: {stats['rows']:,} rows, "
                f"size reduction: {stats['size_reduction_pct']}%"
            )
            
            return stats
        
        except Exception as e:
            logger.error(f"Error processing {file_name}: {str(e)}")
            return {
                'file_name': file_name,
                'status': 'failed',
                'error': str(e)
            }

    def ingest_all(self) -> pl.DataFrame:
        """
        Ingest all CSV files and return summary statistics
        
        Returns:
            Polars DataFrame with ingestion statistics for all files
        """
        logger.info("="*60)
        logger.info("STARTING BRONZE LAYER INGESTION")
        logger.info("="*60)
        
        csv_files = self.get_csv_files()
        
        if not csv_files:
            logger.warning("No CSV files found!")
            return pl.DataFrame()
        
        # Process all files
        stats_list = []
        for csv_file in csv_files:
            stats = self.ingest_file(csv_file)
            stats_list.append(stats)
            
        # Create summary DataFrame
        summary_df = pl.DataFrame(stats_list)
        
        # Log summary
        logger.info("="*60)
        logger.info("INGESTION SUMMARY")
        logger.info("="*60)
        
        successful = summary_df.filter(pl.col('status') == 'success')
        failed = summary_df.filter(pl.col('status') == 'failed')

        logger.info(f"Successful: {len(successful)}")
        logger.info(f"Failed: {len(failed)}")
        
        if len(successful) > 0:
            total_rows = successful['rows'].sum()
            total_csv_size = successful['csv_size_mb'].sum()
            total_parquet_size = successful['parquet_size_mb'].sum()
            avg_reduction = (1 - total_parquet_size / total_csv_size) * 100 if total_csv_size > 0 else 0
            
            logger.info(f"Total rows processed: {total_rows:,}")
            logger.info(f"Total CSV size: {total_csv_size:.2f} MB")
            logger.info(f"Total Parquet size: {total_parquet_size:.2f} MB")
            logger.info(f"Average size reduction: {avg_reduction:.1f}%")
            
        return summary_df
    
def main():
    """Main execution function"""
    # Initialize and run ingestion
    ingestion = BronzeIngestion()
    summary = ingestion.ingest_all()
    
    # Save summary to CSV for reference
    summary_path = Path("logs") / f"bronze_ingestion_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    summary_path.parent.mkdir(exist_ok=True)
    
    if len(summary) > 0:
        summary.write_csv(summary_path)
        logger.info(f"Summary saved to {summary_path}")
        
    return summary

if __name__ == "__main__":
    main()
        