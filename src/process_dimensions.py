"""
Process Dimension Tables

Processes dimension tables (circuits, drivers, constructors, etc.)
that don't have year column and don't need incremental processing.
"""

import polars as pl
from pathlib import Path
import logging
from datetime import datetime

from config import config
from transformation import SilverTransformation

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def process_dimension_tables():
    """Process all dimension tables from Bronze to Silver"""
    logger.info("="*60)
    logger.info("PROCESSING DIMENSION TABLES")
    logger.info("="*60)
    
    transformer = SilverTransformation()
    dimension_tables = config.DIMENSION_TABLES
    
    logger.info(f"Found {len(dimension_tables)} dimension tables to process")
    
    stats = []
    
    for table_name in dimension_tables:
        logger.info(f"Processing {table_name}...")
        
        try:
            # Read from bronze
            bronze_file = config.BRONZE_PATH / f"{table_name}.parquet"
            
            if not bronze_file.exists():
                logger.warning(f"  Bronze file not found: {table_name}")
                continue
            
            df = pl.read_parquet(bronze_file)
            
            # Apply transformation if exists
            if hasattr(transformer, f'transform_{table_name}'):
                transform_method = getattr(transformer, f'transform_{table_name}')
                df_transformed = transform_method(df)
            else:
                # No specific transformation, just pass through
                df_transformed = df
            
            # Write to silver
            silver_file = config.SILVER_PATH / f"{table_name}.parquet"
            df_transformed.write_parquet(silver_file, compression=config.PARQUET_COMPRESSION)
            
            logger.info(f"  SUCCESS: {table_name} - {len(df_transformed):,} rows")
            
            stats.append({
                'table_name': table_name,
                'rows': len(df_transformed),
                'columns': len(df_transformed.columns),
                'status': 'success'
            })
            
        except Exception as e:
            logger.error(f"  ERROR: Failed to process {table_name}: {str(e)}")
            stats.append({
                'table_name': table_name,
                'status': 'failed',
                'error': str(e)
            })
    
    # Summary
    logger.info("="*60)
    logger.info("DIMENSION PROCESSING SUMMARY")
    logger.info("="*60)
    
    successful = [s for s in stats if s.get('status') == 'success']
    failed = [s for s in stats if s.get('status') == 'failed']
    
    logger.info(f"Successful: {len(successful)}")
    logger.info(f"Failed: {len(failed)}")
    
    if successful:
        total_rows = sum(s['rows'] for s in successful)
        logger.info(f"Total rows: {total_rows:,}")
    
    return stats


def main():
    """Main execution"""
    stats = process_dimension_tables()
    
    # Save log
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_path = config.LOGS_PATH / f"dimension_processing_{timestamp}.txt"
    
    with open(log_path, 'w') as f:
        f.write("Dimension Tables Processing\n")
        f.write("="*60 + "\n\n")
        for stat in stats:
            if stat.get('status') == 'success':
                f.write(f"{stat['table_name']}: {stat['rows']:,} rows\n")
            else:
                f.write(f"{stat['table_name']}: FAILED - {stat.get('error', 'Unknown')}\n")
    
    logger.info(f"Log saved to: {log_path}")


if __name__ == "__main__":
    main()
    