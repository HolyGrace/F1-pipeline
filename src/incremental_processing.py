"""
Incremental Processing Module

Handles incremental data processing for fact tables.
Only processes new data based on year/date, avoiding full reprocessing.

This is a key feature for production data pipelines.
"""

import polars as pl
from pathlib import Path
from typing import List, Optional, Set
import logging
from datetime import datetime
import json

from config import config

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class IncrementalProcessor:
    """Handles incremental data processing"""
    
    def __init__(self):
        self.bronze_path = config.BRONZE_PATH
        self.silver_path = config.SILVER_PATH
        self.state_file = config.LOGS_PATH / "processing_state.json"
        
        config.ensure_paths()
        
        # Load processing state (which years have been processed)
        self.state = self._load_state()
        
        logger.info(f"Initialized IncrementalProcessor")
        logger.info(f"Processing state loaded: {len(self.state)} tables tracked")
    
    def _load_state(self) -> dict:
        """Load processing state from file"""
        if self.state_file.exists():
            with open(self.state_file, 'r') as f:
                return json.load(f)
        return {}
    
    def _save_state(self):
        """Save processing state to file"""
        with open(self.state_file, 'w') as f:
            json.dump(self.state, f, indent=2)
        logger.info(f"State saved to {self.state_file}")
    
    def get_processed_years(self, table_name: str) -> Set[int]:
        """Get set of years already processed for a table"""
        return set(self.state.get(table_name, {}).get('processed_years', []))
    
    def get_max_processed_year(self, table_name: str) -> Optional[int]:
        """Get the maximum year already processed for a table"""
        processed = self.get_processed_years(table_name)
        return max(processed) if processed else None
    
    def mark_years_processed(self, table_name: str, years: List[int]):
        """Mark years as processed for a table"""
        if table_name not in self.state:
            self.state[table_name] = {'processed_years': []}
        
        current_years = set(self.state[table_name]['processed_years'])
        current_years.update(years)
        self.state[table_name]['processed_years'] = sorted(list(current_years))
        self.state[table_name]['last_update'] = datetime.now().isoformat()
        
        self._save_state()
    
    def get_years_to_process(self, table_name: str, available_years: List[int], 
                            is_initial_load: bool = False) -> List[int]:
        """
        Determine which years need to be processed
        
        Args:
            table_name: Name of the table
            available_years: All years available in the data
            is_initial_load: If True, process up to INITIAL_LOAD_YEAR
            
        Returns:
            List of years to process
        """
        processed_years = self.get_processed_years(table_name)
        
        if is_initial_load and not processed_years:
            # Initial load: process up to INITIAL_LOAD_YEAR
            years_to_process = [y for y in available_years if y <= config.INITIAL_LOAD_YEAR]
            if years_to_process:
                logger.info(f"  Initial load for {table_name}: {min(years_to_process)}-{max(years_to_process)}")
            else:
                logger.info(f"  {table_name} has no data before {config.INITIAL_LOAD_YEAR}")
        else:
            # Incremental: process only new years
            years_to_process = [y for y in available_years if y not in processed_years]
            if years_to_process:
                logger.info(f"  Incremental load for {table_name}: {years_to_process}")
            else:
                logger.info(f"  {table_name} is up to date")
        
        return sorted(years_to_process)
    
    def process_table_incremental(self, table_name: str, is_initial_load: bool = False) -> dict:
        """
        Process a table incrementally
        
        Args:
            table_name: Name of table to process
            is_initial_load: If True, performs initial historical load
            
        Returns:
            Dictionary with processing statistics
        """
        logger.info(f"Processing {table_name}...")
        
        try:
            # Check if table has year column
            if not config.has_year_column(table_name):
                logger.info(f"  {table_name} is a dimension table - skipping incremental processing")
                return {
                    'table_name': table_name,
                    'status': 'skipped',
                    'reason': 'dimension_table'
                }
            
            # Read bronze data
            bronze_file = self.bronze_path / f"{table_name}.parquet"
            if not bronze_file.exists():
                logger.warning(f"  Bronze file not found: {table_name}")
                return {
                    'table_name': table_name,
                    'status': 'failed',
                    'error': 'bronze_file_not_found'
                }
            
            df_bronze = pl.read_parquet(bronze_file)
            
            # Get available years in bronze data
            # First, we need to join with races table to get the year
            races_bronze = pl.read_parquet(self.bronze_path / "races.parquet")
            
            # Join to get year
            if 'raceId' in df_bronze.columns:
                df_with_year = df_bronze.join(
                    races_bronze.select(['raceId', 'year']),
                    on='raceId',
                    how='left'
                )
            elif 'year' in df_bronze.columns:
                df_with_year = df_bronze
            else:
                logger.warning(f"  Cannot determine year for {table_name}")
                return {
                    'table_name': table_name,
                    'status': 'failed',
                    'error': 'no_year_column'
                }
            
            # Get all available years
            available_years = sorted(df_with_year['year'].unique().to_list())
            
            # Determine which years to process
            years_to_process = self.get_years_to_process(table_name, available_years, is_initial_load)
            
            if not years_to_process or len(years_to_process) == 0:
                return {
                    'table_name': table_name,
                    'status': 'up_to_date',
                    'rows_processed': 0
                }
            
            # Filter data for years to process
            df_to_process = df_with_year.filter(pl.col('year').is_in(years_to_process))
            
            # Apply transformations (import from transformation module)
            from transformation import SilverTransformation
            transformer = SilverTransformation()
            
            # Apply table-specific transformation
            if hasattr(transformer, f'transform_{table_name}'):
                transform_method = getattr(transformer, f'transform_{table_name}')
                df_transformed = transform_method(df_to_process)
            else:
                df_transformed = df_to_process
            
            # Check if silver file exists
            silver_file = self.silver_path / f"{table_name}.parquet"
            
            if silver_file.exists() and not is_initial_load:
                # Append to existing data
                df_existing = pl.read_parquet(silver_file)
                
                # Ensure schema compatibility - align columns
                existing_cols = set(df_existing.columns)
                new_cols = set(df_transformed.columns)
                
                # Get common columns only
                common_cols = existing_cols.intersection(new_cols)
                
                if existing_cols != new_cols:
                    logger.warning(f"  Schema mismatch detected. Aligning to common columns.")
                    logger.warning(f"    Existing only: {existing_cols - new_cols}")
                    logger.warning(f"    New only: {new_cols - existing_cols}")
                    
                    # Select only common columns in same order
                    common_cols_list = sorted(list(common_cols))
                    df_existing = df_existing.select(common_cols_list)
                    df_transformed = df_transformed.select(common_cols_list)
                
                df_final = pl.concat([df_existing, df_transformed])
                logger.info(f"  Appending {len(df_transformed):,} rows to existing {len(df_existing):,} rows")
            else:
                # Write new file
                df_final = df_transformed
                logger.info(f"  Writing {len(df_transformed):,} new rows")
            
            # Write to silver
            df_final.write_parquet(silver_file, compression=config.PARQUET_COMPRESSION)
            
            # Mark years as processed
            self.mark_years_processed(table_name, years_to_process)
            
            stats = {
                'table_name': table_name,
                'status': 'success',
                'years_processed': years_to_process,
                'rows_processed': len(df_transformed),
                'total_rows': len(df_final)
            }
            
            logger.info(f"SUCCESS: {table_name}: Processed {len(years_to_process)} years, {len(df_transformed):,} rows")
            
            return stats
            
        except Exception as e:
            logger.error(f"ERROR: Error processing {table_name}: {str(e)}")
            import traceback
            traceback.print_exc()
            return {
                'table_name': table_name,
                'status': 'failed',
                'error': str(e)
            }
    
    def process_all_incremental(self, is_initial_load: bool = False) -> pl.DataFrame:
        """
        Process all tables incrementally
        
        Args:
            is_initial_load: If True, performs initial historical load
            
        Returns:
            DataFrame with processing summary
        """
        logger.info("="*60)
        if is_initial_load:
            logger.info("STARTING INITIAL LOAD (Historical Data)")
            logger.info(f"Loading data up to year: {config.INITIAL_LOAD_YEAR}")
        else:
            logger.info("STARTING INCREMENTAL PROCESSING (New Data)")
        logger.info("="*60)
        
        # Process only fact tables (those with year column)
        tables_to_process = config.TABLES_WITH_YEAR
        
        stats_list = []
        for table_name in tables_to_process:
            stats = self.process_table_incremental(table_name, is_initial_load)
            stats_list.append(stats)
        
        # Summary
        logger.info("="*60)
        logger.info("PROCESSING SUMMARY")
        logger.info("="*60)
        
        successful = [s for s in stats_list if s.get('status') == 'success']
        up_to_date = [s for s in stats_list if s.get('status') == 'up_to_date']
        failed = [s for s in stats_list if s.get('status') == 'failed']
        
        logger.info(f"Processed: {len(successful)}")
        logger.info(f"Up to date: {len(up_to_date)}")
        logger.info(f"Failed: {len(failed)}")
        
        if successful:
            total_rows = sum(s.get('rows_processed', 0) for s in successful)
            logger.info(f"Total rows processed: {total_rows:,}")
        
        summary_df = pl.DataFrame(stats_list)
        return summary_df
    
    def reset_state(self, table_name: Optional[str] = None):
        """
        Reset processing state
        
        Args:
            table_name: If provided, reset only this table. Otherwise reset all.
        """
        if table_name:
            if table_name in self.state:
                del self.state[table_name]
                logger.info(f"Reset state for {table_name}")
        else:
            self.state = {}
            logger.info(f"Reset all processing state")
        
        self._save_state()


def main():
    """Main execution function"""
    import sys
    
    processor = IncrementalProcessor()
    
    # Check command line arguments
    if len(sys.argv) > 1:
        command = sys.argv[1]
        
        if command == "initial":
            # Initial load
            summary = processor.process_all_incremental(is_initial_load=True)
        elif command == "incremental":
            # Incremental processing
            summary = processor.process_all_incremental(is_initial_load=False)
        elif command == "reset":
            # Reset state
            processor.reset_state()
            logger.info("State reset. Run with 'initial' to reload data.")
            return
        else:
            logger.error(f"Unknown command: {command}")
            logger.info("Usage: python incremental_processing.py [initial|incremental|reset]")
            return
    else:
        # Default: check if this is first run
        if not processor.state:
            logger.info("No state found - running initial load")
            summary = processor.process_all_incremental(is_initial_load=True)
        else:
            logger.info("State found - running incremental processing")
            summary = processor.process_all_incremental(is_initial_load=False)
    
    # Save summary
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    summary_path = config.LOGS_PATH / f"incremental_processing_{timestamp}.csv"
    
    if len(summary) > 0:
        # Flatten nested columns for CSV export
        summary_flat = summary.with_columns([
            # Convert years list to string
            pl.when(pl.col('years_processed').is_not_null())
              .then(pl.col('years_processed').cast(pl.List(pl.Utf8)).list.join(", "))
              .otherwise(None)
              .alias('years_processed_str'),
            pl.col('rows_processed').fill_null(0),
            pl.col('total_rows').fill_null(0)
        ]).select([
            'table_name',
            'status',
            pl.col('years_processed_str').alias('years_processed'),
            'rows_processed',
            'total_rows'
        ])
        summary_flat.write_csv(summary_path)
        logger.info(f"Summary saved to: {summary_path}")


if __name__ == "__main__":
    main()
    