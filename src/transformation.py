"""
Silver Layer Transformation

Cleans and transforms Bronze data into Silver layer.
Applies data type conversions, handles nulls, and enriches data.

Silver = Clean, typed, validated data ready for analytics
"""

import polars as pl
from pathlib import Path
from typing import Dict, Optional
import logging
from datetime import datetime

from config import config

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class SilverTransformation:
    """Handles transformation of Bronze data into Silver layer"""
    
    def __init__(self):
        self.bronze_path = config.BRONZE_PATH
        self.silver_path = config.SILVER_PATH
        
        # Ensure silver directory exists
        config.ensure_paths()
        
        logger.info(f"Initialized SilverTransformation")
        logger.info(f"Bronze path: {self.bronze_path}")
        logger.info(f"Silver path: {self.silver_path}")
        
    def transform_circuits(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Transform circuits table
        - Convert altitude to proper numeric type
        - Handle null values
        """
        return df.with_columns([
            pl.col("alt").cast(pl.Float64, strict=False).alias("altitude"),
            pl.col("lat").cast(pl.Float64, strict=False).alias("latitude"),
            pl.col("lng").cast(pl.Float64, strict=False).alias("longitude")
        ]).drop(["alt", "lat", "lng"])
    
    def transform_constructor_results(self, df: pl.DataFrame) -> pl.DataFrame:
        """Transform constructor_results table"""
        return df.with_columns([
            pl.col("points").cast(pl.Float64, strict=False).alias("points")
        ])
    
    def transform_standings(self, df: pl.DataFrame, standing_type: str) -> pl.DataFrame:
        """
        Transform standings tables (driver_standings, constructor_standings)
        - Convert positionText to int where possible
        """
        return df.with_columns([
            # Position: convert to int, keep null for special values
            pl.when(pl.col("positionText").str.contains(r"^\d+$"))
              .then(pl.col("positionText").cast(pl.Int64, strict=False))
              .otherwise(None)
              .alias("position"),
            
            pl.col("points").cast(pl.Float64, strict=False).alias("points"),
            pl.col("wins").cast(pl.Int64, strict=False).alias("wins")
        ])
    
    def transform_results(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Transform results table
        - Convert points from string to float
        - Convert position to int (handle special values)
        - Handle positionText special values (R=Retired, D=Disqualified, etc.)
        """
        return df.with_columns([
            # Convert points (can be decimals like 1.5, 2.5)
            pl.col("points").cast(pl.Float64, strict=False).alias("points"),
            
            # Position: convert to int, keep null for DNF/DSQ
            pl.when(pl.col("position").str.contains(r"^\d+$"))
              .then(pl.col("position").cast(pl.Int64, strict=False))
              .otherwise(None)
              .alias("position"),
            
            # Create flag columns for special statuses
            pl.col("positionText").is_in(["R", "W", "F"]).alias("did_not_finish"),
            pl.col("positionText").is_in(["D", "E"]).alias("disqualified"),
            
            # Convert milliseconds to seconds for easier interpretation
            pl.col("milliseconds").cast(pl.Int64, strict=False).alias("race_time_ms"),
            (pl.col("milliseconds").cast(pl.Float64, strict=False) / 1000).alias("race_time_seconds")
        ])
    
    def transform_constructor_results(self, df: pl.DataFrame) -> pl.DataFrame:
        """Transform constructor_results table"""
        return df.with_columns([
            pl.col("points").cast(pl.Float64, strict=False).alias("points")
        ])
    
    def transform_standings(self, df: pl.DataFrame, standing_type: str) -> pl.DataFrame:
        """
        Transform standings tables (driver_standings, constructor_standings)
        - Convert positionText to int where possible
        """
        return df.with_columns([
            # Position: convert to int, keep null for special values
            pl.when(pl.col("positionText").str.contains(r"^\d+$"))
              .then(pl.col("positionText").cast(pl.Int64, strict=False))
              .otherwise(None)
              .alias("position"),
            
            pl.col("points").cast(pl.Float64, strict=False).alias("points"),
            pl.col("wins").cast(pl.Int64, strict=False).alias("wins")
        ])
    
    def transform_pit_stops(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Transform pit_stops table
        - Parse duration string (format: "MM:SS.mmm") to seconds
        """
        return df.with_columns([
            # Parse duration string to seconds
            # Handle both "M:SS.mmm" and "MM:SS.mmm" formats
            pl.when(pl.col("duration").is_not_null())
              .then(
                  pl.col("duration")
                    .str.split(":")
                    .list.first()
                    .cast(pl.Float64, strict=False)
                    .mul(60)
                    .add(
                        pl.col("duration")
                          .str.split(":")
                          .list.last()
                          .cast(pl.Float64, strict=False)
                    )
              )
              .otherwise(None)
              .alias("duration_seconds"),
            
            pl.col("milliseconds").cast(pl.Int64, strict=False).alias("pit_time_ms")
        ])
    
    def transform_races(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Transform races table
        - Combine date/time columns
        - Extract useful date features
        """
        return df.with_columns([
            # Combine date fields
            pl.concat_str([
                pl.col("date"),
                pl.lit(" "),
                pl.col("time").fill_null("00:00:00")
            ]).str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S", strict=False).alias("race_datetime"),
            
            # Extract date components for easier filtering
            pl.col("year").cast(pl.Int64).alias("year"),
            pl.col("round").cast(pl.Int64).alias("round")
        ])
    
    def transform_qualifying(self, df: pl.DataFrame) -> pl.DataFrame:
        """Transform qualifying table - basic type casting"""
        return df.with_columns([
            pl.col("qualifyId").cast(pl.Int64, strict=False),
            pl.col("position").cast(pl.Int64, strict=False)
        ])
    
    def transform_lap_times(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Transform lap_times table
        - Convert time string to seconds
        """
        return df.with_columns([
            # Parse lap time (format: "M:SS.mmm")
            pl.when(pl.col("time").is_not_null())
              .then(
                  pl.col("time")
                    .str.split(":")
                    .list.first()
                    .cast(pl.Float64, strict=False)
                    .mul(60)
                    .add(
                        pl.col("time")
                          .str.split(":")
                          .list.last()
                          .cast(pl.Float64, strict=False)
                    )
              )
              .otherwise(None)
              .alias("lap_time_seconds"),
            
            pl.col("milliseconds").cast(pl.Int64, strict=False).alias("lap_time_ms")
        ])
    
    def validate_data_quality(self, df: pl.DataFrame, table_name: str) -> Dict:
        """
        Validate data quality and return statistics
        
        Returns:
            Dictionary with quality metrics
        """
        total_rows = len(df)
        null_counts = df.null_count()
        
        # Calculate percentage of nulls per column
        null_percentages = {}
        for col in df.columns:
            null_count = null_counts[col][0]
            null_pct = (null_count / total_rows * 100) if total_rows > 0 else 0
            if null_pct > 0:
                null_percentages[col] = round(null_pct, 2)
        
        quality_stats = {
            'table_name': table_name,
            'total_rows': total_rows,
            'total_columns': len(df.columns),
            'null_percentages': null_percentages,
            'has_duplicates': df.is_duplicated().any()
        }
        
        # Log warnings for high null percentages
        for col, pct in null_percentages.items():
            if pct > 50:
                logger.warning(f"  {table_name}.{col} has {pct}% null values")
        
        return quality_stats
    
    def transform_table(self, table_name: str) -> Optional[Dict]:
        """
        Transform a single table from Bronze to Silver
        
        Args:
            table_name: Name of the table to transform
            
        Returns:
            Dictionary with transformation statistics
        """
        logger.info(f"Transforming {table_name}...")
        
        try:
            # Read from Bronze
            bronze_file = self.bronze_path / f"{table_name}.parquet"
            
            if not bronze_file.exists():
                logger.warning(f"  Bronze file not found: {table_name}")
                return None
            
            df = pl.read_parquet(bronze_file)
            original_rows = len(df)
            
            # Apply specific transformations based on table
            if table_name == "circuits":
                df_transformed = self.transform_circuits(df)
            elif table_name == "results":
                df_transformed = self.transform_results(df)
            elif table_name == "constructor_results":
                df_transformed = self.transform_constructor_results(df)
            elif table_name == "driver_standings":
                df_transformed = self.transform_standings(df, "driver")
            elif table_name == "constructor_standings":
                df_transformed = self.transform_standings(df, "constructor")
            elif table_name == "pit_stops":
                df_transformed = self.transform_pit_stops(df)
            elif table_name == "races":
                df_transformed = self.transform_races(df)
            elif table_name == "qualifying":
                df_transformed = self.transform_qualifying(df)
            elif table_name == "lap_times":
                df_transformed = self.transform_lap_times(df)
            else:
                # For tables without specific transformations, just pass through
                df_transformed = df
            
            # Validate data quality
            quality_stats = self.validate_data_quality(df_transformed, table_name)
            
            # Write to Silver
            silver_file = self.silver_path / f"{table_name}.parquet"
            df_transformed.write_parquet(silver_file, compression=config.PARQUET_COMPRESSION)
            
            stats = {
                'table_name': table_name,
                'rows': len(df_transformed),
                'columns': len(df_transformed.columns),
                'original_rows': original_rows,
                'quality_stats': quality_stats,
                'status': 'success'
            }
            
            logger.info(f"{table_name}: {stats['rows']:,} rows transformed")
            
            return stats
            
        except Exception as e:
            logger.error(f"Error transforming {table_name}: {str(e)}")
            return {
                'table_name': table_name,
                'status': 'failed',
                'error': str(e)
            }
    
    def transform_all(self) -> pl.DataFrame:
        """Transform all tables from Bronze to Silver"""
        logger.info("="*60)
        logger.info("STARTING SILVER LAYER TRANSFORMATION")
        logger.info("="*60)
        
        # Get all bronze parquet files
        bronze_files = list(self.bronze_path.glob("*.parquet"))
        table_names = [f.stem for f in bronze_files]
        
        logger.info(f"Found {len(table_names)} tables in Bronze layer")
        
        # Transform all tables
        stats_list = []
        for table_name in table_names:
            stats = self.transform_table(table_name)
            if stats:
                stats_list.append(stats)
        
        # Create summary
        logger.info("="*60)
        logger.info("TRANSFORMATION SUMMARY")
        logger.info("="*60)
        
        successful = [s for s in stats_list if s.get('status') == 'success']
        failed = [s for s in stats_list if s.get('status') == 'failed']
        
        logger.info(f"Successful: {len(successful)}")
        logger.info(f"Failed: {len(failed)}")
        
        if successful:
            total_rows = sum(s['rows'] for s in successful)
            logger.info(f"Total rows in Silver: {total_rows:,}")
        
        # Convert to DataFrame for logging
        summary_df = pl.DataFrame([
            {
                'table_name': s['table_name'],
                'rows': s.get('rows', 0),
                'columns': s.get('columns', 0),
                'status': s['status']
            }
            for s in stats_list
        ])
        
        return summary_df


def main():
    """Main execution function"""
    transformation = SilverTransformation()
    summary = transformation.transform_all()
    
    # Save summary
    summary_path = config.LOGS_PATH / f"silver_transformation_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    
    if len(summary) > 0:
        summary.write_csv(summary_path)
        logger.info(f"Summary saved to: {summary_path}")
    
    return summary


if __name__ == "__main__":
    main()