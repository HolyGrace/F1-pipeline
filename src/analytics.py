"""
Gold Layer Analytics

Creates aggregated, analysis-ready datasets from Silver layer.
These are the "data products" ready for dashboards and ML.

Gold = Business-ready aggregations and metrics
"""

import polars as pl
from pathlib import Path
from typing import Optional
import logging
from datetime import datetime

from config import config

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class GoldAnalytics:
    """Creates analytical datasets for Gold layer"""
    
    def __init__(self):
        self.silver_path = config.SILVER_PATH
        self.gold_path = config.GOLD_PATH
        
        config.ensure_paths()
        
        logger.info(f"Initialized GoldAnalytics")
        logger.info(f"Silver path: {self.silver_path}")
        logger.info(f"Gold path: {self.gold_path}")
    
    def create_driver_performance(self) -> Optional[pl.DataFrame]:
        """
        Create driver performance statistics aggregated by driver and year
        
        Metrics:
        - Races participated
        - Wins, podiums, points
        - Average finish position
        - DNF rate
        - Pole positions (from qualifying)
        """
        logger.info("Creating driver_performance table...")
        
        try:
            # Load required tables
            results = pl.read_parquet(self.silver_path / "results.parquet")
            drivers = pl.read_parquet(self.silver_path / "drivers.parquet")
            races = pl.read_parquet(self.silver_path / "races.parquet")
            qualifying = pl.read_parquet(self.silver_path / "qualifying.parquet")
            
            # Join results with races to get year
            results_with_year = results.join(
                races.select(['raceId', 'year']),
                on='raceId',
                how='left'
            )
            
            # Calculate performance metrics per driver per year
            driver_stats = (
                results_with_year
                .group_by(['driverId', 'year'])
                .agg([
                    pl.len().alias('races_entered'),
                    pl.col('position').filter(pl.col('position') == 1).count().alias('wins'),
                    pl.col('position').filter(pl.col('position') <= 3).count().alias('podiums'),
                    pl.col('position').filter(pl.col('position') <= 10).count().alias('points_finishes'),
                    pl.col('points').sum().alias('total_points'),
                    pl.col('position').mean().alias('avg_finish_position'),
                    pl.col('did_not_finish').sum().alias('dnf_count'),
                    pl.col('grid').mean().alias('avg_grid_position')
                ])
            )
            
            # Add pole positions from qualifying
            pole_positions = (
                qualifying
                .join(races.select(['raceId', 'year']), on='raceId', how='left')
                .filter(pl.col('position') == 1)
                .group_by(['driverId', 'year'])
                .agg([
                    pl.len().alias('pole_positions')
                ])
            )
            
            # Combine with driver info
            driver_performance = (
                driver_stats
                .join(pole_positions, on=['driverId', 'year'], how='left')
                .join(
                    drivers.select(['driverId', 'driverRef', 'forename', 'surname', 'nationality']),
                    on='driverId',
                    how='left'
                )
                .with_columns([
                    pl.col('pole_positions').fill_null(0),
                    (pl.col('dnf_count') / pl.col('races_entered') * 100).alias('dnf_rate'),
                    (pl.col('podiums') / pl.col('races_entered') * 100).alias('podium_rate'),
                    pl.concat_str([pl.col('forename'), pl.lit(' '), pl.col('surname')]).alias('driver_name')
                ])
                .select([
                    'driverId', 'driverRef', 'driver_name', 'nationality', 'year',
                    'races_entered', 'wins', 'podiums', 'points_finishes',
                    'total_points', 'pole_positions',
                    'avg_finish_position', 'avg_grid_position',
                    'dnf_count', 'dnf_rate', 'podium_rate'
                ])
                .sort(['year', 'total_points'], descending=[True, True])
            )
            
            # Save to gold
            output_path = self.gold_path / "driver_performance.parquet"
            driver_performance.write_parquet(output_path, compression=config.PARQUET_COMPRESSION)
            
            logger.info(f"SUCCESS: Created driver_performance with {len(driver_performance):,} rows")
            return driver_performance
            
        except Exception as e:
            logger.error(f"ERROR: Failed to create driver_performance: {str(e)}")
            return None
    
    def create_constructor_performance(self) -> Optional[pl.DataFrame]:
        """
        Create constructor (team) performance statistics by year
        
        Metrics:
        - Races participated
        - Wins, podiums, points
        - Championship positions
        """
        logger.info("Creating constructor_performance table...")
        
        try:
            # Load required tables
            results = pl.read_parquet(self.silver_path / "results.parquet")
            constructors = pl.read_parquet(self.silver_path / "constructors.parquet")
            races = pl.read_parquet(self.silver_path / "races.parquet")
            constructor_standings = pl.read_parquet(self.silver_path / "constructor_standings.parquet")
            
            # Join results with races to get year
            results_with_year = results.join(
                races.select(['raceId', 'year']),
                on='raceId',
                how='left'
            )
            
            # Calculate constructor performance
            constructor_stats = (
                results_with_year
                .group_by(['constructorId', 'year'])
                .agg([
                    pl.len().alias('races_entered'),
                    pl.col('position').filter(pl.col('position') == 1).count().alias('wins'),
                    pl.col('position').filter(pl.col('position') <= 3).count().alias('podiums'),
                    pl.col('points').sum().alias('total_points'),
                    pl.col('position').mean().alias('avg_finish_position')
                ])
            )
            
            # Get final championship position per year
            final_standings = (
                constructor_standings
                .join(races.select(['raceId', 'year']), on='raceId', how='left')
                .group_by(['constructorId', 'year'])
                .agg([
                    pl.col('position').last().alias('final_championship_position')
                ])
            )
            
            # Combine with constructor info
            constructor_performance = (
                constructor_stats
                .join(final_standings, on=['constructorId', 'year'], how='left')
                .join(
                    constructors.select(['constructorId', 'constructorRef', 'name', 'nationality']),
                    on='constructorId',
                    how='left'
                )
                .with_columns([
                    (pl.col('podiums') / pl.col('races_entered') * 100).alias('podium_rate'),
                    (pl.col('wins') / pl.col('races_entered') * 100).alias('win_rate')
                ])
                .select([
                    'constructorId', 'constructorRef', 'name', 'nationality', 'year',
                    'races_entered', 'wins', 'podiums', 'total_points',
                    'avg_finish_position', 'final_championship_position',
                    'win_rate', 'podium_rate'
                ])
                .sort(['year', 'total_points'], descending=[True, True])
            )
            
            # Save to gold
            output_path = self.gold_path / "constructor_performance.parquet"
            constructor_performance.write_parquet(output_path, compression=config.PARQUET_COMPRESSION)
            
            logger.info(f"SUCCESS: Created constructor_performance with {len(constructor_performance):,} rows")
            return constructor_performance
            
        except Exception as e:
            logger.error(f"ERROR: Failed to create constructor_performance: {str(e)}")
            return None
    
    def create_circuit_analysis(self) -> Optional[pl.DataFrame]:
        """
        Create circuit characteristics and statistics
        
        Metrics:
        - Number of races held
        - Average winner's time
        - Fastest lap statistics
        - DNF rates
        """
        logger.info("Creating circuit_analysis table...")
        
        try:
            # Load required tables
            circuits = pl.read_parquet(self.silver_path / "circuits.parquet")
            races = pl.read_parquet(self.silver_path / "races.parquet")
            results = pl.read_parquet(self.silver_path / "results.parquet")
            
            # Join races with results to analyze circuit characteristics
            race_results = (
                results
                .join(races.select(['raceId', 'year', 'circuitId']), on='raceId', how='left')
                .join(circuits, on='circuitId', how='left')
            )
            
            # Calculate circuit statistics
            circuit_stats = (
                race_results
                .group_by('circuitId')
                .agg([
                    pl.col('name').first().alias('circuit_name'),
                    pl.col('location').first().alias('location'),
                    pl.col('country').first().alias('country'),
                    pl.col('latitude').first().alias('latitude'),
                    pl.col('longitude').first().alias('longitude'),
                    pl.col('altitude').first().alias('altitude'),
                    pl.col('raceId').n_unique().alias('total_races_held'),
                    pl.col('year').min().alias('first_race_year'),
                    pl.col('year').max().alias('last_race_year'),
                    pl.col('did_not_finish').mean().alias('avg_dnf_rate'),
                    pl.col('fastestLapSpeed').mean().alias('avg_fastest_lap_speed'),
                    pl.col('position').filter(pl.col('position') == 1).alias('winners').n_unique().alias('unique_winners')
                ])
                .with_columns([
                    (pl.col('avg_dnf_rate') * 100).alias('dnf_percentage'),
                    (pl.col('last_race_year') - pl.col('first_race_year') + 1).alias('years_active')
                ])
                .select([
                    'circuitId', 'circuit_name', 'location', 'country',
                    'latitude', 'longitude', 'altitude',
                    'total_races_held', 'first_race_year', 'last_race_year', 'years_active',
                    'unique_winners', 'avg_fastest_lap_speed', 'dnf_percentage'
                ])
                .sort('total_races_held', descending=True)
            )
            
            # Save to gold
            output_path = self.gold_path / "circuit_analysis.parquet"
            circuit_stats.write_parquet(output_path, compression=config.PARQUET_COMPRESSION)
            
            logger.info(f"SUCCESS: Created circuit_analysis with {len(circuit_stats):,} rows")
            return circuit_stats
            
        except Exception as e:
            logger.error(f"ERROR: Failed to create circuit_analysis: {str(e)}")
            return None
    
    def create_race_results_enriched(self) -> Optional[pl.DataFrame]:
        """
        Create enriched race results with all context
        This is the main fact table for analysis and ML
        
        Includes:
        - Race details
        - Driver info
        - Constructor info
        - Circuit info
        - Performance metrics
        """
        logger.info("Creating race_results_enriched table...")
        
        try:
            # Load all required tables
            results = pl.read_parquet(self.silver_path / "results.parquet")
            races = pl.read_parquet(self.silver_path / "races.parquet")
            drivers = pl.read_parquet(self.silver_path / "drivers.parquet")
            constructors = pl.read_parquet(self.silver_path / "constructors.parquet")
            circuits = pl.read_parquet(self.silver_path / "circuits.parquet")
            
            # Create comprehensive enriched table
            enriched = (
                results
                # Join with races
                .join(
                    races.select(['raceId', 'year', 'round', 'circuitId', 'name', 'date']),
                    on='raceId',
                    how='left'
                )
                # Join with drivers
                .join(
                    drivers.select(['driverId', 'driverRef', 'forename', 'surname', 'nationality']),
                    on='driverId',
                    how='left',
                    suffix='_driver'
                )
                # Join with constructors
                .join(
                    constructors.select(['constructorId', 'constructorRef', 'name', 'nationality']),
                    on='constructorId',
                    how='left',
                    suffix='_constructor'
                )
                # Join with circuits
                .join(
                    circuits.select(['circuitId', 'name', 'location', 'country']),
                    on='circuitId',
                    how='left',
                    suffix='_circuit'
                )
                .with_columns([
                    pl.concat_str([pl.col('forename'), pl.lit(' '), pl.col('surname')]).alias('driver_name'),
                    (pl.col('position') <= 3).alias('is_podium'),
                    (pl.col('position') == 1).alias('is_win'),
                    (pl.col('grid') - pl.col('position')).alias('positions_gained')
                ])
                .select([
                    'resultId', 'raceId', 'year', 'round', 'date', 'name',
                    'driverId', 'driverRef', 'driver_name', 'nationality',
                    'constructorId', 'constructorRef', 'name_constructor', 'nationality_constructor',
                    'circuitId', 'name_circuit', 'location', 'country',
                    'grid', 'position', 'positionText', 'points', 
                    'laps', 'race_time_ms', 'fastestLap', 'fastestLapTime', 'fastestLapSpeed',
                    'did_not_finish', 'disqualified', 'is_podium', 'is_win', 'positions_gained'
                ])
                .sort(['year', 'round', 'position'])
            )
            
            # Save to gold
            output_path = self.gold_path / "race_results_enriched.parquet"
            enriched.write_parquet(output_path, compression=config.PARQUET_COMPRESSION)
            
            logger.info(f"SUCCESS: Created race_results_enriched with {len(enriched):,} rows")
            return enriched
            
        except Exception as e:
            logger.error(f"ERROR: Failed to create race_results_enriched: {str(e)}")
            return None
    
    def create_all_analytics(self) -> dict:
        """Create all gold layer analytics tables"""
        logger.info("="*60)
        logger.info("STARTING GOLD LAYER ANALYTICS CREATION")
        logger.info("="*60)
        
        results = {
            'driver_performance': self.create_driver_performance(),
            'constructor_performance': self.create_constructor_performance(),
            'circuit_analysis': self.create_circuit_analysis(),
            'race_results_enriched': self.create_race_results_enriched()
        }
        
        # Summary
        logger.info("="*60)
        logger.info("ANALYTICS CREATION SUMMARY")
        logger.info("="*60)
        
        success_count = sum(1 for v in results.values() if v is not None)
        total_count = len(results)
        
        logger.info(f"Successful: {success_count}/{total_count}")
        
        for name, df in results.items():
            if df is not None:
                logger.info(f"  SUCCESS: {name} - {len(df):,} rows")
            else:
                logger.info(f"  FAILED: {name}")
        
        return results


def main():
    """Main execution function"""
    analytics = GoldAnalytics()
    results = analytics.create_all_analytics()
    
    # Save summary
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    summary_path = config.LOGS_PATH / f"gold_analytics_{timestamp}.txt"
    
    with open(summary_path, 'w') as f:
        f.write("Gold Layer Analytics Summary\n")
        f.write("="*60 + "\n\n")
        for name, df in results.items():
            if df is not None:
                f.write(f"{name}: {len(df):,} rows\n")
                f.write(f"Columns: {', '.join(df.columns)}\n\n")
            else:
                f.write(f"{name}: FAILED\n\n")
    
    logger.info(f"Summary saved to: {summary_path}")
    
    return results


if __name__ == "__main__":
    main()
    