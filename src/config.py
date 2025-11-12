"""
Configuration Module

Centralized configuration for F1 Data Pipeline
Contains paths, schema definitions, and processing parameters.
"""

from pathlib import Path
import polars as pl
from typing import Dict

class Config:
    """Central configuration for the F1 data pipeline"""
    
    PROJECT_ROOT = Path(__file__).parent.parent
    
    RAW_PATH = PROJECT_ROOT / "data" / "raw"
    BRONZE_PATH = PROJECT_ROOT / "data" / "bronze"
    SILVER_PATH = PROJECT_ROOT / "data" / "silver"
    GOLD_PATH = PROJECT_ROOT / "data" / "gold"
    
    # Other paths
    LOGS_PATH = PROJECT_ROOT / "logs"
    MODELS_PATH = PROJECT_ROOT / "models"
    
    # SCHEMA DEFINITIONS
    # Schema overrides for Bronze layer (preserve raw data as strings when ambiguous)
    BRONZE_SCHEMA_OVERRIDES: Dict[str, Dict[str, type]] = {
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
    
    # PROCESSING PARAMETERS
    # Incremental processing settings
    INCREMENTAL_COLUMN = "year"
    START_YEAR = 1950
    INITIAL_LOAD_YEAR = 2010

    # Parquet settings
    PARQUET_COMPRESSION = "snappy"
    
    # Tables that contain the year column (for incremental processing)
    TABLES_WITH_YEAR = [
        'races',
        'results',
        'qualifying',
        'sprint_results',
        'lap_times',
        'pit_stops',
        'driver_standings',
        'constructor_standings',
        'constructor_results'
    ]
    
    # Dimension tables (no year, load once)
    DIMENSION_TABLES = [
        'circuits',
        'constructors',
        'drivers',
        'seasons',
        'status'
    ]
    
    # RELATIONSHIP MAPPING
    # Key relationships between tables (for Silver layer joins)
    TABLE_RELATIONSHIPS = {
        'results': {
            'raceId': 'races',
            'driverId': 'drivers',
            'constructorId': 'constructors',
            'statusId': 'status'
        },
        'qualifying': {
            'raceId': 'races',
            'driverId': 'drivers',
            'constructorId': 'constructors'
        },
        'lap_times': {
            'raceId': 'races',
            'driverId': 'drivers'
        },
        'pit_stops': {
            'raceId': 'races',
            'driverId': 'drivers'
        },
        'races': {
            'circuitId': 'circuits'
        }
    }
    
    # HELPER METHODS
    
    @classmethod
    def ensure_paths(cls):
        """Create all necessary directories if they don't exist"""
        for path in [cls.BRONZE_PATH, cls.SILVER_PATH, cls.GOLD_PATH,
                     cls.LOGS_PATH, cls.MODELS_PATH]:
            path.mkdir(parents=True, exist_ok=True)
            
    @classmethod
    def get_bronze_schema_override(cls, table_name: str) -> Dict[str, type]:
        """Get schema override for a specific table"""
        return cls.BRONZE_SCHEMA_OVERRIDES.get(table_name, {})
    
    @classmethod
    def is_dimension_table(cls, table_name: str) -> bool:
        """Check if a table is a dimension table (no incremental processing)"""
        return table_name in cls.DIMENSION_TABLES
    
    @classmethod
    def has_year_column(cls, table_name: str) -> bool:
        """Check if a table has year column for incremental processing"""
        return table_name in cls.TABLES_WITH_YEAR
    
# Create a singleton instance
config = Config()

if __name__ == "__main__":
    # Test configuration
    print("="*60)
    print("F1 DATA PIPELINE CONFIGURATION")
    print("="*60)
    
    print(f"\n Project root: {config.PROJECT_ROOT}")
    print(f"\nData paths:")
    print(f"  Raw: {config.RAW_PATH}")
    print(f"  Bronze: {config.BRONZE_PATH}")
    print(f"  Silver: {config.SILVER_PATH}")
    print(f"  Gold: {config.GOLD_PATH}")
    
    print(f"\n Schema overrides: {len(config.BRONZE_SCHEMA_OVERRIDES)} tables")
    for table in config.BRONZE_SCHEMA_OVERRIDES.keys():
        print(f"  - {table}")
        
    print(f"\n Incremental processing:")
    print(f"  Tables with year: {len(config.TABLES_WITH_YEAR)}")
    print(f"  Dimension tables: {len(config.DIMENSION_TABLES)}")
    
    print(f"\n Relationships defined for {len(config.TABLE_RELATIONSHIPS)} tables")
    
    print("\nConfiguration loaded successfully!")
    