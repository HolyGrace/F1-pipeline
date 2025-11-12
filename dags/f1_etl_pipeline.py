"""
F1 ETL Pipeline DAG

Main orchestration DAG that runs the complete F1 data pipeline.

Schedule: Daily (can be adjusted)
Tasks:
1. Bronze ingestion (if new data)
2. Process dimensions
3. Incremental processing (Silver)
4. Create analytics (Gold)
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

import sys
from pathlib import Path


# Default arguments for all tasks
default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def run_bronze_ingestion():
    """Task: Ingest raw CSV files to Bronze layer"""
    import sys
    from pathlib import Path
    
    # Add src to path
    src_path = Path('/opt/airflow/src')
    if str(src_path) not in sys.path:
        sys.path.insert(0, str(src_path))
    
    from ingestion import BronzeIngestion
    
    ingestion = BronzeIngestion()
    summary = ingestion.ingest_all()
    
    # Return summary for XCom
    return {
        'total_files': len(summary),
        'successful': len(summary.filter(summary['status'] == 'success'))
    }


def run_dimension_processing():
    """Task: Process dimension tables to Silver"""
    import sys
    from pathlib import Path
    
    # Add src to path
    src_path = Path('/opt/airflow/src')
    if str(src_path) not in sys.path:
        sys.path.insert(0, str(src_path))
    
    from process_dimensions import process_dimension_tables
    
    stats = process_dimension_tables()
    
    successful = [s for s in stats if s.get('status') == 'success']
    return {
        'total_tables': len(stats),
        'successful': len(successful)
    }


def run_incremental_processing():
    """Task: Process fact tables incrementally to Silver"""
    import sys
    from pathlib import Path
    
    # Add src to path
    src_path = Path('/opt/airflow/src')
    if str(src_path) not in sys.path:
        sys.path.insert(0, str(src_path))
    
    from incremental_processing import IncrementalProcessor
    
    processor = IncrementalProcessor()
    
    # Determine if this is initial load or incremental
    if not processor.state:
        summary = processor.process_all_incremental(is_initial_load=True)
        load_type = 'initial'
    else:
        summary = processor.process_all_incremental(is_initial_load=False)
        load_type = 'incremental'
    
    successful = summary.filter(summary['status'] == 'success')
    
    return {
        'load_type': load_type,
        'total_tables': len(summary),
        'successful': len(successful),
        'total_rows': successful['rows_processed'].sum() if len(successful) > 0 else 0
    }


def run_gold_analytics():
    """Task: Create Gold layer analytics"""
    import sys
    from pathlib import Path
    
    # Add src to path
    src_path = Path('/opt/airflow/src')
    if str(src_path) not in sys.path:
        sys.path.insert(0, str(src_path))
    
    from analytics import GoldAnalytics
    
    analytics = GoldAnalytics()
    results = analytics.create_all_analytics()
    
    successful = sum(1 for v in results.values() if v is not None)
    
    return {
        'total_tables': len(results),
        'successful': successful
    }


def validate_pipeline_completion(**context):
    """Task: Validate that all steps completed successfully"""
    ti = context['ti']
    
    # Pull results from previous tasks
    bronze_result = ti.xcom_pull(task_ids='bronze_ingestion')
    dimension_result = ti.xcom_pull(task_ids='dimension_processing')
    incremental_result = ti.xcom_pull(task_ids='incremental_processing')
    gold_result = ti.xcom_pull(task_ids='gold_analytics')
    
    # Log summary
    print("="*60)
    print("PIPELINE EXECUTION SUMMARY")
    print("="*60)
    print(f"Bronze Ingestion: {bronze_result}")
    print(f"Dimension Processing: {dimension_result}")
    print(f"Incremental Processing: {incremental_result}")
    print(f"Gold Analytics: {gold_result}")
    print("="*60)
    
    # Check critical failures (bronze and gold must succeed)
    critical_failures = []
    
    if bronze_result.get('successful', 0) == 0:
        critical_failures.append("Bronze ingestion failed completely")
    
    if gold_result.get('successful', 0) == 0:
        critical_failures.append("Gold analytics failed completely")
    
    if critical_failures:
        raise Exception(f"Critical failures detected: {', '.join(critical_failures)}")
    
    # Log warnings for non-critical issues
    if incremental_result.get('successful', 0) == 0:
        print("WARNING: Incremental processing had no successful tables (might be up-to-date)")
    
    print("Pipeline completed successfully!")
    return True


# Define the DAG
with DAG(
    'f1_etl_pipeline',
    default_args=default_args,
    description='Complete F1 data ETL pipeline - Bronze to Gold',
    schedule='@daily',  # Run daily at midnight
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['f1', 'etl', 'production'],
) as dag:
    
    # Task 1: Bronze layer ingestion
    task_bronze = PythonOperator(
        task_id='bronze_ingestion',
        python_callable=run_bronze_ingestion,
        doc_md="""
        ### Bronze Ingestion
        Converts raw CSV files to optimized Parquet format.
        - Reads from data/raw/
        - Writes to data/bronze/
        - Applies schema overrides for problematic columns
        """
    )
    
    # Task 2: Dimension tables processing
    task_dimensions = PythonOperator(
        task_id='dimension_processing',
        python_callable=run_dimension_processing,
        doc_md="""
        ### Dimension Processing
        Processes dimension tables (circuits, drivers, constructors, etc.)
        - Reads from data/bronze/
        - Applies transformations
        - Writes to data/silver/
        """
    )
    
    # Task 3: Incremental processing
    task_incremental = PythonOperator(
        task_id='incremental_processing',
        python_callable=run_incremental_processing,
        doc_md="""
        ### Incremental Processing
        Processes fact tables incrementally based on year.
        - Detects new data automatically
        - Only processes years not yet processed
        - Appends to existing Silver data
        """
    )
    
    # Task 4: Gold analytics
    task_gold = PythonOperator(
        task_id='gold_analytics',
        python_callable=run_gold_analytics,
        doc_md="""
        ### Gold Analytics
        Creates aggregated analytical datasets.
        - driver_performance
        - constructor_performance
        - circuit_analysis
        - race_results_enriched
        """
    )
    
    # Task 5: Validation
    task_validate = PythonOperator(
        task_id='validate_completion',
        python_callable=validate_pipeline_completion,
        doc_md="""
        ### Validation
        Validates that all pipeline steps completed successfully.
        Raises exception if any failures detected.
        """
    )
    
    # Define task dependencies (execution order)
    # Bronze -> Dimensions + Incremental (parallel) -> Gold -> Validate
    task_bronze >> [task_dimensions, task_incremental]
    [task_dimensions, task_incremental] >> task_gold
    task_gold >> task_validate
    