"""
APAP ETL Pipeline - Dagster Workflow Orchestration
===================================================
Orchestrates the complete ETL pipeline:
1. Staging Layer - Extract data from APIs to staging databases
2. Mart Layer - Transform and load data to data warehouse
3. BI Reporting Layer - Generate analyses and reports

Uses Dagster for workflow management, scheduling, and monitoring.
Note: Requires Python 3.11 or 3.12 for full Dagster UI support.
"""

import os
import sys
import subprocess
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List

# Try to import Dagster, fall back to standalone mode if not available
DAGSTER_AVAILABLE = False
try:
    from dagster import (
        asset,
        op,
        job,
        schedule,
        Definitions,
        AssetExecutionContext,
        OpExecutionContext,
        In,
        Out,
        DagsterLogManager,
        get_dagster_logger,
        ScheduleDefinition,
        DefaultScheduleStatus
    )
    DAGSTER_AVAILABLE = True
except Exception as e:
    print(f"Dagster not available: {e}")
    print("Running in standalone mode without Dagster UI...")
    # Create dummy decorators for standalone mode
    def asset(*args, **kwargs):
        def decorator(func):
            return func
        return decorator if not args else decorator(args[0])
    
    def op(*args, **kwargs):
        def decorator(func):
            return func
        return decorator if not args else decorator(args[0])
    
    def job(*args, **kwargs):
        def decorator(func):
            return func
        return decorator if not args else decorator(args[0])
    
    class DagsterLogManager:
        def info(self, msg): print(f"[INFO] {msg}")
        def error(self, msg): print(f"[ERROR] {msg}")
        def warning(self, msg): print(f"[WARNING] {msg}")
    
    def get_dagster_logger():
        return DagsterLogManager()
    
    class AssetExecutionContext:
        log = DagsterLogManager()
    
    class OpExecutionContext:
        log = DagsterLogManager()

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from Pipeline.config import (
    BASE_DIR, STAGING_DIR, MART_DIR, BIREPORTING_DIR,
    STAGING_SCRIPTS, MART_SCRIPTS, BIREPORTING_SCRIPTS,
    PIPELINE_SETTINGS
)


# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

def run_python_script(script_path: Path, args: List[str] = None, 
                      working_dir: Path = None, logger: DagsterLogManager = None) -> Dict[str, Any]:
    """
    Execute a Python script and capture output.
    
    Args:
        script_path: Path to the Python script
        args: Optional list of command line arguments
        working_dir: Working directory for script execution
        logger: Dagster logger for output
        
    Returns:
        Dictionary with execution results
    """
    if logger is None:
        logger = get_dagster_logger()
        
    if not script_path.exists():
        return {
            'success': False,
            'error': f'Script not found: {script_path}',
            'output': '',
            'duration': 0
        }
    
    cmd = [sys.executable, str(script_path)]
    if args:
        cmd.extend(args)
    
    start_time = datetime.now()
    
    # Set environment with UTF-8 encoding
    env = os.environ.copy()
    env['PYTHONIOENCODING'] = 'utf-8'
    
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            cwd=str(working_dir) if working_dir else str(script_path.parent),
            timeout=PIPELINE_SETTINGS['timeout_seconds'],
            env=env,
            encoding='utf-8',
            errors='replace'
        )
        
        duration = (datetime.now() - start_time).total_seconds()
        
        if result.stdout:
            logger.info(result.stdout)
        if result.stderr and result.returncode != 0:
            logger.error(result.stderr)
        
        return {
            'success': result.returncode == 0,
            'output': result.stdout,
            'error': result.stderr if result.returncode != 0 else '',
            'return_code': result.returncode,
            'duration': duration
        }
        
    except subprocess.TimeoutExpired:
        return {
            'success': False,
            'error': f'Script timed out after {PIPELINE_SETTINGS["timeout_seconds"]} seconds',
            'output': '',
            'duration': PIPELINE_SETTINGS['timeout_seconds']
        }
    except Exception as e:
        return {
            'success': False,
            'error': str(e),
            'output': '',
            'duration': (datetime.now() - start_time).total_seconds()
        }


# ============================================================================
# STAGING ASSETS
# ============================================================================

@asset(
    group_name="staging",
    description="Extract World Bank data from API to MongoDB staging",
    compute_kind="python",
    metadata={"source": "World Bank API", "target": "MongoDB"}
)
def staging_worldbank(context: AssetExecutionContext) -> Dict[str, Any]:
    """Run World Bank staging ETL."""
    context.log.info("Starting World Bank Staging ETL...")
    
    script_path = STAGING_DIR / "worldbank_data.py"
    result = run_python_script(script_path, logger=context.log)
    
    if result['success']:
        context.log.info(f"World Bank staging completed in {result['duration']:.2f}s")
    else:
        context.log.error(f"World Bank staging failed: {result['error']}")
        raise Exception(f"World Bank staging failed: {result['error']}")
        
    return result


@asset(
    group_name="staging",
    description="Extract Ireland Groundwater data from API to MongoDB staging",
    compute_kind="python",
    metadata={"source": "Ireland Groundwater API", "target": "MongoDB"}
)
def staging_ireland(context: AssetExecutionContext) -> Dict[str, Any]:
    """Run Ireland Groundwater staging ETL."""
    context.log.info("Starting Ireland Groundwater Staging ETL...")
    
    script_path = STAGING_DIR / "ireland_groundwater_data.py"
    result = run_python_script(script_path, logger=context.log)
    
    if result['success']:
        context.log.info(f"Ireland staging completed in {result['duration']:.2f}s")
    else:
        context.log.error(f"Ireland staging failed: {result['error']}")
        raise Exception(f"Ireland staging failed: {result['error']}")
        
    return result


@asset(
    group_name="staging",
    description="Extract EV Population data from API to PostgreSQL staging",
    compute_kind="python",
    metadata={"source": "data.wa.gov API", "target": "PostgreSQL"}
)
def staging_ev_population(context: AssetExecutionContext) -> Dict[str, Any]:
    """Run EV Population staging ETL."""
    context.log.info("Starting EV Population Staging ETL...")
    
    script_path = STAGING_DIR / "ev_population_data.py"
    result = run_python_script(script_path, logger=context.log)
    
    if result['success']:
        context.log.info(f"EV Population staging completed in {result['duration']:.2f}s")
    else:
        context.log.error(f"EV Population staging failed: {result['error']}")
        raise Exception(f"EV Population staging failed: {result['error']}")
        
    return result


# ============================================================================
# MART ASSETS
# ============================================================================

@asset(
    group_name="mart",
    deps=[staging_worldbank],
    description="Transform World Bank data from staging to mart",
    compute_kind="python",
    metadata={"source": "MongoDB", "target": "PostgreSQL Mart"}
)
def mart_worldbank(context: AssetExecutionContext) -> Dict[str, Any]:
    """Run World Bank mart transformation."""
    context.log.info("Starting World Bank Mart Transformation...")
    
    script_path = MART_DIR / "worldbank_mart.py"
    result = run_python_script(script_path, logger=context.log)
    
    if result['success']:
        context.log.info(f"World Bank mart completed in {result['duration']:.2f}s")
    else:
        context.log.error(f"World Bank mart failed: {result['error']}")
        raise Exception(f"World Bank mart failed: {result['error']}")
        
    return result


@asset(
    group_name="mart",
    deps=[staging_ireland],
    description="Transform Ireland Groundwater data from staging to mart",
    compute_kind="python",
    metadata={"source": "MongoDB", "target": "PostgreSQL Mart"}
)
def mart_ireland(context: AssetExecutionContext) -> Dict[str, Any]:
    """Run Ireland Groundwater mart transformation."""
    context.log.info("Starting Ireland Groundwater Mart Transformation...")
    
    script_path = MART_DIR / "ireland_groundwater_mart.py"
    result = run_python_script(script_path, logger=context.log)
    
    if result['success']:
        context.log.info(f"Ireland mart completed in {result['duration']:.2f}s")
    else:
        context.log.error(f"Ireland mart failed: {result['error']}")
        raise Exception(f"Ireland mart failed: {result['error']}")
        
    return result


@asset(
    group_name="mart",
    deps=[staging_ev_population],
    description="Transform EV Population data from staging to mart",
    compute_kind="python",
    metadata={"source": "PostgreSQL Staging", "target": "PostgreSQL Mart"}
)
def mart_ev_population(context: AssetExecutionContext) -> Dict[str, Any]:
    """Run EV Population mart transformation."""
    context.log.info("Starting EV Population Mart Transformation...")
    
    script_path = MART_DIR / "ev_population_mart.py"
    result = run_python_script(script_path, logger=context.log)
    
    if result['success']:
        context.log.info(f"EV Population mart completed in {result['duration']:.2f}s")
    else:
        context.log.error(f"EV Population mart failed: {result['error']}")
        raise Exception(f"EV Population mart failed: {result['error']}")
        
    return result


# ============================================================================
# BI REPORTING ASSETS
# ============================================================================

@asset(
    group_name="bireporting",
    deps=[mart_worldbank, mart_ireland, mart_ev_population],
    description="Generate BI reports and analyses from mart data",
    compute_kind="python",
    metadata={"source": "PostgreSQL Mart", "output": "Reports & Charts"}
)
def bi_reporting(context: AssetExecutionContext) -> Dict[str, Any]:
    """Run BI Reporting analysis and report generation."""
    context.log.info("Starting BI Reporting Analysis...")
    
    script_info = BIREPORTING_SCRIPTS['analysis']
    script_path = BIREPORTING_DIR / script_info['script']
    result = run_python_script(
        script_path, 
        args=script_info.get('args', []),
        working_dir=BIREPORTING_DIR,
        logger=context.log
    )
    
    if result['success']:
        context.log.info(f"BI Reporting completed in {result['duration']:.2f}s")
    else:
        context.log.error(f"BI Reporting failed: {result['error']}")
        raise Exception(f"BI Reporting failed: {result['error']}")
        
    return result


# ============================================================================
# OPS FOR JOB-BASED EXECUTION
# ============================================================================

@op(description="Run all staging ETL tasks")
def run_staging(context: OpExecutionContext) -> Dict[str, Any]:
    """Execute all staging tasks."""
    context.log.info("=" * 60)
    context.log.info("STAGING LAYER STARTED")
    context.log.info("=" * 60)
    
    results = {}
    
    # WorldBank
    context.log.info("Running World Bank staging...")
    script_path = STAGING_DIR / "worldbank_data.py"
    results['worldbank'] = run_python_script(script_path, logger=context.log)
    
    # Ireland
    context.log.info("Running Ireland staging...")
    script_path = STAGING_DIR / "ireland_groundwater_data.py"
    results['ireland'] = run_python_script(script_path, logger=context.log)
    
    # EV Population
    context.log.info("Running EV Population staging...")
    script_path = STAGING_DIR / "ev_population_data.py"
    results['ev_population'] = run_python_script(script_path, logger=context.log)
    
    # Check results
    all_success = all(r['success'] for r in results.values())
    if all_success:
        context.log.info("All staging tasks completed successfully")
    else:
        failed = [k for k, v in results.items() if not v['success']]
        context.log.warning(f"Some staging tasks failed: {failed}")
    
    return results


@op(description="Run all mart transformation tasks")
def run_mart(context: OpExecutionContext, staging_results: Dict[str, Any]) -> Dict[str, Any]:
    """Execute all mart transformation tasks."""
    context.log.info("=" * 60)
    context.log.info("MART LAYER STARTED")
    context.log.info("=" * 60)
    
    results = {}
    
    # WorldBank
    context.log.info("Running World Bank mart transformation...")
    script_path = MART_DIR / "worldbank_mart.py"
    results['worldbank'] = run_python_script(script_path, logger=context.log)
    
    # Ireland
    context.log.info("Running Ireland mart transformation...")
    script_path = MART_DIR / "ireland_groundwater_mart.py"
    results['ireland'] = run_python_script(script_path, logger=context.log)
    
    # EV Population
    context.log.info("Running EV Population mart transformation...")
    script_path = MART_DIR / "ev_population_mart.py"
    results['ev_population'] = run_python_script(script_path, logger=context.log)
    
    # Check results
    all_success = all(r['success'] for r in results.values())
    if all_success:
        context.log.info("All mart tasks completed successfully")
    else:
        failed = [k for k, v in results.items() if not v['success']]
        context.log.warning(f"Some mart tasks failed: {failed}")
    
    return results


@op(description="Run BI reporting and analysis")
def run_bireporting(context: OpExecutionContext, mart_results: Dict[str, Any]) -> Dict[str, Any]:
    """Execute BI reporting."""
    context.log.info("=" * 60)
    context.log.info("BI REPORTING LAYER STARTED")
    context.log.info("=" * 60)
    
    script_info = BIREPORTING_SCRIPTS['analysis']
    script_path = BIREPORTING_DIR / script_info['script']
    result = run_python_script(
        script_path, 
        args=script_info.get('args', []),
        working_dir=BIREPORTING_DIR,
        logger=context.log
    )
    
    if result['success']:
        context.log.info(f"BI Reporting completed in {result['duration']:.2f}s")
    else:
        context.log.error(f"BI Reporting failed: {result['error']}")
    
    return {'analysis': result}


# ============================================================================
# JOBS
# ============================================================================

@job(description="Full ETL Pipeline: Staging → Mart → BI Reporting")
def full_etl_pipeline():
    """Complete ETL pipeline job."""
    staging_results = run_staging()
    mart_results = run_mart(staging_results)
    run_bireporting(mart_results)


@job(description="Staging only job")
def staging_job():
    """Run only staging layer."""
    run_staging()


@job(description="Mart only job")  
def mart_job():
    """Run only mart layer."""
    staging_results = run_staging()
    run_mart(staging_results)


# ============================================================================
# SCHEDULES (only if Dagster is available)
# ============================================================================

if DAGSTER_AVAILABLE:
    # Daily schedule at 2 AM
    daily_etl_schedule = ScheduleDefinition(
        job=full_etl_pipeline,
        cron_schedule="0 2 * * *",
        name="daily_etl_schedule",
        description="Run full ETL pipeline daily at 2 AM",
        default_status=DefaultScheduleStatus.STOPPED
    )

    # Weekly schedule on Sunday at midnight
    weekly_etl_schedule = ScheduleDefinition(
        job=full_etl_pipeline,
        cron_schedule="0 0 * * 0",
        name="weekly_etl_schedule", 
        description="Run full ETL pipeline weekly on Sunday at midnight",
        default_status=DefaultScheduleStatus.STOPPED
    )


    # ============================================================================
    # DAGSTER DEFINITIONS
    # ============================================================================

    defs = Definitions(
        assets=[
            staging_worldbank,
            staging_ireland,
            staging_ev_population,
            mart_worldbank,
            mart_ireland,
            mart_ev_population,
            bi_reporting
        ],
        jobs=[
            full_etl_pipeline,
            staging_job,
            mart_job
        ],
        schedules=[
            daily_etl_schedule,
            weekly_etl_schedule
        ]
    )


# ============================================================================
# MAIN ENTRY POINT (for direct execution)
# ============================================================================

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='APAP Dagster ETL Pipeline')
    parser.add_argument('--stage', choices=['staging', 'mart', 'bireporting', 'full'],
                       default='full', help='Which stage to run')
    parser.add_argument('--dev', action='store_true',
                       help='Run Dagster development server (dagster dev)')
    
    args = parser.parse_args()
    
    if args.dev:
        # Launch Dagster dev server
        print("Starting Dagster development server...")
        print("Open http://localhost:3000 in your browser")
        os.system(f"{sys.executable} -m dagster dev -f {__file__}")
    else:
        # Direct execution using the simple runner
        print("\n" + "=" * 60)
        print("APAP ETL PIPELINE - Direct Execution Mode")
        print("=" * 60)
        print("\nFor Dagster UI, run: python etl_pipeline.py --dev")
        print("Or: dagster dev -f etl_pipeline.py")
        print("\n" + "=" * 60)
        
        # Use subprocess to run stages directly
        from run_pipeline import PipelineRunner
        runner = PipelineRunner()
        
        if args.stage == 'staging':
            result = runner.run_staging()
        elif args.stage == 'mart':
            result = runner.run_mart()
        elif args.stage == 'bireporting':
            result = runner.run_bireporting()
        else:
            result = runner.run_full_pipeline()
        
        print(f"\nFinal Status: {result.get('status', 'COMPLETED')}")
