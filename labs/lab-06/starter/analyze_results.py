#!/usr/bin/env python3
"""
============================================================
LAB 06 STARTER: Analyze dbt run_results.json
============================================================

OBJECTIVE: Parse dbt artifacts to find performance bottlenecks
           and identify optimization opportunities.

USAGE:
    python labs/lab-06/starter/analyze_results.py

PREREQUISITES:
    Run 'dbt build' first to generate target/run_results.json

============================================================
"""

import json
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any, Optional


def load_run_results(path: str = "target/run_results.json") -> Dict[str, Any]:
    """
    Load run_results.json from dbt target directory.
    
    Args:
        path: Path to run_results.json file
        
    Returns:
        Parsed JSON as dictionary
        
    Raises:
        FileNotFoundError: If run_results.json doesn't exist
    """
    with open(path, 'r') as f:
        return json.load(f)


def analyze_timing(results: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Extract timing information from run results.
    
    Args:
        results: Parsed run_results.json
        
    Returns:
        List of dictionaries with timing info per model
    """
    timings = []
    
    for result in results.get('results', []):
        unique_id = result.get('unique_id', 'unknown')
        status = result.get('status', 'unknown')
        
        # Get total execution time
        exec_time = result.get('execution_time', 0)
        
        # Get detailed timing breakdown if available
        timing_info = result.get('timing', [])
        compile_time = 0
        execute_time = 0
        
        for t in timing_info:
            try:
                started = datetime.fromisoformat(t['started_at'].replace('Z', '+00:00'))
                completed = datetime.fromisoformat(t['completed_at'].replace('Z', '+00:00'))
                duration = (completed - started).total_seconds()
                
                if t.get('name') == 'compile':
                    compile_time = duration
                elif t.get('name') == 'execute':
                    execute_time = duration
            except (KeyError, ValueError):
                continue
        
        # Extract model name from unique_id
        # Format: model.project.model_name or test.project.test_name
        node_name = unique_id.split('.')[-1] if unique_id else 'unknown'
        node_type = unique_id.split('.')[0] if unique_id else 'unknown'
        
        timings.append({
            'node': node_name,
            'type': node_type,
            'unique_id': unique_id,
            'status': status,
            'total_seconds': round(exec_time, 2),
            'compile_seconds': round(compile_time, 2),
            'execute_seconds': round(execute_time, 2)
        })
    
    return timings


def print_report(timings: List[Dict[str, Any]], show_tests: bool = False):
    """
    Print a formatted performance report.
    
    Args:
        timings: List of timing dictionaries
        show_tests: Include tests in report (default: False)
    """
    print("\n" + "=" * 60)
    print("DBT PERFORMANCE REPORT")
    print("=" * 60)
    
    # Filter to models only (unless show_tests is True)
    if not show_tests:
        timings = [t for t in timings if t['type'] == 'model']
    
    # Sort by execution time (slowest first)
    sorted_timings = sorted(timings, key=lambda x: x['total_seconds'], reverse=True)
    
    print(f"\n{'Model':<40} {'Status':<10} {'Time (s)':<10}")
    print("-" * 60)
    
    for t in sorted_timings[:15]:  # Top 15 slowest
        print(f"{t['node']:<40} {t['status']:<10} {t['total_seconds']:<10}")
    
    # Summary stats
    total_time = sum(t['total_seconds'] for t in timings)
    success_count = len([t for t in timings if t['status'] == 'success'])
    error_count = len([t for t in timings if t['status'] == 'error'])
    warn_count = len([t for t in timings if t['status'] == 'warn'])
    
    print("\n" + "-" * 60)
    print(f"Total models: {len(timings)}")
    print(f"Successful: {success_count}")
    if error_count > 0:
        print(f"Errors: {error_count}")
    if warn_count > 0:
        print(f"Warnings: {warn_count}")
    print(f"Total time: {round(total_time, 2)} seconds")
    print("=" * 60)


# ============================================================
# TODO: Implement these functions
# ============================================================

def find_bottlenecks(timings: List[Dict[str, Any]], threshold_seconds: float = 5.0) -> List[Dict[str, Any]]:
    """
    Find models that exceed the time threshold.
    
    Args:
        timings: List of timing dictionaries
        threshold_seconds: Time threshold in seconds (default: 5.0)
        
    Returns:
        List of models exceeding threshold, sorted by time (slowest first)
    
    TODO: Implement this function
    """
    # TODO: Filter timings to only include models (not tests)
    # TODO: Find models where total_seconds > threshold_seconds
    # TODO: Sort by total_seconds descending
    # TODO: Return the filtered list
    
    pass  # Remove this and implement


def analyze_by_layer(timings: List[Dict[str, Any]]) -> Dict[str, float]:
    """
    Group execution time by model layer.
    
    Layers:
        - staging: models starting with 'stg_'
        - intermediate: models starting with 'int_'
        - marts: models starting with 'fct_' or 'dim_'
        - other: everything else
    
    Args:
        timings: List of timing dictionaries
        
    Returns:
        Dictionary mapping layer name to total seconds
    
    TODO: Implement this function
    """
    # TODO: Initialize layer totals
    # TODO: Iterate through timings
    # TODO: Categorize each model by its prefix
    # TODO: Sum up times for each layer
    # TODO: Return the layer totals
    
    pass  # Remove this and implement


def calculate_critical_path(timings: List[Dict[str, Any]], manifest_path: str = "target/manifest.json") -> List[str]:
    """
    Identify the critical path (longest chain of dependent models).
    
    This requires reading the manifest.json to understand dependencies.
    
    Args:
        timings: List of timing dictionaries
        manifest_path: Path to manifest.json
        
    Returns:
        List of model names in the critical path (from source to final)
    
    TODO: BONUS - Implement this function (advanced)
    """
    # TODO: Load manifest.json
    # TODO: Build dependency graph
    # TODO: Find the longest path by total execution time
    # TODO: Return the path
    
    pass  # Remove this and implement


def generate_recommendations(timings: List[Dict[str, Any]]) -> List[str]:
    """
    Generate optimization recommendations based on timing analysis.
    
    Args:
        timings: List of timing dictionaries
        
    Returns:
        List of recommendation strings
    
    TODO: Implement this function
    """
    recommendations = []
    
    # TODO: Check for slow models (> 10s) - recommend incremental
    # TODO: Check for models with high compile time - recommend breaking into CTEs
    # TODO: Check for models with high execute time - recommend partitioning/clustering
    # TODO: Add recommendations to the list
    
    pass  # Remove this and implement


# ============================================================
# Main execution
# ============================================================

if __name__ == "__main__":
    try:
        # Load results
        results = load_run_results()
        
        # Analyze timing
        timings = analyze_timing(results)
        
        # Print basic report
        print_report(timings)
        
        # --------------------------------------------------------
        # TODO: Uncomment these sections after implementing functions
        # --------------------------------------------------------
        
        # # Find bottlenecks
        # print("\n" + "=" * 60)
        # print("BOTTLENECKS (> 5.0s)")
        # print("=" * 60)
        # bottlenecks = find_bottlenecks(timings, threshold_seconds=5.0)
        # if bottlenecks:
        #     for b in bottlenecks:
        #         print(f"  ⚠️  {b['node']}: {b['total_seconds']}s")
        # else:
        #     print("  ✅ No bottlenecks found!")
        
        # # Analyze by layer
        # print("\n" + "=" * 60)
        # print("TIME BY LAYER")
        # print("=" * 60)
        # layer_times = analyze_by_layer(timings)
        # if layer_times:
        #     for layer, time in layer_times.items():
        #         print(f"  {layer.capitalize()}: {time:.2f}s")
        
        # # Generate recommendations
        # print("\n" + "=" * 60)
        # print("RECOMMENDATIONS")
        # print("=" * 60)
        # recommendations = generate_recommendations(timings)
        # if recommendations:
        #     for rec in recommendations:
        #         print(f"  {rec}")
        # else:
        #     print("  ✅ No recommendations - performance looks good!")
        
    except FileNotFoundError:
        print("=" * 60)
        print("ERROR: target/run_results.json not found")
        print("=" * 60)
        print("\nPlease run 'dbt build' first to generate artifacts.")
        print("\nExample:")
        print("  $ dbt build")
        print("  $ python labs/lab-06/starter/analyze_results.py")
