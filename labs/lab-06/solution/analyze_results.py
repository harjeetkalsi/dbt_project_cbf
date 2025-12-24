#!/usr/bin/env python3
"""
============================================================
LAB 06 SOLUTION: Analyze dbt run_results.json
============================================================

Complete implementation with all analysis functions.

============================================================
"""

import json
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any, Optional
from collections import defaultdict


def load_run_results(path: str = "target/run_results.json") -> Dict[str, Any]:
    """Load run_results.json from dbt target directory."""
    with open(path, 'r') as f:
        return json.load(f)


def load_manifest(path: str = "target/manifest.json") -> Dict[str, Any]:
    """Load manifest.json from dbt target directory."""
    with open(path, 'r') as f:
        return json.load(f)


def analyze_timing(results: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Extract timing information from run results."""
    timings = []
    
    for result in results.get('results', []):
        unique_id = result.get('unique_id', 'unknown')
        status = result.get('status', 'unknown')
        exec_time = result.get('execution_time', 0)
        
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
    """Print a formatted performance report."""
    print("\n" + "=" * 60)
    print("DBT PERFORMANCE REPORT")
    print("=" * 60)
    
    if not show_tests:
        timings = [t for t in timings if t['type'] == 'model']
    
    sorted_timings = sorted(timings, key=lambda x: x['total_seconds'], reverse=True)
    
    print(f"\n{'Model':<40} {'Status':<10} {'Time (s)':<10}")
    print("-" * 60)
    
    for t in sorted_timings[:15]:
        status_icon = "✅" if t['status'] == 'success' else "❌" if t['status'] == 'error' else "⚠️"
        print(f"{t['node']:<40} {status_icon} {t['status']:<7} {t['total_seconds']:<10}")
    
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


def find_bottlenecks(timings: List[Dict[str, Any]], threshold_seconds: float = 5.0) -> List[Dict[str, Any]]:
    """
    Find models that exceed the time threshold.
    
    SOLUTION: Filter, sort, and return models exceeding threshold.
    """
    # Filter to models only
    models = [t for t in timings if t['type'] == 'model']
    
    # Find models exceeding threshold
    bottlenecks = [t for t in models if t['total_seconds'] > threshold_seconds]
    
    # Sort by time (slowest first)
    bottlenecks.sort(key=lambda x: x['total_seconds'], reverse=True)
    
    return bottlenecks


def analyze_by_layer(timings: List[Dict[str, Any]]) -> Dict[str, float]:
    """
    Group execution time by model layer.
    
    SOLUTION: Categorize models by naming convention and sum times.
    """
    layer_times = {
        'staging': 0.0,
        'intermediate': 0.0,
        'marts': 0.0,
        'other': 0.0
    }
    
    for t in timings:
        if t['type'] != 'model':
            continue
            
        node = t['node']
        time = t['total_seconds']
        
        if node.startswith('stg_'):
            layer_times['staging'] += time
        elif node.startswith('int_'):
            layer_times['intermediate'] += time
        elif node.startswith(('fct_', 'dim_')):
            layer_times['marts'] += time
        else:
            layer_times['other'] += time
    
    return layer_times


def calculate_critical_path(timings: List[Dict[str, Any]], manifest_path: str = "target/manifest.json") -> List[Dict[str, Any]]:
    """
    Identify the critical path (longest chain by total time).
    
    SOLUTION: Build dependency graph and find longest path.
    """
    try:
        manifest = load_manifest(manifest_path)
    except FileNotFoundError:
        return []
    
    # Build timing lookup
    timing_lookup = {t['unique_id']: t['total_seconds'] for t in timings}
    
    # Build dependency graph
    nodes = manifest.get('nodes', {})
    
    # Find all model nodes and their dependencies
    model_deps = {}
    for node_id, node_info in nodes.items():
        if node_info.get('resource_type') == 'model':
            deps = node_info.get('depends_on', {}).get('nodes', [])
            # Filter to only model dependencies
            model_deps[node_id] = [d for d in deps if d.startswith('model.')]
    
    # Calculate path times using dynamic programming
    path_times = {}
    path_chains = {}
    
    def get_path_time(node_id: str) -> tuple:
        """Recursively calculate longest path time to this node."""
        if node_id in path_times:
            return path_times[node_id], path_chains[node_id]
        
        node_time = timing_lookup.get(node_id, 0)
        deps = model_deps.get(node_id, [])
        
        if not deps:
            path_times[node_id] = node_time
            path_chains[node_id] = [node_id]
            return node_time, [node_id]
        
        # Find longest upstream path
        max_upstream_time = 0
        max_upstream_chain = []
        
        for dep in deps:
            dep_time, dep_chain = get_path_time(dep)
            if dep_time > max_upstream_time:
                max_upstream_time = dep_time
                max_upstream_chain = dep_chain
        
        total_time = max_upstream_time + node_time
        total_chain = max_upstream_chain + [node_id]
        
        path_times[node_id] = total_time
        path_chains[node_id] = total_chain
        
        return total_time, total_chain
    
    # Calculate for all models
    for node_id in model_deps:
        get_path_time(node_id)
    
    # Find the longest path
    if not path_times:
        return []
    
    critical_node = max(path_times, key=path_times.get)
    critical_path = path_chains[critical_node]
    
    # Build result with details
    result = []
    for node_id in critical_path:
        node_name = node_id.split('.')[-1]
        result.append({
            'node': node_name,
            'unique_id': node_id,
            'time': timing_lookup.get(node_id, 0)
        })
    
    return result


def generate_recommendations(timings: List[Dict[str, Any]]) -> List[str]:
    """
    Generate optimization recommendations based on timing analysis.
    
    SOLUTION: Analyze patterns and suggest improvements.
    """
    recommendations = []
    
    models = [t for t in timings if t['type'] == 'model']
    
    for t in models:
        node = t['node']
        total_time = t['total_seconds']
        compile_time = t['compile_seconds']
        execute_time = t['execute_seconds']
        
        # Very slow models - consider incremental
        if total_time > 30:
            recommendations.append(
                f"🔴 {node} ({total_time}s): Consider incremental materialization"
            )
        elif total_time > 10:
            recommendations.append(
                f"🟡 {node} ({total_time}s): Review for optimization - partitioning/clustering"
            )
        
        # High compile time relative to execute time
        if compile_time > 2 and compile_time > execute_time:
            recommendations.append(
                f"⚙️  {node}: High compile time ({compile_time}s) - simplify Jinja logic"
            )
        
        # Very high execute time - needs optimization
        if execute_time > 20:
            recommendations.append(
                f"🐌 {node}: High execute time ({execute_time}s) - check for full table scans"
            )
    
    # Layer analysis
    layer_times = analyze_by_layer(timings)
    if layer_times.get('marts', 0) > layer_times.get('staging', 0) * 3:
        recommendations.append(
            "📊 Marts layer is significantly slower than staging - consider pre-aggregation"
        )
    
    return recommendations


def calculate_parallelization_efficiency(results: Dict[str, Any], timings: List[Dict[str, Any]]) -> float:
    """
    Calculate how well models ran in parallel.
    
    Ratio > 1 means significant parallelization occurred.
    """
    total_model_time = sum(t['total_seconds'] for t in timings if t['type'] == 'model')
    elapsed_time = results.get('elapsed_time', total_model_time)
    
    if elapsed_time > 0:
        return round(total_model_time / elapsed_time, 2)
    return 1.0


def save_performance_snapshot(timings: List[Dict[str, Any]], output_path: str = 'performance_history.json'):
    """Save performance metrics for historical tracking."""
    models = [t for t in timings if t['type'] == 'model']
    
    snapshot = {
        'timestamp': datetime.now().isoformat(),
        'total_time': sum(t['total_seconds'] for t in models),
        'model_count': len(models),
        'slowest_model': max(models, key=lambda x: x['total_seconds']) if models else None,
        'avg_time': sum(t['total_seconds'] for t in models) / len(models) if models else 0,
        'layer_breakdown': analyze_by_layer(timings)
    }
    
    try:
        with open(output_path, 'r') as f:
            history = json.load(f)
    except FileNotFoundError:
        history = []
    
    history.append(snapshot)
    
    with open(output_path, 'w') as f:
        json.dump(history, f, indent=2, default=str)
    
    print(f"\n📁 Snapshot saved to {output_path}")


# ============================================================
# Main execution
# ============================================================

if __name__ == "__main__":
    try:
        # Load results
        results = load_run_results()
        timings = analyze_timing(results)
        
        # Basic report
        print_report(timings)
        
        # Bottlenecks
        print("\n" + "=" * 60)
        print("BOTTLENECKS (> 5.0s)")
        print("=" * 60)
        bottlenecks = find_bottlenecks(timings, threshold_seconds=5.0)
        if bottlenecks:
            for b in bottlenecks:
                print(f"  ⚠️  {b['node']}: {b['total_seconds']}s")
        else:
            print("  ✅ No bottlenecks found!")
        
        # Layer analysis
        print("\n" + "=" * 60)
        print("TIME BY LAYER")
        print("=" * 60)
        layer_times = analyze_by_layer(timings)
        for layer, time in layer_times.items():
            if time > 0:
                bar = "█" * int(time / 2)
                print(f"  {layer.capitalize():<15} {time:>6.2f}s  {bar}")
        
        # Critical path
        print("\n" + "=" * 60)
        print("CRITICAL PATH")
        print("=" * 60)
        critical_path = calculate_critical_path(timings)
        if critical_path:
            total_path_time = sum(p['time'] for p in critical_path)
            print(f"  Longest dependency chain ({total_path_time:.2f}s total):")
            for i, node in enumerate(critical_path):
                arrow = "→" if i < len(critical_path) - 1 else "⬤"
                print(f"  {arrow} {node['node']} ({node['time']}s)")
        else:
            print("  Could not calculate critical path (manifest not found)")
        
        # Parallelization
        print("\n" + "=" * 60)
        print("PARALLELIZATION")
        print("=" * 60)
        efficiency = calculate_parallelization_efficiency(results, timings)
        print(f"  Efficiency ratio: {efficiency}x")
        if efficiency > 2:
            print("  ✅ Good parallelization!")
        elif efficiency > 1.5:
            print("  👍 Moderate parallelization")
        else:
            print("  ⚠️  Limited parallelization - check dependencies")
        
        # Recommendations
        print("\n" + "=" * 60)
        print("RECOMMENDATIONS")
        print("=" * 60)
        recommendations = generate_recommendations(timings)
        if recommendations:
            for rec in recommendations:
                print(f"  {rec}")
        else:
            print("  ✅ No recommendations - performance looks good!")
        
        # Save snapshot
        save_performance_snapshot(timings)
        
    except FileNotFoundError:
        print("=" * 60)
        print("ERROR: target/run_results.json not found")
        print("=" * 60)
        print("\nPlease run 'dbt build' first to generate artifacts.")
