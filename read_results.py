import json
with open('target/run_results.json') as f:
    results = json.load(f)
for r in results['results']:
    print(r['unique_id'], r['execution_time'])
