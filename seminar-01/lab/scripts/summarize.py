"""Read the exact JSON emitted by our k6 handleSummary()."""
import json, sys
if len(sys.argv)!=2: raise SystemExit('Usage: python3 scripts/summarize.py results/file.json')
with open(sys.argv[1],encoding='utf-8') as f: data=json.load(f)
m=data['summary']['metrics']
def values(key): return m.get(key,{}).get('values',{})
print('RUN:',data.get('label'),'requested:',data.get('rate'),'iterations/s')
for key in ['client_started','http_reqs','iterations','dropped_iterations','http_req_failed','client_success','client_good','client_payload_valid','client_wall_ms','client_wall_success_ms','http_req_duration']:
    print(key, json.dumps(values(key),ensure_ascii=False))
print('All threshold failures:')
for key,value in m.items():
    for threshold,result in value.get('thresholds',{}).items():
        if not result.get('ok',True): print('FAIL',key,threshold)
