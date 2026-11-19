#!/usr/bin/env bash
# Re-runs every runtime bug found in the product review against a fresh node.
# Usage: regressions.sh <base_url> <root_token>      (node started WITHOUT --seed)
# Prints PASS/FAIL per bug. B1 needs a node restart: run with RESTARTED=1 the second time.
set -u
B=$1; T="Authorization: Bearer $2"; J="Content-Type: application/json"
q() { curl -s -X POST "$B/v1/projects/$P/query/insights" -H "$T" -H "$J" -d "$1" | python3 -c "import json,sys;print(json.load(sys.stdin)['series'][0]['overall_value'])"; }
ok() { if [ "$2" = "$3" ]; then echo "PASS  $1 (got $2)"; else echo "FAIL  $1 (got $2, want $3)"; fi; }
TR='"time_range":{"start_ts_ms":1793000000000,"end_ts_ms":1800000000000},"interval":"day"'

if [ "${RESTARTED:-0}" = 1 ]; then
  P=$(cat /tmp/pulse_regress_project)
else
  R=$(curl -s -X POST "$B/v1/projects" -H "$T" -H "$J" -d '{"name":"Regressions","timezone":"UTC"}')
  P=$(echo "$R" | python3 -c "import json,sys;print(json.load(sys.stdin)['project']['id'])")
  K="x-pulse-key: $(echo "$R" | python3 -c "import json,sys;print(json.load(sys.stdin)['api_key']['plaintext_key'])")"
  echo "$P" > /tmp/pulse_regress_project

  # B1 setup: 1200 profiles, first 100 gold
  python3 -c "import json;print(json.dumps({'profiles':[{'distinct_id':f'p{i:04d}','\$set':{'tier':'gold' if i<100 else 'basic'}} for i in range(1200)]}))" \
    | curl -s -o /dev/null -X POST "$B/v1/engage" -H "$K" -H "$J" --data-binary @-
  python3 -c "import json;print(json.dumps({'events':[{'event':'Login','userId':f'p{i:04d}','timestamp':1794900000000+i} for i in range(1200)]}))" \
    | curl -s -o /dev/null -X POST "$B/v1/ingest" -H "$K" -H "$J" --data-binary @-

  # B4 import keeps original timestamp when sentAt is present
  curl -s -o /dev/null -X POST "$B/v1/projects/$P/import" -H "$T" -H "$J" \
    -d '[{"event":"Hist","userId":"h1","timestamp":"2026-11-02T10:00:00Z","sentAt":"2026-11-02T10:00:01Z","messageId":"h-1"}]'
  ts=$(curl -s "$B/v1/projects/$P/export?format=ndjson&start_ts_ms=1700000000000&end_ts_ms=1900000000000&limit=50000" -H "$T" | grep '"Hist"' | python3 -c "import json,sys;print(json.loads(sys.stdin.readline())['event_ts_ms'])")
  ok "B4 import keeps original timestamp" "$ts" 1793613600000

  # B11 Segment identify inside a batch does not drop the batch
  code=$(curl -s -o /dev/null -w "%{http_code}" -X POST "$B/v1/ingest" -H "$K" -H "$J" -d '{"batch":[{"type":"identify","userId":"seg1","traits":{"plan":"pro"}},{"type":"track","event":"Seg Track","userId":"seg1","timestamp":1794900000000}]}')
  ok "B11 Segment identify+track batch accepted" "$code" 200

  # B13 partial batch: good events kept when one is bad
  r=$(curl -s -X POST "$B/v1/ingest" -H "$K" -H "$J" -d '{"events":[{"event":"Good","userId":"g1","timestamp":1794900000000},{"userId":"g2"},{"event":"Good","userId":"g3","timestamp":1794900000001}]}')
  echo "INFO  B13 partial batch response: $r"

  # B7 retry with no id and no timestamp
  for n in 1 2; do curl -s -o /dev/null -X POST "$B/v1/ingest" -H "$K" -H "$J" -d '{"event":"NoTs","userId":"r1","properties":{"a":1}}'; done

  # B5 coercion
  curl -s -o /dev/null -X POST "$B/v1/ingest" -H "$K" -H "$J" -d '{"events":[{"event":"Pay","userId":"c1","timestamp":1794900000000,"properties":{"amount":499}},{"event":"Pay","userId":"c2","timestamp":1794900000001,"properties":{"amount":"499"}}]}'
  curl -s -o /dev/null -X POST "$B/v1/admin/flush" -H "$T"

  # B9 invalid timezone rejected
  code=$(curl -s -o /dev/null -w "%{http_code}" -X POST "$B/v1/projects" -H "$T" -H "$J" -d '{"name":"Bad TZ","timezone":"Mars/Olympus"}')
  ok "B9 invalid timezone rejected" "$code" 400

  # B14 size limit on event names
  code=$(python3 -c "import json;print(json.dumps({'event':'E'*300,'userId':'big'}))" | curl -s -o /dev/null -w "%{http_code}" -X POST "$B/v1/ingest" -H "$K" -H "$J" --data-binary @-)
  echo "INFO  B14 300-char event name status: $code"

  # B8 MCP unknown event is an error
  err=$(curl -s -X POST "$B/v1/mcp" -H "$T" -H "x-pulse-project: $P" -H "$J" -d '{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"query_insights","arguments":{"event_name":"Logn","days":30}}}' | python3 -c "import json,sys;d=json.load(sys.stdin);print(bool(d.get('error') or d.get('result',{}).get('isError')))")
  ok "B8 MCP unknown event flagged" "$err" True
fi

ok "B1 user.tier=gold uniques" "$(q '{'"$TR"',"series":[{"label":"A","event_name":"Login","metric":"uniques","filters":[{"prop_name":"user.tier","filter":{"op":"eq","val":"gold"}}]}]}')" 100.0
n=$(curl -s "$B/v1/projects/$P/users?search=p0005" -H "$T" | python3 -c "import json,sys;d=json.load(sys.stdin);d=d if isinstance(d,list) else d.get('users',d.get('profiles',[]));print(len(d))")
ok "B2 profile search finds old user" "$n" 1
echo "INFO  B7 no-id, no-timestamp event sent twice is stored twice (expected; send messageId): $(q '{'"$TR"',"series":[{"label":"A","event_name":"NoTs","metric":"totals"}]}')"
a=$(q '{'"$TR"',"series":[{"label":"A","event_name":"Pay","metric":"totals","filters":[{"prop_name":"amount","filter":{"op":"eq","val":499}}]}]}')
b=$(q '{'"$TR"',"series":[{"label":"A","event_name":"Pay","metric":"totals","filters":[{"prop_name":"amount","filter":{"op":"eq","val":"499.0"}}]}]}')
ok "B5 eq 499 and eq \"499.0\" agree" "$a" "$b"
ok "B13 good events from partial batch stored" "$(q '{'"$TR"',"series":[{"label":"A","event_name":"Good","metric":"totals"}]}')" 2.0
ok "B11 track event from Segment batch stored" "$(q '{'"$TR"',"series":[{"label":"A","event_name":"Seg Track","metric":"totals"}]}')" 1.0
