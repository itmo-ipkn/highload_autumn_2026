#!/usr/bin/env bash
# Эксперимент 3: KEY_MODE=user_id против KEY_MODE=none, каждый прогон с чистого стека
# и одинаковой длительности.
EXP=3
. "$(dirname "$0")/lib.sh"

DURATION=${DURATION:-180}

for mode in user_id none; do
  export KEY_MODE=$mode
  EXP=3-$mode
  fresh "$DURATION"
  stop_and_verify "$RESULTS/3-$mode.txt"
  sql 'SELECT partition, count(*) AS events, count(DISTINCT user_id) AS users,
              count(DISTINCT consumer_id) AS consumers
       FROM events GROUP BY 1 ORDER BY 1' >"$RESULTS/3-$mode-partitions.txt"
  # пользователь в KEY_MODE=user_id живёт в одной партиции, в none - в нескольких
  sql 'SELECT n AS partitions_per_user, count(*) AS users
       FROM (SELECT user_id, count(DISTINCT partition) AS n FROM events GROUP BY 1) t
       GROUP BY 1 ORDER BY 1' >>"$RESULTS/3-$mode-partitions.txt"
done
unset KEY_MODE
log "done"
