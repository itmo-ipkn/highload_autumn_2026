# HW2 — финальный прогон, 27 ноября 2026

[Гипотезы до запуска](hypotheses.md). `SHOW synchronous_commit` вернул `on` во всех четырёх экземплярах Postgres. Три producer (по 35 событий/с, 6 пользователей на под), три consumer, `POISON_RATE=0.02`. Во всех сериях финальные `enqueued=acked=seq_total`, `failed=unacked=0`, lag каждой из трёх партиций равен нулю.

**1. SIGKILL consumer.** `events[1]` передана от `consumer-1` к `consumer-3`: закоммиченный следующий оффсет **362** совпал с первым прочитанным. Идемпотентный sink отбросил 15 повторных доставок. **5 495 ACK = 5 387 уникальных в sink + 108 poison в DLT**; пропусков и нарушений порядка нет. [Снимок](exp1/report.png) · [передача](exp1/takeover.txt) · [первая запись](exp1/claim.jsonl) · [verify: PASS](exp1/exp1-verify.txt). [Graphana](exp1/graphana.png)

**2. Остановка брокеров.** Лидеры указаны в порядке `events[0,1,2]`. ACK и `consumer_events_processed_total` — суммы по трём подам, начало → конец замера.

| Фаза | Лидеры | Размер ISR на каждой партиции | ACK | Consumer processed |
|---|---|---:|---:|---:|
| [До отказа](exp2/isr-before.txt) | 2, 3, 1 | 3 | — | — |
| [Остановлен kafka-2](exp2/isr-one-down.txt) | 3, 3, 1 | 2 | 2 480 → 3 566 | 2 689 → 3 705 |
| [Остановлен также kafka-3](exp2/isr-two-down.txt) | 1, 1, 1 | 1 | 3 869 → 3 869 | 3 869 → 3 869 |
| [Оба вернулись](exp2/isr-restored.txt) | 1, 1, 1 | 3 | — | — |

При одном отказе `events[0]` сменила лидера, запись и обработка продолжились. При двух отказах `kafka-1` ответил на `describe`, но одной реплики недостаточно для `acks=all` при min ISR=2: оба счётчика стояли. После восстановления **5 164 ACK = 5 063 в sink + 101 poison в DLT**. [Снимок](exp2/report.png) · метрики при одном отказе: [начало](exp2/one-down-start.txt)/[конец](exp2/one-down-end.txt), при двух: [начало](exp2/two-down-start.txt)/[конец](exp2/two-down-end.txt) · [финальные counters](exp2/exp2-producers.txt) · [verify: PASS](exp2/exp2-verify.txt). [Graphana](exp2/graphana.png)

**3. Ключ и порядок.** В обеих сериях по **1 500 ACK** (3 × 500), без пропусков, дублей и ошибок DLT. С `user_id`: 1 470 в sink + 30 в DLT, **0** нарушений порядка ([verify: PASS](exp3/user_id-verify.txt)). Без ключа: 1 454 + 46, **243** нарушения ([verify: FAIL только по порядку](exp3/none-verify.txt), [exit 2](exp3/none-status.txt)). События одного пользователя могли попасть в разные партиции и записаться в sink в другом порядке. [Снимок обеих серий](exp3/report.png). [Graphana](exp3/graphana.png)
