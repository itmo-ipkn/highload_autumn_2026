export interface Identity {
  user_id: string;
  seq: bigint;
}
export interface OrderedEvent extends Identity {
  processing_order: bigint | null;
}

export function parseDltIdentity(value: Buffer | null): Identity | undefined {
  try {
    const e = JSON.parse(value?.toString("utf8") ?? "null");
    if (
      e &&
      typeof e.user_id === "string" &&
      e.user_id.trim() &&
      Number.isSafeInteger(e.seq) &&
      e.seq > 0
    ) {
      return { user_id: e.user_id, seq: BigInt(e.seq) };
    }
  } catch {
    /* A malformed DLT record is diagnostic, not a failed scan. */
  }
  return undefined;
}

export function analyzeEvents(
  events: OrderedEvent[],
  dlt: Identity[],
  duplicates: bigint,
) {
  const sequences = new Map<string, Set<bigint>>();
  for (const e of [...events, ...dlt]) {
    let values = sequences.get(e.user_id);
    if (!values) sequences.set(e.user_id, (values = new Set()));
    values.add(e.seq);
  }
  let gaps = 0n;
  const gap_examples: { user_id: string; from: string; to: string }[] = [];
  const compare = (a: bigint, b: bigint) => (a < b ? -1 : a > b ? 1 : 0);
  for (const [user_id, values] of sequences) {
    let previous = 0n;
    for (const seq of [...values].sort(compare)) {
      if (seq > previous + 1n) {
        gaps += seq - previous - 1n;
        if (gap_examples.length < 5)
          gap_examples.push({
            user_id,
            from: String(previous + 1n),
            to: String(seq - 1n),
          });
      }
      previous = seq;
    }
  }
  const ordered = new Map<string, OrderedEvent[]>();
  let unknown_order_events = 0;
  for (const e of events) {
    if (e.processing_order === null) {
      unknown_order_events++;
      continue;
    }
    let values = ordered.get(e.user_id);
    if (!values) ordered.set(e.user_id, (values = []));
    values.push(e);
  }
  let ordering_violations = 0;
  const ordering_examples: {
    user_id: string;
    previous_seq: string;
    seq: string;
  }[] = [];
  for (const [user_id, values] of ordered) {
    values.sort((a, b) => compare(a.processing_order!, b.processing_order!));
    for (let i = 1; i < values.length; i++) {
      if (values[i]!.seq < values[i - 1]!.seq) {
        ordering_violations++;
        if (ordering_examples.length < 5)
          ordering_examples.push({
            user_id,
            previous_seq: String(values[i - 1]!.seq),
            seq: String(values[i]!.seq),
          });
      }
    }
  }
  return {
    processed_events: events.length,
    dlt_events: dlt.length,
    duplicates,
    gaps,
    ordering_violations,
    unknown_order_events,
    ordered_events_checked: events.length - unknown_order_events,
    ordering_status: unknown_order_events ? "partial" : "complete",
    gap_examples,
    ordering_examples,
  };
}
export type Verification = ReturnType<typeof analyzeEvents>;

export function formatReport(
  handle: string,
  checkedAt: string,
  result: Verification,
  skipped: number,
) {
  const { gap_examples, ordering_examples, ...metrics } = result;
  return [
    `github: ${handle} | checked_at: ${checkedAt}`,
    "Verification results",
    "--------------------",
    ...Object.entries(metrics).map(([key, value]) => `${key}: ${value}`),
    `skipped_invalid_dlt: ${skipped}`,
    ...gap_examples.map((e) => `gap: ${JSON.stringify(e)}`),
    ...ordering_examples.map((e) => `ordering_violation: ${JSON.stringify(e)}`),
    "Limitations: gaps checks 1..max observed seq; lost tails and wholly missing users cannot be detected without generation bounds.",
    "Limitations: Kafka/PostgreSQL are separate snapshots; in-flight processing can produce temporary gaps.",
    "Limitations: producer restarts reset seq; combined runs cannot be distinguished without run_id.",
    ...(result.unknown_order_events
      ? [
          "Order is partial: historical NULL processing_order records are excluded; processed_at is not used.",
        ]
      : []),
  ].join("\n");
}
