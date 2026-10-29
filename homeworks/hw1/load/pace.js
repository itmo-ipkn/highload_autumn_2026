// Pacing for one infinite k6 run.
//
// R(t) = 5 + 25 * min(t / 120, 1) requests/s, where t is seconds since the
// scenario started. Slots are points on that integral and are shared by all
// VUs. A VU that misses its slot skips it instead of firing a catch-up burst.
//
// The zero-duration externally-controlled executor is what keeps this process
// alive. That meaning of duration "0s" is specific to k6 1.6.0; other
// executors do not treat it as "run until the process is stopped".

export const VUS = 30;
export const RAMP_SECONDS = 120;
export const START_RPS = 5;
export const PLATEAU_RPS = 30;
export const RAMP_SLOTS = 2100;
export const POD_CYCLE = 200;
export const GRACE_SECONDS = 0.2;

export const OPS = {
  GET: 'get',
  PUT: 'put',
  POST: 'post',
  GET_404: 'get404',
  POST_400: 'post400',
  PUT_400: 'put400',
};

const POD_COUNTS = [
  [OPS.GET, 91],
  [OPS.PUT, 91],
  [OPS.POST, 10],
  [OPS.GET_404, 4],
  [OPS.POST_400, 2],
  [OPS.PUT_400, 2],
];

const podSequence = buildPodSequence();

export function targetRps(seconds) {
  const elapsed = Math.max(seconds, 0);
  const progress = Math.min(elapsed / RAMP_SECONDS, 1);
  return START_RPS + (PLATEAU_RPS - START_RPS) * progress;
}

export function cumulativeRequests(seconds) {
  if (seconds <= 0) return 0;
  if (seconds >= RAMP_SECONDS) {
    return RAMP_SLOTS + (seconds - RAMP_SECONDS) * PLATEAU_RPS;
  }
  return START_RPS * seconds + ((PLATEAU_RPS - START_RPS) / (2 * RAMP_SECONDS)) * seconds * seconds;
}

// Inverse of cumulativeRequests for an integer slot index.
export function slotTimeSeconds(slot) {
  if (slot <= RAMP_SLOTS) {
    // Inverse of cumulativeRequests on the ramp: (slope/2)*t^2 + START_RPS*t = slot.
    const slope = (PLATEAU_RPS - START_RPS) / RAMP_SECONDS;
    const linear = (2 * START_RPS) / slope;
    const discriminant = linear * linear + (8 * slot) / slope;
    return (-linear + Math.sqrt(discriminant)) / 2;
  }
  return RAMP_SECONDS + (slot - RAMP_SLOTS) / PLATEAU_RPS;
}

export function scheduleAt(slot) {
  return {
    pod: slot % 2,
    op: podSequence[Math.floor(slot / 2) % POD_CYCLE],
  };
}

export function firstSlotAtOrAfter(vu, timeSec) {
  if (timeSec <= slotTimeSeconds(vu)) return vu;
  let low = 0;
  let high = 1;
  while (slotTimeSeconds(vu + high * VUS) < timeSec) {
    low = high;
    high *= 2;
    if (high > 1e9) {
      throw new Error('slot search diverged');
    }
  }
  while (low + 1 < high) {
    const mid = Math.floor((low + high) / 2);
    if (slotTimeSeconds(vu + mid * VUS) < timeSec) low = mid;
    else high = mid;
  }
  return vu + high * VUS;
}

// state is { vu, slot }, the next slot this VU intends to execute.
export function planIteration(state, now) {
  const target = slotTimeSeconds(state.slot);
  if (now > target + GRACE_SECONDS) {
    const slot = firstSlotAtOrAfter(state.vu, now);
    if (slot <= state.slot) {
      throw new Error(`slot search stuck at ${state.slot}`);
    }
    return { action: 'skip', slot, skipped: (slot - state.slot) / VUS };
  }
  if (now < target) {
    return { action: 'sleep', seconds: target - now, slot: state.slot };
  }
  return { action: 'request', slot: state.slot };
}

// After one request, move to the next still-due slot. Missed slots are counted
// and dropped; the caller must not execute them later.
export function advanceAfterRequest(state, finishedAt) {
  const upcoming = state.slot + VUS;
  if (finishedAt > slotTimeSeconds(upcoming) + GRACE_SECONDS) {
    const slot = firstSlotAtOrAfter(state.vu, finishedAt);
    return { slot, skipped: (slot - upcoming) / VUS };
  }
  return { slot: upcoming, skipped: 0 };
}

function buildPodSequence() {
  const total = POD_COUNTS.reduce((sum, [, count]) => sum + count, 0);
  if (total !== POD_CYCLE) {
    throw new Error(`pod schedule sums to ${total}, want ${POD_CYCLE}`);
  }
  const state = POD_COUNTS.map(([op, count]) => ({ op, count, debt: 0 }));
  const sequence = [];
  for (let index = 0; index < POD_CYCLE; index++) {
    for (const item of state) item.debt += item.count;
    let best = 0;
    for (let candidate = 1; candidate < state.length; candidate++) {
      if (state[candidate].debt > state[best].debt) best = candidate;
    }
    sequence.push(state[best].op);
    state[best].debt -= POD_CYCLE;
  }
  return sequence;
}

function assertInvariants() {
  const counts = {};
  for (let slot = 0; slot < POD_CYCLE * 2; slot++) {
    const scheduled = scheduleAt(slot);
    if (scheduled.pod !== slot % 2) {
      throw new Error(`pod drifted at slot ${slot}`);
    }
    counts[scheduled.op] = (counts[scheduled.op] || 0) + 1;
  }
  const expected = {
    [OPS.GET]: 182,
    [OPS.PUT]: 182,
    [OPS.POST]: 20,
    [OPS.GET_404]: 8,
    [OPS.POST_400]: 4,
    [OPS.PUT_400]: 4,
  };
  for (const [op, count] of Object.entries(expected)) {
    if (counts[op] !== count) {
      throw new Error(`${op} count ${counts[op]}, want ${count}`);
    }
  }
  for (const op of Object.keys(expected)) {
    const pods = new Set();
    for (let slot = 0; slot < POD_CYCLE * 2; slot++) {
      const scheduled = scheduleAt(slot);
      if (scheduled.op === op) pods.add(scheduled.pod);
    }
    if (pods.size !== 2) {
      throw new Error(`${op} is missing from one instance`);
    }
  }
  if (cumulativeRequests(RAMP_SECONDS) !== RAMP_SLOTS) {
    throw new Error('ramp integral is not 2100 slots');
  }
  if (targetRps(0) !== START_RPS || targetRps(RAMP_SECONDS) !== PLATEAU_RPS || targetRps(1000) !== PLATEAU_RPS) {
    throw new Error('target rate endpoints drifted');
  }
  let previous = slotTimeSeconds(0);
  for (let slot = 1; slot < VUS; slot++) {
    const time = slotTimeSeconds(slot);
    if (time - previous < 0.05) {
      throw new Error(`startup slots ${slot - 1} and ${slot} are too close`);
    }
    previous = time;
  }
  if (slotTimeSeconds(VUS - 1) < 4) {
    throw new Error('the first VU slots are not staggered');
  }
}

assertInvariants();
