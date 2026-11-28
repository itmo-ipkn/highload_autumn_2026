import {
  GRACE_SECONDS,
  OPS,
  PLATEAU_RPS,
  RAMP_SECONDS,
  RAMP_SLOTS,
  VUS,
  advanceAfterRequest,
  cumulativeRequests,
  firstSlotAtOrAfter,
  planIteration,
  scheduleAt,
  slotTimeSeconds,
  targetRps,
} from './pace.js';

function assert(condition, message) {
  if (!condition) throw new Error(message);
}

function close(actual, expected, epsilon, label) {
  if (Math.abs(actual - expected) > epsilon) {
    throw new Error(`${label}: got ${actual}, want ${expected}`);
  }
}

function simulate(vu, durationOf) {
  return simulateFrom({ vu, slot: vu }, 0, durationOf);
}

function simulateFrom(state, startNow, durationOf) {
  const vu = state.vu;
  let now = startNow;
  const fired = [];
  const firedAt = [];
  for (let request = 0; request < 12; request++) {
    for (;;) {
      const decision = planIteration(state, now);
      if (decision.action === 'sleep') {
        now += decision.seconds;
        continue;
      }
      if (decision.action === 'skip') {
        assert(decision.skipped > 0, 'skip moved no slots');
        state = { vu, slot: decision.slot };
        continue;
      }
      assert(decision.action === 'request', `unexpected action ${decision.action}`);
      break;
    }
    fired.push(state.slot);
    firedAt.push(now);
    now += durationOf(request, now);
    const advanced = advanceAfterRequest(state, now);
    assert(advanced.slot > state.slot, 'request did not advance the slot');
    assert(advanced.skipped >= 0, 'negative skip count');
    state = { vu, slot: advanced.slot };
  }
  return { fired, firedAt };
}

export function runPaceChecks() {
  close(cumulativeRequests(60), 5 * 60 + (5 / 48) * 60 * 60, 1e-9, 'ramp integral at 60s');
  assert(cumulativeRequests(RAMP_SECONDS) === RAMP_SLOTS, 'ramp ends at 2100 requests');
  close(cumulativeRequests(180), RAMP_SLOTS + 60 * PLATEAU_RPS, 1e-9, 'plateau integral');
  close(targetRps(60), 17.5, 1e-12, 'mid-ramp rate');

  for (const slot of [0, 1, 30, 500, 2100, 2101, 100000]) {
    close(cumulativeRequests(slotTimeSeconds(slot)), slot, 1e-6, `inverse at slot ${slot}`);
  }
  close(slotTimeSeconds(2130) - slotTimeSeconds(2100), 1, 1e-12, 'plateau VU interval');

  for (const time of [0, 0.5, 10, 119, 120, 180, 3600]) {
    const slot = firstSlotAtOrAfter(0, time);
    assert(slotTimeSeconds(slot) >= time - 1e-9, `slot ${slot} is before ${time}`);
    if (slot >= VUS) {
      assert(slotTimeSeconds(slot - VUS) < time, `earlier slot still covers ${time}`);
    }
  }

  const seen = {};
  for (let slot = 0; slot < 4000; slot++) {
    const scheduled = scheduleAt(slot);
    const key = `${scheduled.pod}:${scheduled.op}`;
    seen[key] = (seen[key] || 0) + 1;
  }
  for (const op of Object.values(OPS)) {
    assert(seen[`0:${op}`] > 0 && seen[`1:${op}`] > 0, `${op} missed an instance`);
    close(seen[`0:${op}`], seen[`1:${op}`], 0, `${op} instance split`);
  }
  const get404 = seen['0:get404'] + seen['1:get404'];
  const bad = seen['0:post400'] + seen['1:post400'] + seen['0:put400'] + seen['1:put400'];
  const posts = seen['0:post'] + seen['1:post'];
  close(get404 / 4000, 0.02, 1e-12, '404 share');
  close(bad / 4000, 0.02, 1e-12, '400 share');
  close(posts / 4000, 0.05, 1e-12, 'successful POST share');

  // An operation that lands on an even global slot also lands on the following
  // odd slot, so the instance choice is not the parity of the operation index.
  let paired = 0;
  for (let slot = 0; slot < 400; slot += 2) {
    if (scheduleAt(slot).op === scheduleAt(slot + 1).op && scheduleAt(slot).pod !== scheduleAt(slot + 1).pod) {
      paired += 1;
    }
  }
  assert(paired === 200, 'operations are not issued to both instances');

  const fast = simulate(0, () => 0.001);
  for (let index = 1; index < fast.fired.length; index++) {
    assert(fast.fired[index] - fast.fired[index - 1] === VUS, `fast VU caught up or skipped: ${fast.fired}`);
    assert(fast.firedAt[index] - fast.firedAt[index - 1] > 0.5, 'fast VU fired a burst');
  }

  const plateau = simulateFrom({ vu: 0, slot: RAMP_SLOTS }, slotTimeSeconds(RAMP_SLOTS), (request) => (request === 1 ? 3.5 : 0.01));
  assert(plateau.fired[2] - plateau.fired[1] > VUS, `plateau stall retried missed slots: ${plateau.fired}`);
  const burst = [];
  for (let index = 1; index < plateau.firedAt.length; index++) {
    if (plateau.firedAt[index] - plateau.firedAt[index - 1] < 0.05) burst.push(index);
  }
  assert(burst.length === 0, `plateau stall fired a catch-up burst at ${burst}`);

  const late = planIteration({ vu: 3, slot: 3 }, slotTimeSeconds(3) + GRACE_SECONDS + 0.01);
  assert(late.action === 'skip', 'late slot was still executed');
  const withinGrace = planIteration({ vu: 3, slot: 3 }, slotTimeSeconds(3) + GRACE_SECONDS);
  assert(withinGrace.action === 'request', 'grace rejected a slot that was still due');

  const jumped = advanceAfterRequest({ vu: 0, slot: 0 }, slotTimeSeconds(0) + 10);
  assert(jumped.slot >= VUS * 2, 'a long request kept the missed slots queued');
  assert(jumped.skipped >= 1, 'a long request did not record skipped slots');
  const afterJump = planIteration({ vu: 0, slot: jumped.slot }, slotTimeSeconds(0) + 10);
  assert(afterJump.action !== 'skip', 'a jump still left a backlog');
}
