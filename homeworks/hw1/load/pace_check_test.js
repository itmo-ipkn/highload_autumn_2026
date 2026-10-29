import { test } from 'node:test';

import { runPaceChecks } from './pace_check.js';

test('paced schedule skips missed slots and keeps both instances covered', () => {
  runPaceChecks();
});
