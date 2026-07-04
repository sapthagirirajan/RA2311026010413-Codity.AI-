const { v4: uuidv4 } = require('uuid');

const now = () => new Date().toISOString();
const newId = () => uuidv4();

/** Retry delay calculators for the three required strategies. */
function computeRetryDelayMs(policy, attemptNumber) {
  const { strategy, base_delay_ms: base, max_delay_ms: maxDelay } = policy;
  let delay;
  if (strategy === 'fixed') {
    delay = base;
  } else if (strategy === 'linear') {
    delay = base * attemptNumber;
  } else {
    // exponential backoff with jitter
    delay = base * Math.pow(2, attemptNumber - 1);
  }
  delay = Math.min(delay, maxDelay);
  const jitter = Math.floor(delay * 0.1 * Math.random());
  return delay + jitter;
}

module.exports = { now, newId, computeRetryDelayMs };
