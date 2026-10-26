'use strict';

// Runs against a real broker: RABBIT_URL, and RABBIT_CONTAINER for the tests that run rabbitmqctl or docker stop/start.
const { test } = require('node:test');
const assert = require('node:assert/strict');
const { execSync } = require('node:child_process');
const amqplib = require('amqplib');
const { createRabbit, RetryLater } = require('..');

const URL = process.env.RABBIT_URL || 'amqp://guest:guest@localhost:5672';
const CONTAINER = process.env.RABBIT_CONTAINER || 'rabbit';
const NO_BROKER = 'amqp://guest:guest@localhost:5999';
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/;
const quiet = { info() {}, warn() {}, error() {} };
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
const timeouts = () => process.getActiveResourcesInfo().filter((r) => r === 'Timeout').length;
let counter = 0;
const queueName = (label) => `mmc-rabbitmq-test.${label}.${process.pid}.${++counter}`;

async function until(check, ms = 30_000) {
  const end = Date.now() + ms;
  while (!(await check())) {
    if (Date.now() > end) throw new Error('timed out waiting');
    await sleep(100);
  }
}

async function admin(fn) {
  const conn = await amqplib.connect(URL);
  conn.on('error', () => {});
  try {
    const ch = await conn.createConfirmChannel();
    ch.on('error', () => {});
    return await fn(ch);
  } finally {
    await conn.close().catch(() => {});
  }
}

const depth = (queue) => admin(async (ch) => (await ch.checkQueue(queue)).messageCount);
const exists = (queue) => admin((ch) => ch.checkQueue(queue)).then(() => true, () => false);
const rabbitmqctl = (args) =>
  execSync(`docker exec -u rabbitmq ${CONTAINER} rabbitmqctl ${args}`, { stdio: 'ignore', timeout: 60_000 });

const quorum = (queue, args = {}) => (ch) =>
  ch.assertQueue(queue, { durable: true, arguments: { 'x-queue-type': 'quorum', ...args } });

const withDeadLetters = (mq, queue) => {
  const dlq = `${queue}.dl`;
  mq.declare(quorum(dlq));
  mq.declare(quorum(queue, { 'x-dead-letter-exchange': '', 'x-dead-letter-routing-key': dlq }));
  return dlq;
};

function rabbit(t, options = {}) {
  const mq = createRabbit({ url: URL, name: 'mmc-rabbitmq-test', log: quiet, retryDelay: { min: 200, max: 2000 }, ...options });
  t.after(() => mq.close(500));
  return mq;
}

const noBroker = (t, options = {}) => rabbit(t, { url: NO_BROKER, name: 'no-broker', ...options });

test('delivers confirmed, persistent publishes, each with its own message id', async (t) => {
  const queue = queueName('roundtrip');
  const mq = rabbit(t);
  const got = [];
  mq.declare(quorum(queue));
  mq.consume(queue, async (msg) => got.push(msg), { prefetch: 10 });
  await mq.start();
  await Promise.all(Array.from({ length: 50 }, (_, i) => mq.sendToQueue(queue, { i })));
  assert.deepEqual([mq.stats().held, mq.stats().heldBytes], [0, 0]);
  await until(() => got.length === 50);
  await mq.close(2000);
  assert.deepEqual(got.map((m) => JSON.parse(m.content).i).sort((a, b) => a - b), [...Array(50).keys()]);
  assert.equal(new Set(got.map((m) => m.properties.messageId)).size, 50);
  assert.ok(got.every((m) => UUID.test(m.properties.messageId) && m.properties.deliveryMode === 2));
});

test('holds publishes through a broker restart, keeps consuming and declares again', { timeout: 180_000 }, async (t) => {
  const queue = queueName('restart');
  const redeclared = queueName('redeclared');
  const mq = rabbit(t);
  const seen = new Map();
  mq.declare(quorum(queue));
  mq.declare(quorum(redeclared));
  mq.consume(queue, async (msg) => {
    const i = JSON.parse(msg.content).i;
    seen.set(i, (seen.get(i) || 0) + 1);
  }, { prefetch: 20 });
  await mq.start();
  await mq.ready(10_000);
  const sends = [];
  let i = 0;
  const timer = setInterval(() => sends.push(mq.sendToQueue(queue, { i: i++ })), 20);
  await sleep(1000);
  await admin((ch) => ch.deleteQueue(redeclared));
  execSync(`docker stop ${CONTAINER}`, { stdio: 'ignore' });
  await sleep(5000);
  execSync(`docker start ${CONTAINER}`, { stdio: 'ignore' });
  await mq.ready(90_000);
  await sleep(1000);
  clearInterval(timer);
  const results = await Promise.allSettled(sends);
  assert.equal(results.filter((r) => r.status === 'rejected').length, 0);
  await until(() => seen.size === i, 60_000);
  await until(() => exists(redeclared));
  await mq.close(2000);
  const duplicates = [...seen.values()].filter((n) => n > 1).length;
  assert.ok(duplicates <= 20, `${duplicates} of ${i} messages delivered more than once`);
});

test('close() sends publishes held through an outage once the broker is back within its grace', { timeout: 180_000 }, async (t) => {
  const queue = queueName('close-after-outage');
  const mq = rabbit(t);
  mq.declare(quorum(queue));
  await mq.start();
  await mq.ready(10_000);
  execSync(`docker stop ${CONTAINER}`, { stdio: 'ignore' });
  const held = Array.from({ length: 50 }, (_, i) => mq.sendToQueue(queue, { i }));
  execSync(`docker start ${CONTAINER}`, { stdio: 'ignore' });
  await mq.close(90_000);
  const results = await Promise.allSettled(held);
  assert.equal(results.filter((r) => r.status === 'rejected').length, 0);
  assert.equal(await depth(queue), 50);
});

test('RetryLater retries in place and acks once', async (t) => {
  const queue = queueName('retry');
  const mq = rabbit(t);
  let calls = 0;
  mq.declare(quorum(queue));
  mq.consume(queue, async () => {
    if (++calls < 3) throw new RetryLater('dependency down');
  }, { prefetch: 1 });
  await mq.start();
  await mq.sendToQueue(queue, { x: 1 });
  await until(() => calls === 3);
  await mq.close(2000);
  assert.equal(calls, 3);
  assert.equal(await depth(queue), 0);
});

test('other handler errors, including non-Error throws, reject the message and the consumer carries on', async (t) => {
  const queue = queueName('poison');
  const mq = rabbit(t);
  const dlq = withDeadLetters(mq, queue);
  const good = [];
  mq.consume(queue, async (msg) => {
    const body = String(msg.content);
    if (body === 'bad') throw new Error('bad payload');
    if (body === 'worse') throw undefined; // eslint-disable-line no-throw-literal
    good.push(body);
  }, { prefetch: 1 });
  await mq.start();
  for (const body of ['bad', 'worse', 'good']) await mq.sendToQueue(queue, body);
  await until(async () => good.length === 1 && (await depth(dlq)) === 2);
  await mq.close(2000);
});

test('onError: requeue puts a failed message back for another try', async (t) => {
  const queue = queueName('requeue');
  const mq = rabbit(t);
  let calls = 0;
  mq.declare(quorum(queue));
  mq.consume(queue, async () => {
    if (++calls === 1) throw new Error('flaky');
  }, { prefetch: 1, onError: 'requeue' });
  await mq.start();
  await mq.sendToQueue(queue, 'x');
  await until(() => calls === 2);
  await mq.close(2000);
  assert.equal(await depth(queue), 0);
});

test('retryLimit ends RetryLater retries with the onError outcome', async (t) => {
  const queue = queueName('retry-limit');
  const mq = rabbit(t);
  const dlq = withDeadLetters(mq, queue);
  let calls = 0;
  mq.consume(queue, async () => {
    calls++;
    throw new RetryLater('still down');
  }, { prefetch: 1, retryLimit: { attempts: 3 } });
  await mq.start();
  await mq.sendToQueue(queue, 'x');
  await until(async () => (await depth(dlq)) === 1);
  await mq.close(2000);
  assert.equal(calls, 3);
});

test('retryLimit.ms ends RetryLater retries once the next wait would pass it', async (t) => {
  const queue = queueName('retry-ms');
  const mq = rabbit(t);
  const dlq = withDeadLetters(mq, queue);
  let calls = 0;
  mq.consume(queue, async () => {
    calls++;
    throw new RetryLater('still down');
  }, { prefetch: 1, retryLimit: { ms: 500 } });
  await mq.start();
  await mq.sendToQueue(queue, 'x');
  await until(async () => (await depth(dlq)) === 1);
  await mq.close(2000);
  assert.equal(calls, 2);
});

test('RetryLater stops when close() is called and the message goes back to the queue', async (t) => {
  const queue = queueName('retry-close');
  const mq = rabbit(t, { retryDelay: { min: 5000, max: 5000 } });
  let calls = 0;
  mq.declare(quorum(queue));
  mq.consume(queue, async () => {
    calls++;
    throw new RetryLater('down');
  }, { prefetch: 1 });
  await mq.start();
  await mq.sendToQueue(queue, 'x');
  await until(() => calls === 1);
  const begun = Date.now();
  await mq.close(10_000);
  assert.ok(Date.now() - begun < 2000, `close waited ${Date.now() - begun} ms for a RetryLater sleep`);
  await sleep(500);
  assert.equal(calls, 1);
  assert.equal(await depth(queue), 1);
});

test('handlers run at most prefetch at a time', async (t) => {
  const queue = queueName('prefetch');
  const mq = rabbit(t);
  let running = 0;
  let peak = 0;
  let peakInFlight = 0;
  mq.declare(quorum(queue));
  mq.consume(queue, async () => {
    peak = Math.max(peak, ++running);
    await sleep(150);
    peakInFlight = Math.max(peakInFlight, mq.stats().inFlight);
    await sleep(150);
    running--;
  }, { prefetch: 3 });
  await mq.start();
  await Promise.all(Array.from({ length: 10 }, (_, i) => mq.sendToQueue(queue, { i })));
  await until(async () => running === 0 && (await depth(queue)) === 0 && mq.stats().inFlight === 0);
  assert.deepEqual([peak, peakInFlight, mq.stats().consumers, mq.stats().subscribed], [3, 3, 1, 1]);
  await mq.close(2000);
});

test('close waits for in-flight handlers before acking', async (t) => {
  const queue = queueName('drain');
  const mq = rabbit(t);
  let started = false;
  let finished = false;
  mq.declare(quorum(queue));
  mq.consume(queue, async () => {
    started = true;
    await sleep(500);
    finished = true;
  }, { prefetch: 1 });
  await mq.start();
  await mq.sendToQueue(queue, { x: 1 });
  await until(() => started);
  await mq.close(5000);
  assert.equal(finished, true);
  assert.equal(await depth(queue), 0);
});

test('close waits for publishes the broker has not confirmed yet', async (t) => {
  const queue = queueName('close-confirms');
  const mq = rabbit(t);
  mq.declare(quorum(queue));
  await mq.start();
  await mq.ready(10_000);
  const sends = Array.from({ length: 200 }, (_, i) => mq.sendToQueue(queue, { i }));
  await mq.close(5000);
  const results = await Promise.allSettled(sends);
  assert.equal(results.filter((r) => r.status === 'rejected').length, 0);
  assert.equal(await depth(queue), 200);
});

test('rejects a publish to a missing exchange and keeps publishing', async (t) => {
  const queue = queueName('missing-exchange');
  const mq = rabbit(t);
  mq.declare(quorum(queue));
  await mq.start();
  await mq.ready(10_000);
  await assert.rejects(mq.publish(`no-such-exchange-${process.pid}`, 'key', 'x'), /no exchange/);
  await mq.sendToQueue(queue, 'still works');
  await mq.close(2000);
});

test('rejects an unroutable publish instead of dropping it silently', async (t) => {
  const mq = rabbit(t);
  await mq.start();
  await assert.rejects(mq.sendToQueue(`no-such-queue-${process.pid}`, 'x'), /unroutable/);
  await mq.close(2000);
});

test('a publish the broker refuses fails alone and the publishes around it get through', async (t) => {
  const queue = queueName('bad-publish');
  const mq = rabbit(t);
  const seen = new Map();
  const ids = new Map();
  mq.declare(quorum(queue));
  mq.consume(queue, async (msg) => {
    const i = JSON.parse(msg.content).i;
    seen.set(i, (seen.get(i) || 0) + 1);
    ids.set(i, (ids.get(i) || new Set()).add(msg.properties.messageId));
  }, { prefetch: 50 });
  await mq.start();
  await mq.ready(10_000);
  const good = [];
  for (let i = 0; i < 20; i++) good.push(mq.sendToQueue(queue, { i }));
  const bad = mq.sendToQueue(queue, { i: -1 }, { expiration: '-5' });
  for (let i = 20; i < 40; i++) good.push(mq.sendToQueue(queue, { i }));
  const begun = Date.now();
  await assert.rejects(bad, /PRECONDITION_FAILED|expiration/i);
  await Promise.all(good);
  assert.ok(Date.now() - begun < 10_000);
  await until(() => seen.size === 40);
  await sleep(500);
  await mq.close(2000);
  assert.equal(seen.has(-1), false);
  assert.ok([...seen.values()].every((n) => n <= 2), 'a good publish was delivered more than twice');
  assert.ok([...ids.values()].every((set) => set.size === 1), 'a re-sent publish got a new message id');
});

test('bad publishes after several reconnects do not stall the good ones', { timeout: 120_000 }, async (t) => {
  const queue = queueName('bad-after-reconnects');
  const mq = rabbit(t, { retryDelay: { min: 200, max: 30_000 } });
  mq.declare(quorum(queue));
  await mq.start();
  await mq.ready(10_000);
  for (let n = 0; n < 3; n++) {
    rabbitmqctl('close_all_connections reconnect-test');
    await sleep(500);
    await mq.ready(30_000);
  }
  const begun = Date.now();
  const bad = Array.from({ length: 4 }, () => mq.sendToQueue(queue, 'bad', { expiration: '-5' }));
  const good = Array.from({ length: 20 }, (_, i) => mq.sendToQueue(queue, { i }));
  // Reconnects must not have grown the reopen delay.
  await assert.rejects(bad[0], /PRECONDITION_FAILED|expiration/i);
  assert.ok(Date.now() - begun < 1000, `first bad publish found after ${Date.now() - begun} ms`);
  // Each bad publish found must reset the delay, not double it.
  await Promise.all(good);
  assert.ok(Date.now() - begun < 3000, `good publishes took ${Date.now() - begun} ms`);
  for (const p of bad) await assert.rejects(p, /PRECONDITION_FAILED|expiration/i);
  await mq.close(2000);
});

test('a nacked publish during the bad-publish search does not block the rest', async (t) => {
  const full = queueName('full');
  const queue = queueName('after-nack');
  const mq = rabbit(t);
  mq.declare((ch) => ch.assertQueue(full, { durable: true, arguments: { 'x-max-length': 1, 'x-overflow': 'reject-publish' } }));
  mq.declare(quorum(queue));
  await mq.start();
  await mq.ready(10_000);
  await mq.sendToQueue(full, 'fills it');
  const begun = Date.now();
  const bad = mq.sendToQueue(queue, 'bad', { expiration: '-5' });
  mq.sendToQueue(full, 'nacked until there is room');
  const good = Array.from({ length: 20 }, (_, i) => mq.sendToQueue(queue, { i }));
  await assert.rejects(bad, /PRECONDITION_FAILED|expiration/i);
  await Promise.all(good);
  assert.ok(Date.now() - begun < 8000, `good publishes took ${Date.now() - begun} ms`);
  assert.equal(mq.stats().held, 1);
  await mq.close(500);
});

test('a publish amqplib cannot encode fails alone', async (t) => {
  const queue = queueName('unencodable');
  const mq = rabbit(t);
  mq.declare(quorum(queue));
  await mq.start();
  await mq.ready(10_000);
  const before = mq.sendToQueue(queue, 'before');
  const bad = mq.sendToQueue(queue, 'bad', { headers: { notEncodable: () => {} } });
  const after = mq.sendToQueue(queue, 'after');
  await assert.rejects(bad);
  await Promise.all([before, after]);
  await mq.close(2000);
  assert.equal(await depth(queue), 2);
});

test('a burst larger than the socket buffer is sent in full', { timeout: 60_000 }, async (t) => {
  const queue = queueName('burst');
  const mq = rabbit(t);
  mq.declare(quorum(queue));
  await mq.start();
  await mq.ready(10_000);
  const body = Buffer.alloc(4096, 1);
  await Promise.all(Array.from({ length: 3000 }, () => mq.sendToQueue(queue, body)));
  assert.equal(mq.stats().held, 0);
  await mq.close(2000);
  assert.equal(await depth(queue), 3000);
});

test('a consumer timeout aborts the running handler before the message is redelivered', { timeout: 120_000 }, async (t) => {
  const queue = queueName('consumer-timeout');
  const mq = rabbit(t);
  const signals = [];
  let overlapped = false;
  mq.declare(quorum(queue, { 'x-consumer-timeout': 1000 }));
  mq.consume(queue, async (msg, { signal }) => {
    if (signals.some((s) => !s.aborted)) overlapped = true;
    signals.push(signal);
    if (signals.length > 1) return;
    await new Promise((resolve) => {
      signal.addEventListener('abort', resolve);
      setTimeout(resolve, 90_000);
    });
  }, { prefetch: 1 });
  await mq.start();
  await mq.sendToQueue(queue, 'x');
  await until(() => signals.length >= 2, 100_000);
  await mq.close(2000);
  assert.equal(overlapped, false, 'the redelivery started while the first run was still live');
});

test('a broker cancel aborts the signal of in-flight handlers', async (t) => {
  const queue = queueName('cancel-signal');
  const mq = rabbit(t);
  let started = false;
  let aborted = false;
  mq.declare(quorum(queue));
  mq.consume(queue, async (msg, { signal }) => {
    started = true;
    await new Promise((resolve) => {
      signal.addEventListener('abort', resolve);
      setTimeout(resolve, 10_000);
    });
    aborted = signal.aborted;
  }, { prefetch: 1 });
  await mq.start();
  await mq.sendToQueue(queue, 'x');
  await until(() => started);
  await admin((ch) => ch.deleteQueue(queue));
  await until(() => aborted, 5000);
  await mq.close(2000);
});

test('a broken subscription or publish does not replay other consumers’ messages', async (t) => {
  const slow = queueName('isolated-slow');
  const doomed = queueName('isolated-doomed');
  const mq = rabbit(t);
  let calls = 0;
  let done = 0;
  mq.declare(quorum(slow));
  mq.declare(quorum(doomed));
  mq.consume(slow, async () => {
    calls++;
    await sleep(1500);
    done++;
  }, { prefetch: 5 });
  mq.consume(doomed, async () => {}, { prefetch: 1 });
  await mq.start();
  await mq.ready(10_000);
  await until(() => mq.stats().subscribed === 2);
  await Promise.all(Array.from({ length: 5 }, (_, i) => mq.sendToQueue(slow, { i })));
  await until(() => calls === 5);
  await assert.rejects(mq.publish(`no-such-exchange-${process.pid}`, 'key', 'x'), /no exchange/);
  await admin((ch) => ch.deleteQueue(doomed));
  await until(() => done === 5);
  await sleep(1000);
  assert.equal(calls, 5);
  assert.equal(mq.isReady(), true);
  assert.equal(mq.stats().subscribed, 1);
  await mq.close(2000);
  assert.equal(await depth(slow), 0);
});

test('a missing queue does not hold up the others, and its consumer subscribes once it exists', async (t) => {
  const present = queueName('present');
  const absent = queueName('absent');
  const mq = rabbit(t);
  const got = [];
  mq.declare(quorum(present));
  mq.consume(absent, async (msg) => got.push(String(msg.content)), { prefetch: 1 });
  mq.consume(present, async () => {}, { prefetch: 1 });
  await mq.start();
  await mq.ready(10_000);
  await mq.sendToQueue(present, 'x');
  await until(() => mq.stats().subscribed === 1);
  await admin(quorum(absent));
  await until(() => mq.stats().subscribed === 2, 15_000);
  await mq.sendToQueue(absent, 'first');
  await until(() => got.length === 1);
  // A queue deleted under the consumer: the broker cancels the subscription, and it resubscribes once the queue is back.
  await admin((ch) => ch.deleteQueue(absent));
  await until(() => mq.stats().subscribed === 1);
  await admin(quorum(absent));
  await mq.sendToQueue(absent, 'second');
  await until(() => got.length === 2, 15_000);
  await mq.close(2000);
  assert.deepEqual(got, ['first', 'second']);
});

test('a failing declaration does not skip the others or take publishing down', async (t) => {
  const queue = queueName('declared');
  const after = queueName('declared-after');
  const late = queueName('declared-late');
  const mq = rabbit(t);
  mq.declare(quorum(queue));
  // RabbitMQ 4.x answers a transient non-exclusive queue by closing the connection; 3.13 accepts it.
  mq.declare((ch) => ch.assertQueue(queueName('transient'), { durable: false }));
  mq.declare((ch) => ch.assertQueue(queue, { durable: true, arguments: { 'x-queue-type': 'classic' } }));
  mq.declare(quorum(after));
  await mq.start();
  await mq.ready(10_000);
  await mq.sendToQueue(queue, 'x');
  await until(() => exists(after), 10_000);
  mq.declare(quorum(late));
  await until(() => exists(late), 10_000);
  assert.equal(mq.isReady(), true);
  await mq.close(2000);
});

test('a publish the broker nacks is held and sent again until it is confirmed', async (t) => {
  const queue = queueName('nack');
  const mq = rabbit(t);
  const got = [];
  mq.declare((ch) => ch.assertQueue(queue, { durable: true, arguments: { 'x-max-length': 1, 'x-overflow': 'reject-publish' } }));
  await mq.start();
  await mq.ready(10_000);
  await mq.sendToQueue(queue, 'first');
  let confirmed = false;
  const second = mq.sendToQueue(queue, 'second').then(() => (confirmed = true));
  await sleep(1500);
  assert.equal(confirmed, false);
  assert.equal(mq.stats().held, 1);
  mq.consume(queue, async (msg) => got.push(String(msg.content)), { prefetch: 1 });
  await second;
  await until(() => got.length === 2);
  await mq.close(2000);
  assert.deepEqual(got, ['first', 'second']);
});

test('consumers keep consuming while the broker blocks publishing', { timeout: 60_000 }, async (t) => {
  const queue = queueName('alarm');
  const mq = rabbit(t);
  let consumed = 0;
  let held;
  mq.declare(quorum(queue));
  await mq.start();
  await mq.ready(10_000);
  await Promise.all(Array.from({ length: 50 }, (_, i) => mq.sendToQueue(queue, { i })));
  try {
    rabbitmqctl('set_vm_memory_high_watermark 0.000001');
    held = mq.sendToQueue(queue, { i: 50 });
    await until(() => mq.stats().blocked, 15_000);
    assert.equal(mq.isReady(), false);
    // Subscribing after start() works too.
    mq.consume(queue, async () => consumed++, { prefetch: 10 });
    await until(() => consumed >= 50, 20_000);
  } finally {
    rabbitmqctl('set_vm_memory_high_watermark 0.4');
  }
  await held;
  await until(() => consumed === 51);
  await mq.close(2000);
});

test('a handler whose publish hits the hold limit retries in place until there is room', async (t) => {
  const full = queueName('hold-full');
  const queue = queueName('hold-in');
  const out = queueName('hold-out');
  const mq = rabbit(t, { maxHeldBytes: 1000 });
  mq.declare((ch) => ch.assertQueue(full, { durable: true, arguments: { 'x-max-length': 1, 'x-overflow': 'reject-publish' } }));
  const dlq = withDeadLetters(mq, queue);
  mq.declare(quorum(out));
  let calls = 0;
  mq.consume(queue, async () => {
    calls++;
    await mq.sendToQueue(out, 'forwarded');
  }, { prefetch: 1 });
  await mq.start();
  await mq.ready(10_000);
  await mq.sendToQueue(full, 'fills it');
  const held = mq.sendToQueue(full, 'x'.repeat(1000));
  assert.equal(mq.stats().heldBytes, 1000);
  await admin((ch) => (ch.sendToQueue(queue, Buffer.from('in')), ch.waitForConfirms()));
  await until(() => calls >= 3);
  await admin((ch) => ch.purgeQueue(full));
  await held;
  await until(async () => (await depth(out)) === 1);
  await mq.close(2000);
  assert.equal(await depth(queue), 0);
  assert.equal(await depth(dlq), 0);
});

test('consume() needs an explicit prefetch and a known onError', (t) => {
  const mq = noBroker(t);
  assert.throws(() => mq.consume('q', () => {}), /explicit prefetch/);
  assert.throws(() => mq.consume('q', () => {}, { prefetch: 1, onError: 'drop' }), /onError/);
});

test('without a broker: refuses past the hold limit with RetryLater and shuts down promptly', async (t) => {
  const mq = noBroker(t, { maxHeldBytes: 5000 });
  await mq.start();
  let refused = 0;
  let closedUnconfirmed = 0;
  for (let i = 0; i < 10; i++) {
    mq.sendToQueue('q', 'y'.repeat(1000)).catch((err) => {
      if (err instanceof RetryLater && /hold limit/.test(err.message)) refused++;
      if (/closed before the broker confirmed/.test(err.message)) closedUnconfirmed++;
    });
  }
  await assert.rejects(
    mq.sendToQueue('q', 'z'.repeat(5001)),
    (err) => !(err instanceof RetryLater) && /larger than maxHeldBytes/.test(err.message),
  );
  await sleep(50);
  assert.equal(refused, 5);
  assert.deepEqual([mq.stats().held, mq.stats().heldBytes], [5, 5000]);
  assert.equal(mq.isReady(), false);
  await mq.close(300);
  await sleep(10);
  assert.equal(closedUnconfirmed, 5);
  assert.equal(mq.stats().heldBytes, 0);
});

test('createRabbit rejects options that would disable the hold cap or spin its timers', () => {
  const make = (options) => createRabbit({ url: NO_BROKER, name: 'options', log: quiet, ...options });
  for (const maxHeldBytes of [NaN, 0, -1, Infinity, null, '512Mi']) {
    assert.throws(() => make({ maxHeldBytes }), /maxHeldBytes must be a positive number/);
  }
  for (const heartbeat of [NaN, -1, null]) {
    assert.throws(() => make({ heartbeat }), /heartbeat must be 0 or more/);
  }
  for (const retryDelay of [{ min: NaN }, { max: NaN }, { min: 0 }, { min: 5000, max: 1000 }, { min: null }]) {
    assert.throws(() => make({ retryDelay }), /retryDelay needs 0 < min <= max/);
  }
  assert.doesNotThrow(() => make({ heartbeat: 0, retryDelay: { min: 1, max: 1 } }));
});

test('without a broker: the hold cap defaults to 64 MiB', { timeout: 5000 }, async (t) => {
  const mq = noBroker(t);
  await mq.start();
  await assert.rejects(
    mq.sendToQueue('q', Buffer.alloc(64 * 2 ** 20 + 1)),
    (err) => !(err instanceof RetryLater) && /larger than maxHeldBytes/.test(err.message),
  );
  mq.sendToQueue('q', Buffer.alloc(64 * 2 ** 20)).catch(() => {});
  assert.equal(mq.stats().heldBytes, 64 * 2 ** 20);
  await mq.close(200);
});

test('without a broker: publishWithin rejects at its deadline and holds nothing', async (t) => {
  const mq = noBroker(t);
  await mq.start();
  await assert.rejects(mq.publishWithin(300, '', 'q', 'x'), /no broker confirm within 300 ms/);
  assert.equal(mq.stats().held, 0);
  await assert.rejects(mq.ready(200), /not ready within 200 ms/);
  await mq.close(300);
});

test('without a broker: publish failures are rejected promises, never throws or unhandled rejections', async (t) => {
  const unhandled = [];
  const record = (err) => unhandled.push(err);
  process.on('unhandledRejection', record);
  try {
    const mq = noBroker(t, { maxHeldBytes: 10 });
    await mq.start();
    await assert.rejects(mq.sendToQueue('q', undefined), /a Buffer, a string or a JSON value/);
    mq.sendToQueue('q', 'x'.repeat(100));
    mq.sendToQueue('q', 'small');
    mq.publishWithin(100, '', 'q', 'tiny');
    await sleep(200);
    await mq.close(200);
    mq.sendToQueue('q', 'after close');
    await sleep(50);
  } finally {
    process.off('unhandledRejection', record);
  }
  assert.equal(unhandled.length, 0);
});

test('without a broker: start() and close() are idempotent, and close() stays within its grace and leaves no timers', async (t) => {
  const before = timeouts();
  const mq = noBroker(t);
  mq.consume('q', () => {}, { prefetch: 1 });
  const starting = mq.start();
  assert.equal(mq.start(), starting);
  await starting;
  mq.sendToQueue('q', 'x').catch(() => {});
  await sleep(1500);
  const begun = Date.now();
  const closing = mq.close(500);
  assert.equal(mq.close(500), closing);
  await closing;
  assert.ok(Date.now() - begun < 700, `close took ${Date.now() - begun} ms`);
  await assert.rejects(mq.sendToQueue('q', 'x'), /closed/);
  const neverStarted = noBroker(t);
  await neverStarted.close(100);
  await assert.rejects(neverStarted.start(), /closed/);
  await sleep(50);
  assert.ok(timeouts() <= before, `${timeouts() - before} timers left running`);
});
