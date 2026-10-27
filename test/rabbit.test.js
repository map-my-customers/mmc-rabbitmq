'use strict';

// Runs against a real broker: RABBIT_URL, and RABBIT_CONTAINER for the tests that run rabbitmqctl or docker stop/start.
const { test } = require('node:test');
const assert = require('node:assert/strict');
const { execSync } = require('node:child_process');
const { monitorEventLoopDelay } = require('node:perf_hooks');
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

const queueName = (t, label) => {
  const queue = `mmc-rabbitmq-test.${label}.${process.pid}.${++counter}`;
  t.after(() => admin((ch) => ch.deleteQueue(queue)).catch(() => {}));
  return queue;
};
const depth = (queue) => admin(async (ch) => (await ch.checkQueue(queue)).messageCount);
const consumerCount = (queue) => admin(async (ch) => (await ch.checkQueue(queue)).consumerCount);
const exists = (queue) => admin((ch) => ch.checkQueue(queue)).then(() => true, () => false);
const rabbitmqctl = (args) =>
  execSync(`docker exec -u rabbitmq ${CONTAINER} rabbitmqctl ${args}`, { stdio: 'ignore', timeout: 60_000 });

const quorum = (queue, args = {}) => (ch) =>
  ch.assertQueue(queue, { durable: true, arguments: { 'x-queue-type': 'quorum', ...args } });

const withDeadLetters = (t, mq, queue) => {
  const dlq = `${queue}.dl`;
  t.after(() => admin((ch) => ch.deleteQueue(dlq)).catch(() => {}));
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
  const queue = queueName(t, 'roundtrip');
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
  const queue = queueName(t, 'restart');
  const redeclared = queueName(t, 'redeclared');
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
  // Held through the outage: it must wait for the queue's re-declaration, not go out as unroutable before it.
  sends.push(mq.sendToQueue(redeclared, 'held'));
  await sleep(5000);
  execSync(`docker start ${CONTAINER}`, { stdio: 'ignore' });
  await mq.ready(90_000);
  await sleep(1000);
  clearInterval(timer);
  const results = await Promise.allSettled(sends);
  assert.deepEqual(results.filter((r) => r.status === 'rejected').map((r) => r.reason.message), []);
  await until(() => seen.size === i, 60_000);
  assert.equal(await depth(redeclared), 1);
  await mq.close(2000);
  const duplicates = [...seen.values()].filter((n) => n > 1).length;
  assert.ok(duplicates <= 20, `${duplicates} of ${i} messages delivered more than once`);
});

test('close() sends publishes held through an outage once the broker is back within its grace', { timeout: 180_000 }, async (t) => {
  const queue = queueName(t, 'close-after-outage');
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
  const queue = queueName(t, 'retry');
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
  const queue = queueName(t, 'poison');
  const mq = rabbit(t);
  const dlq = withDeadLetters(t, mq, queue);
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
  const queue = queueName(t, 'requeue');
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
  const queue = queueName(t, 'retry-limit');
  const mq = rabbit(t);
  const dlq = withDeadLetters(t, mq, queue);
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
  const queue = queueName(t, 'retry-ms');
  const mq = rabbit(t);
  const dlq = withDeadLetters(t, mq, queue);
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
  const queue = queueName(t, 'retry-close');
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
  const queue = queueName(t, 'prefetch');
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
  const queue = queueName(t, 'drain');
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
  const queue = queueName(t, 'close-confirms');
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
  const queue = queueName(t, 'missing-exchange');
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
  const queue = queueName(t, 'bad-publish');
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
  const queue = queueName(t, 'bad-after-reconnects');
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
  const full = queueName(t, 'full');
  const queue = queueName(t, 'after-nack');
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

test('once the refused publish is found, the rest go out together again', async (t) => {
  const queue = queueName(t, 'after-found');
  const mq = rabbit(t);
  mq.declare(quorum(queue));
  await mq.start();
  await mq.ready(10_000);
  const watched = await watchPublishes(t, queue);
  const bad = mq.sendToQueue(queue, 'bad', { expiration: '-5' });
  const good = Array.from({ length: 100 }, (_, i) => mq.sendToQueue(queue, { i }));
  await assert.rejects(bad, /PRECONDITION_FAILED|expiration/i);
  watched.maxUnconfirmed = watched.unconfirmed;
  await Promise.all(good);
  assert.ok(watched.maxUnconfirmed > 1, 'the publishes after the refused one were re-sent one at a time');
  await mq.close(2000);
});

test('a publish amqplib cannot encode fails alone', async (t) => {
  const queue = queueName(t, 'unencodable');
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
  const queue = queueName(t, 'burst');
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
  const queue = queueName(t, 'consumer-timeout');
  const mq = rabbit(t);
  const signals = [];
  let overlapped = false;
  mq.declare(quorum(queue, { 'x-consumer-timeout': 1000 }));
  mq.consume(queue, async (msg, { signal }) => {
    if (signals.some((s) => !s.aborted)) overlapped = true;
    signals.push(signal);
    if (signals.length > 1) return;
    await new Promise((resolve) => {
      const timer = setTimeout(resolve, 90_000);
      signal.addEventListener('abort', () => {
        clearTimeout(timer);
        resolve();
      });
    });
  }, { prefetch: 1 });
  await mq.start();
  await mq.sendToQueue(queue, 'x');
  await until(() => signals.length >= 2, 100_000);
  await mq.close(2000);
  assert.equal(overlapped, false, 'the redelivery started while the first run was still live');
});

test('a broker cancel aborts the signal of in-flight handlers', async (t) => {
  const queue = queueName(t, 'cancel-signal');
  const mq = rabbit(t);
  let started = false;
  let aborted = false;
  mq.declare(quorum(queue));
  mq.consume(queue, async (msg, { signal }) => {
    started = true;
    await new Promise((resolve) => {
      const timer = setTimeout(resolve, 10_000);
      signal.addEventListener('abort', () => {
        clearTimeout(timer);
        resolve();
      });
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
  const slow = queueName(t, 'isolated-slow');
  const doomed = queueName(t, 'isolated-doomed');
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
  const present = queueName(t, 'present');
  const absent = queueName(t, 'absent');
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
  const queue = queueName(t, 'declared');
  const after = queueName(t, 'declared-after');
  const late = queueName(t, 'declared-late');
  const mq = rabbit(t);
  mq.declare(quorum(queue));
  // RabbitMQ 4.x answers a transient non-exclusive queue by closing the connection; 3.13 accepts it.
  mq.declare((ch) => ch.assertQueue(queueName(t, 'transient'), { durable: false }));
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
  const queue = queueName(t, 'nack');
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
  const queue = queueName(t, 'alarm');
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
  const full = queueName(t, 'hold-full');
  const queue = queueName(t, 'hold-in');
  const out = queueName(t, 'hold-out');
  const mq = rabbit(t, { maxHeldBytes: 1000 });
  mq.declare((ch) => ch.assertQueue(full, { durable: true, arguments: { 'x-max-length': 1, 'x-overflow': 'reject-publish' } }));
  const dlq = withDeadLetters(t, mq, queue);
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
  assert.throws(() => mq.consume('q', () => {}, { prefetch: 65536 }), /0 to 65535/);
  for (const retryLimit of [{ attempts: NaN }, { attempts: 0 }, { ms: -1 }, { ms: '5000' }]) {
    assert.throws(() => mq.consume('q', () => {}, { prefetch: 1, retryLimit }), /retryLimit/);
  }
  assert.doesNotThrow(() => mq.consume('q', () => {}, { prefetch: 65535, retryLimit: { attempts: 3, ms: Infinity } }));
});

test('without a broker: refuses past the hold limit with RetryLater and shuts down promptly', async (t) => {
  // Each held publish counts as its body plus 1536 bytes: room for five 1000-byte bodies.
  const mq = noBroker(t, { maxHeldBytes: 5 * 2536 });
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
    mq.sendToQueue('q', 'z'.repeat(5 * 2536 + 1)),
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
  for (const heartbeat of [NaN, -1, null, 0.5, 65536]) {
    assert.throws(() => make({ heartbeat }), /heartbeat must be a whole number of seconds/);
  }
  for (const retryDelay of [{ min: NaN }, { max: NaN }, { min: 0 }, { min: 5000, max: 1000 }, { min: null }]) {
    assert.throws(() => make({ retryDelay }), /retryDelay needs 0 < min <= max/);
  }
  assert.doesNotThrow(() => make({ heartbeat: 0, retryDelay: { min: 1, max: 1 } }));
  assert.doesNotThrow(() => make({ retryDelay: { max: 5000 } }));
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
  await assert.rejects(mq.sendToQueue('q', 'x'), RetryLater);
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
  await assert.rejects(mq.sendToQueue('q', 'x'), (err) => err instanceof RetryLater && /closed/.test(err.message));
  const neverStarted = noBroker(t);
  await neverStarted.close(100);
  await assert.rejects(neverStarted.start(), /closed/);
  await sleep(50);
  assert.ok(timeouts() <= before, `${timeouts() - before} timers left running`);
});

// Wraps amqplib's ConfirmChannel publish for one test, to see what the library puts on the wire.
async function watchPublishes(t, routingKey) {
  const proto = await admin((ch) => Object.getPrototypeOf(ch));
  const original = proto.publish;
  const seen = { sent: 0, unconfirmed: 0, maxUnconfirmed: 0 };
  proto.publish = function (exchange, key, content, options, cb) {
    if (key !== routingKey) return original.call(this, exchange, key, content, options, cb);
    seen.sent++;
    seen.maxUnconfirmed = Math.max(seen.maxUnconfirmed, ++seen.unconfirmed);
    return original.call(this, exchange, key, content, options, (...args) => {
      seen.unconfirmed--;
      if (cb) cb(...args);
    });
  };
  t.after(() => (proto.publish = original));
  return seen;
}

test('a nacked publish waits out its backoff instead of going out with every other publish', async (t) => {
  const full = queueName(t, 'nack-backoff-full');
  const other = queueName(t, 'nack-backoff-other');
  const mq = rabbit(t, { retryDelay: { min: 500, max: 2000 } });
  mq.declare((ch) => ch.assertQueue(full, { durable: true, arguments: { 'x-max-length': 1, 'x-overflow': 'reject-publish' } }));
  mq.declare(quorum(other));
  await mq.start();
  await mq.ready(10_000);
  await mq.sendToQueue(full, 'fills it');
  const watched = await watchPublishes(t, full);
  mq.sendToQueue(full, 'nacked').catch(() => {});
  const end = Date.now() + 1200;
  while (Date.now() < end) {
    mq.sendToQueue(other, 'x');
    await sleep(20);
  }
  // Sent at 0 ms and again after 500 ms; the next try is at 1500 ms.
  assert.ok(watched.sent <= 2, `the nacked publish was sent ${watched.sent} times in 1.2 s`);
  await mq.close(500);
});

test('a large held backlog drains without stalling the event loop, at most 256 unconfirmed at once', { timeout: 120_000 }, async (t) => {
  const queue = queueName(t, 'backlog');
  const mq = rabbit(t, { maxHeldBytes: 256 * 2 ** 20 });
  mq.declare((ch) => ch.assertQueue(queue, { durable: true, arguments: { 'x-queue-type': 'classic' } }));
  const watched = await watchPublishes(t, queue);
  const count = 100_000;
  // Held until start(), like a backlog built during an outage.
  const sends = Array.from({ length: count }, (_, i) => mq.sendToQueue(queue, String(i), { persistent: false }));
  const lag = monitorEventLoopDelay({ resolution: 10 });
  lag.enable();
  await mq.start();
  await Promise.all(sends);
  lag.disable();
  assert.ok(watched.maxUnconfirmed <= 256, `${watched.maxUnconfirmed} publishes unconfirmed at once`);
  assert.ok(lag.max / 1e6 < 250, `the event loop stalled for ${Math.round(lag.max / 1e6)} ms`);
  await mq.close(2000);
  assert.equal(await depth(queue), count);
});

test('without a broker: close() releases a large backlog promptly', async (t) => {
  const mq = noBroker(t, { maxHeldBytes: 2 ** 30 });
  await mq.start();
  let released = 0;
  for (let i = 0; i < 100_000; i++) mq.sendToQueue('q', 'x').catch(() => released++);
  const begun = Date.now();
  await mq.close(500);
  assert.ok(Date.now() - begun < 2000, `close(500) took ${Date.now() - begun} ms to release 100000 held publishes`);
  assert.equal(released, 100_000);
});

test('close() requeues a message whose handler is still waiting for its publish to be confirmed', async (t) => {
  const full = queueName(t, 'close-full');
  const queue = queueName(t, 'close-in');
  const errors = [];
  const mq = rabbit(t, { log: { ...quiet, error: (line) => errors.push(line) } });
  mq.declare((ch) => ch.assertQueue(full, { durable: true, arguments: { 'x-max-length': 1, 'x-overflow': 'reject-publish' } }));
  const dlq = withDeadLetters(t, mq, queue);
  let started = false;
  mq.consume(queue, async () => {
    started = true;
    await mq.sendToQueue(full, 'nacked until close');
  }, { prefetch: 1, retryLimit: { attempts: 1 } });
  await mq.start();
  await mq.ready(10_000);
  await mq.sendToQueue(full, 'fills it');
  await admin((ch) => (ch.sendToQueue(queue, Buffer.from('in')), ch.waitForConfirms()));
  await until(() => started);
  await mq.close(1000);
  // The broker state alone can't show it: the channel close usually overtakes a reject sent at the same moment.
  assert.deepEqual(errors.filter((line) => /handler failed/.test(line)), []);
  // A message returned by a closed channel can take a moment to show as ready.
  await until(async () => (await depth(queue)) === 1, 5000);
  assert.equal(await depth(dlq), 0);
});

// Holds back each basic.consume on `queue`, so a subscription stays half done while the library's timers fire.
async function slowConsume(t, queue, ms) {
  const proto = await admin((ch) => Object.getPrototypeOf(Object.getPrototypeOf(ch)));
  const original = proto.consume;
  const calls = new Map();
  proto.consume = async function (name, ...rest) {
    if (name !== queue) return original.call(this, name, ...rest);
    calls.set(this, (calls.get(this) ?? 0) + 1);
    await sleep(ms);
    return original.call(this, name, ...rest);
  };
  t.after(() => (proto.consume = original));
  return calls;
}

test('a dropped consume connection comes back with one subscription per consumer', { timeout: 120_000 }, async (t) => {
  const queue = queueName(t, 'one-subscription');
  const mq = rabbit(t);
  mq.declare(quorum(queue));
  const calls = await slowConsume(t, queue, 5000);
  const sent = () => [...calls.values()].reduce((a, b) => a + b, 0);
  mq.consume(queue, async () => {}, { prefetch: 1 });
  await mq.start();
  await until(async () => (await consumerCount(queue).catch(() => 0)) === 1, 15_000);
  // Each drop doubles the retry delay, and the last timer (800 ms) fires while the re-subscribe waits for consume-ok.
  // The waits let each timer fire before the next drop: rabbitmqctl blocks the event loop.
  for (let drop = 1; drop <= 3; drop++) {
    rabbitmqctl('close_all_connections subscription-test');
    await until(() => sent() > drop, 10_000);
    if (drop < 3) await sleep(1000);
  }
  await until(async () => (await consumerCount(queue).catch(() => 0)) >= 1, 30_000);
  await sleep(2500);
  assert.equal(await consumerCount(queue), 1);
  assert.ok([...calls.values()].every((n) => n === 1), 'a channel was subscribed twice');
  await mq.close(2000);
});

test('a failed declaration is retried until it succeeds', async (t) => {
  const queue = queueName(t, 'bind-retry');
  const exchange = queueName(t, 'bind-retry-ex');
  t.after(() => admin((ch) => ch.deleteExchange(exchange)).catch(() => {}));
  const mq = rabbit(t);
  mq.declare(quorum(queue));
  mq.declare((ch) => ch.bindQueue(queue, exchange, 'k'));
  await mq.start();
  await mq.ready(10_000);
  await admin((ch) => ch.assertExchange(exchange, 'direct', { durable: true }));
  await until(() => mq.publish(exchange, 'k', 'x').then(() => true, () => false), 15_000);
  await mq.close(2000);
  assert.equal(await depth(queue), 1);
});

test('close() clears the library retry timers, and a round still running starts none and logs nothing', async (t) => {
  const queue = queueName(t, 'timers');
  const before = timeouts();
  const errors = [];
  const mq = rabbit(t, { retryDelay: { min: 5000, max: 5000 }, log: { ...quiet, error: (line) => errors.push(line) } });
  mq.declare(quorum(queue));
  // Fails every time, so a declaration retry is pending when close() runs.
  mq.declare((ch) => ch.assertQueue(queue, { durable: true, arguments: { 'x-queue-type': 'classic' } }));
  await mq.start();
  await mq.ready(10_000);
  let started;
  const running = new Promise((resolve) => (started = resolve));
  mq.declare(async () => {
    started();
    await sleep(500);
    throw new Error('fails after close()');
  });
  await running;
  await mq.close(1000);
  const atClose = errors.length;
  await sleep(600);
  assert.ok(timeouts() <= before, `${timeouts() - before} timers left running`);
  assert.deepEqual(errors.slice(atClose), []);
});

test('without a broker: ready() and start() reject once close() has started, and leave no timers', async (t) => {
  const before = timeouts();
  const mq = noBroker(t);
  await mq.start();
  mq.sendToQueue('q', 'held').catch(() => {});
  const waiting = mq.ready(60_000);
  const begun = Date.now();
  const closing = mq.close(2000);
  await assert.rejects(waiting, /closed/);
  assert.ok(Date.now() - begun < 500, `ready() rejected ${Date.now() - begun} ms after close() started`);
  await closing;
  await assert.rejects(mq.ready(60_000), /closed/);
  await assert.rejects(mq.start(), /closed/);
  await sleep(50);
  assert.ok(timeouts() <= before, `${timeouts() - before} timers left running`);
});

test('without a broker: publishWithin and ready reject a timeout setTimeout cannot keep', async (t) => {
  const mq = noBroker(t);
  await mq.start();
  for (const ms of [NaN, Infinity, 0, -1, 2 ** 31, '100']) {
    await assert.rejects(mq.publishWithin(ms, '', 'q', 'x'), TypeError);
    await assert.rejects(mq.ready(ms), TypeError);
  }
  assert.equal(mq.stats().held, 0);
  await mq.close(200);
});

test('after a reconnect, isReady() stays false until that connection’s declarations have run', { timeout: 60_000 }, async (t) => {
  const queue = queueName(t, 'redeclare-ready');
  const mq = rabbit(t);
  let rounds = 0;
  let gate = Promise.resolve();
  mq.declare(async (ch) => {
    rounds++;
    await gate;
    await quorum(queue)(ch);
  });
  await mq.start();
  await mq.ready(10_000);
  let open;
  gate = new Promise((resolve) => (open = resolve));
  rabbitmqctl('close_all_connections redeclare-test');
  await until(() => rounds === 2, 10_000);
  // Long enough for the publish channel to reopen.
  await sleep(1000);
  assert.equal(mq.isReady(), false);
  const sent = mq.publish('', queue, 'x');
  open();
  await mq.ready(10_000);
  await sent;
  await mq.close(2000);
  assert.equal(await depth(queue), 1);
});

test('a declaration the broker never answers fails after 10 s, and the next one gets a new connection', { timeout: 60_000 }, async (t) => {
  const queue = queueName(t, 'declare-hang');
  const before = timeouts();
  const probe = await amqplib.connect(URL);
  const proto = Object.getPrototypeOf(probe);
  await probe.close();
  // A connection that ran the hung declaration answers nothing more, like a broker that has gone quiet.
  const stuck = new Set();
  const createChannel = proto.createChannel;
  proto.createChannel = function (...args) {
    return stuck.has(this.connection) ? new Promise(() => {}) : createChannel.apply(this, args);
  };
  t.after(() => (proto.createChannel = createChannel));
  const mq = rabbit(t);
  let calls = 0;
  mq.declare((ch) => {
    calls++;
    stuck.add(ch.connection);
    return new Promise(() => {});
  });
  mq.declare(quorum(queue));
  await mq.start();
  await mq.ready(15_000);
  await mq.publish('', queue, 'x');
  // Its retry is waiting for an answer when close() runs.
  await until(() => calls === 2, 5000);
  await mq.close(2000);
  assert.equal(await depth(queue), 1);
  await sleep(50);
  assert.ok(timeouts() <= before, `${timeouts() - before} timers left running`);
});

test('a handler that publishes after close() has finished puts its message back', async (t) => {
  const queue = queueName(t, 'late-publish');
  const errors = [];
  const mq = rabbit(t, { log: { ...quiet, error: (line) => errors.push(line) } });
  const dlq = withDeadLetters(t, mq, queue);
  let release;
  const closed = new Promise((resolve) => (release = resolve));
  mq.consume(
    queue,
    async () => {
      await closed;
      await mq.publish('', queue, 'again');
    },
    { prefetch: 1 },
  );
  await mq.start();
  await mq.ready(10_000);
  await mq.publish('', queue, 'x');
  await until(() => mq.stats().inFlight === 1, 10_000);
  await mq.close(500);
  release();
  await sleep(200);
  assert.deepEqual(errors.filter((line) => line.includes('handler failed')), []);
  await until(async () => (await depth(queue)) === 1, 5000);
  assert.equal(await depth(dlq), 0);
});

test('a declaration round still connecting when close() finishes declares nothing', async (t) => {
  const mq = rabbit(t);
  let runs = 0;
  mq.declare(async () => runs++);
  await mq.start();
  await mq.ready(10_000);
  // Slows the declare connection of the round the drop starts, so close() finishes while it is still connecting.
  const original = amqplib.connect;
  let connecting;
  const started = new Promise((resolve) => (connecting = resolve));
  amqplib.connect = async function (url, options) {
    if (/\(declare\)$/.test(options?.clientProperties?.connection_name)) {
      connecting();
      await sleep(400);
    }
    return original.call(this, url, options);
  };
  t.after(() => (amqplib.connect = original));
  rabbitmqctl('close_all_connections declare-after-close');
  await started;
  await mq.close(1000);
  const atClose = runs;
  await sleep(1000);
  assert.equal(runs, atClose, 'a declaration ran after close() returned');
});

test('publishes made while amqplib closes the publish channel itself are held, not rejected', async (t) => {
  const queue = queueName(t, 'closing');
  const mq = rabbit(t);
  mq.declare(quorum(queue));
  await mq.start();
  await mq.ready(10_000);
  // The next publish to `queue` makes amqplib close its channel with an error, as it does on an unexpected frame.
  const proto = await admin((ch) => Object.getPrototypeOf(ch));
  const original = proto.publish;
  let armed = true;
  proto.publish = function (exchange, routingKey, ...rest) {
    if (armed && routingKey === queue) {
      armed = false;
      this.closeWithError(0, 'closed by the test', 541, new Error('closed by the test'));
    }
    return original.call(this, exchange, routingKey, ...rest);
  };
  t.after(() => (proto.publish = original));
  await Promise.all(Array.from({ length: 20 }, (_, i) => mq.sendToQueue(queue, String(i))));
  await mq.close(2000);
  assert.equal(await depth(queue), 20);
});

// The heartbeat each connection whose name contains `name` agreed with the broker.
const heartbeats = (name) =>
  execSync(`docker exec -u rabbitmq ${CONTAINER} rabbitmqctl -q list_connections client_properties timeout`, {
    encoding: 'utf8',
    timeout: 60_000,
  })
    .split('\n')
    .filter((line) => line.includes(name))
    .map((line) => Number(line.trim().split(/\s+/).pop()));

// Wraps amqplib's ConfirmChannel.publish for one test; hook(key, cb) returns the confirm callback to use.
async function patchPublish(t, hook) {
  const proto = await admin((ch) => Object.getPrototypeOf(ch));
  const original = proto.publish;
  proto.publish = function (exchange, key, content, options, cb) {
    return original.call(this, exchange, key, content, options, hook(key, cb));
  };
  t.after(() => (proto.publish = original));
}

test('an unroutable publish rejects even when the caller passes headers', async (t) => {
  const mq = rabbit(t);
  await mq.start();
  await mq.ready(10_000);
  await assert.rejects(mq.sendToQueue(`no-such-queue-${process.pid}`, 'x', { headers: { trace: 'abc' } }), /unroutable/);
  await mq.close(2000);
});

test('a publishWithin that timed out while held is never sent later', async (t) => {
  const queue = queueName(t, 'late-held');
  const mq = rabbit(t);
  let release;
  const gate = new Promise((resolve) => (release = resolve));
  mq.declare(quorum(queue));
  mq.declare(async () => gate);
  await mq.start();
  await assert.rejects(mq.publishWithin(300, '', queue, 'too late'), /no broker confirm within 300 ms/);
  assert.equal(mq.stats().held, 0);
  release();
  await mq.ready(10_000);
  await sleep(500);
  await mq.close(2000);
  assert.equal(await depth(queue), 0);
});

test('a publishWithin that timed out while waiting out a nack is never sent later', async (t) => {
  const full = queueName(t, 'late-parked');
  const mq = rabbit(t);
  mq.declare((ch) => ch.assertQueue(full, { durable: true, arguments: { 'x-max-length': 1, 'x-overflow': 'reject-publish' } }));
  await mq.start();
  await mq.ready(10_000);
  await mq.sendToQueue(full, 'fills it');
  await assert.rejects(mq.publishWithin(300, '', full, 'too late'), /no broker confirm within 300 ms/);
  await admin((ch) => ch.purgeQueue(full));
  await sleep(1500);
  assert.equal(await depth(full), 0);
  await mq.close(500);
});

test('the confirm of a publish that timed out does not unbalance the held counters', async (t) => {
  const queue = queueName(t, 'late-confirm');
  const mq = rabbit(t);
  mq.declare(quorum(queue));
  await mq.start();
  await mq.ready(10_000);
  await patchPublish(t, (key, cb) => (key === queue ? (...args) => setTimeout(() => cb(...args), 500) : cb));
  await assert.rejects(mq.publishWithin(100, '', queue, 'x'), /no broker confirm within 100 ms/);
  await sleep(800);
  assert.deepEqual([mq.stats().held, mq.stats().heldBytes], [0, 0]);
  await mq.close(500);
});

test('publishes held two at a time never reach a cap they fit under', async (t) => {
  const queue = queueName(t, 'cap-leak');
  const mq = rabbit(t, { maxHeldBytes: 20_000 });
  mq.declare(quorum(queue));
  await mq.start();
  await mq.ready(10_000);
  // Each counts 1000 + 1536 bytes: two fit, and 60 in total would not if the count never went down.
  for (let i = 0; i < 30; i++) {
    await Promise.all([mq.sendToQueue(queue, 'x'.repeat(1000)), mq.sendToQueue(queue, 'x'.repeat(1000))]);
  }
  assert.deepEqual([mq.stats().held, mq.stats().heldBytes], [0, 0]);
  await mq.close(2000);
  assert.equal(await depth(queue), 60);
});

test('publishing pauses when amqplib reports a full buffer and resumes on drain', { timeout: 60_000 }, async (t) => {
  const queue = queueName(t, 'backpressure');
  const mq = rabbit(t);
  mq.declare(quorum(queue));
  await mq.start();
  await mq.ready(10_000);
  // Every 10th publish reports a full buffer, and 'drain' follows 20 ms later.
  const events = [];
  const watched = new WeakSet();
  const proto = await admin((ch) => Object.getPrototypeOf(ch));
  const original = proto.publish;
  let calls = 0;
  proto.publish = function (...args) {
    if (!watched.has(this)) {
      watched.add(this);
      this.prependListener('drain', () => events.push('drain'));
    }
    original.apply(this, args);
    if (++calls % 10 !== 0) {
      events.push('publish');
      return true;
    }
    events.push('full');
    setTimeout(() => this.emit('drain'), 20);
    return false;
  };
  t.after(() => (proto.publish = original));
  await Promise.all(Array.from({ length: 100 }, (_, i) => mq.sendToQueue(queue, { i })));
  assert.ok(events.includes('full'));
  let full = false;
  for (const event of events) {
    if (event === 'drain') full = false;
    else assert.equal(full, false, 'a publish went out between a full buffer and its drain');
    if (event === 'full') full = true;
  }
  await mq.close(2000);
  assert.equal(await depth(queue), 100);
});

test('close() stays within its grace when the broker stops answering', { timeout: 60_000 }, async (t) => {
  const queue = queueName(t, 'paused');
  const mq = rabbit(t);
  mq.declare(quorum(queue));
  mq.consume(queue, () => new Promise(() => {}), { prefetch: 1 });
  await mq.start();
  await mq.ready(10_000);
  await until(() => mq.stats().subscribed === 1, 10_000);
  await mq.sendToQueue(queue, 'stuck in the handler');
  await until(() => mq.stats().inFlight === 1, 10_000);
  execSync(`docker pause ${CONTAINER}`, { stdio: 'ignore' });
  let outcome;
  const begun = Date.now();
  try {
    mq.sendToQueue(queue, 'never confirmed').catch(() => {});
    outcome = await Promise.race([mq.close(1000).then(() => 'closed'), sleep(8000).then(() => 'hung')]);
  } finally {
    execSync(`docker unpause ${CONTAINER}`, { stdio: 'ignore' });
  }
  assert.equal(outcome, 'closed');
  assert.ok(Date.now() - begun < 3000, `close(1000) took ${Date.now() - begun} ms`);
});

test('close() with no grace period, or one that is not a number of ms, waits for a running handler', async (t) => {
  for (const grace of [undefined, NaN, -1, '500']) {
    const queue = queueName(t, 'close-default');
    const mq = rabbit(t);
    let started = false;
    let finished = false;
    mq.declare(quorum(queue));
    mq.consume(queue, async () => {
      started = true;
      await sleep(400);
      finished = true;
    }, { prefetch: 1 });
    await mq.start();
    await mq.sendToQueue(queue, 'x');
    await until(() => started);
    await mq.close(grace);
    assert.equal(finished, true, `close(${String(grace)}) did not wait for the handler`);
  }
});

test('the heartbeat option applies unless the URL or the URL object sets its own', async (t) => {
  const heartbeatOf = async (options) => {
    const name = `mmc-rabbitmq-heartbeat-${process.pid}-${++counter}`;
    const mq = rabbit(t, { name, ...options });
    await mq.start();
    await mq.ready(10_000);
    const found = heartbeats(`${name} (publish)`);
    await mq.close(500);
    assert.equal(found.length, 1);
    return found[0];
  };
  assert.equal(await heartbeatOf({}), 60);
  assert.equal(await heartbeatOf({ heartbeat: 20 }), 20);
  assert.equal(await heartbeatOf({ url: `${URL}?heartbeat=7`, heartbeat: 20 }), 7);
  const parsed = new globalThis.URL(URL);
  const object = { protocol: 'amqp', hostname: parsed.hostname, port: Number(parsed.port), username: 'guest', password: 'guest' };
  assert.equal(await heartbeatOf({ url: { ...object, heartbeat: 9 }, heartbeat: 20 }), 9);
});

test('a consumer of a missing queue retries with a growing delay, not in a loop', async (t) => {
  const absent = queueName(t, 'absent');
  const warnings = [];
  const mq = rabbit(t, { log: { ...quiet, warn: (line) => warnings.push(line) } });
  mq.declare(quorum(queueName(t, 'present')));
  mq.consume(absent, async () => {}, { prefetch: 1 });
  await mq.start();
  await mq.ready(10_000);
  const attempts = () => warnings.filter((line) => line.includes(`subscribing to ${absent} failed`)).length;
  const before = attempts();
  await sleep(1500);
  // At about 0, 200, 600 and 1400 ms.
  assert.ok(attempts() - before >= 2 && attempts() - before <= 5, `${attempts() - before} subscribe attempts in 1.5 s`);
  await mq.close(500);
});

test('a failing declaration is retried with a growing delay, and the others are not run again', async (t) => {
  const mq = rabbit(t);
  let good = 0;
  let failing = 0;
  mq.declare(async () => void good++);
  mq.declare(async () => {
    failing++;
    throw new Error('always fails');
  });
  await mq.start();
  await sleep(1500);
  // At about 0, 200, 600 and 1400 ms.
  assert.ok(failing >= 2 && failing <= 5, `${failing} runs of the failing declaration in 1.5 s`);
  assert.equal(good, 1);
  await mq.close(500);
});

test('declarations whose connection fails to open run on the retry', async (t) => {
  const original = amqplib.connect;
  let declareConnects = 0;
  amqplib.connect = async function (url, options) {
    if (/\(declare\)$/.test(options?.clientProperties?.connection_name) && declareConnects++ === 0) {
      throw new Error('refused by the test');
    }
    return original.call(this, url, options);
  };
  t.after(() => (amqplib.connect = original));
  const queue = queueName(t, 'declare-connect-fails');
  const mq = rabbit(t);
  mq.declare(quorum(queue));
  await mq.start();
  await until(() => exists(queue), 5000);
  await mq.close(500);
});

