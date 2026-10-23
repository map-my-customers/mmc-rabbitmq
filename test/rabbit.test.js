'use strict';

// Runs against a real broker: RABBIT_URL, and RABBIT_CONTAINER for the restart test (docker stop/start).
const { test } = require('node:test');
const assert = require('node:assert/strict');
const { execSync } = require('node:child_process');
const amqplib = require('amqplib');
const { createRabbit, RetryLater } = require('..');

const URL = process.env.RABBIT_URL || 'amqp://guest:guest@localhost:5672';
const CONTAINER = process.env.RABBIT_CONTAINER || 'rabbit';
const NO_BROKER = 'amqp://guest:guest@localhost:5999';
const quiet = { info() {}, warn() {}, error() {} };
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
let counter = 0;
const queueName = (label) => `mmc-rabbitmq-test.${label}.${process.pid}.${++counter}`;

async function until(check, ms = 30_000) {
  const end = Date.now() + ms;
  while (!(await check())) {
    if (Date.now() > end) throw new Error('timed out waiting');
    await sleep(100);
  }
}

async function depth(queue) {
  const conn = await amqplib.connect(URL);
  const ch = await conn.createChannel();
  const { messageCount } = await ch.checkQueue(queue);
  await conn.close();
  return messageCount;
}

const quorum = (queue, args = {}) => (ch) =>
  ch.assertQueue(queue, { durable: true, arguments: { 'x-queue-type': 'quorum', ...args } });

const rabbit = (options = {}) =>
  createRabbit({ url: URL, name: 'mmc-rabbitmq-test', log: quiet, retryDelay: { min: 200, max: 2000 }, ...options });

test('delivers confirmed publishes to a consumer', async () => {
  const queue = queueName('roundtrip');
  const mq = rabbit();
  const got = [];
  mq.declare(quorum(queue));
  mq.consume(queue, async (msg) => got.push(JSON.parse(msg.content).i), { prefetch: 10 });
  await mq.start();
  await Promise.all(Array.from({ length: 50 }, (_, i) => mq.sendToQueue(queue, { i })));
  await until(() => got.length === 50);
  await mq.close(2000);
  assert.deepEqual([...got].sort((a, b) => a - b), [...Array(50).keys()]);
});

test('holds publishes through a broker restart and keeps consuming', { timeout: 180_000 }, async () => {
  const queue = queueName('restart');
  const mq = rabbit();
  const seen = new Set();
  mq.declare(quorum(queue));
  mq.consume(queue, async (msg) => seen.add(JSON.parse(msg.content).i), { prefetch: 20 });
  await mq.start();
  await until(() => mq.isReady());
  const sends = [];
  let i = 0;
  const timer = setInterval(() => sends.push(mq.sendToQueue(queue, { i: i++ })), 20);
  await sleep(1000);
  execSync(`docker stop ${CONTAINER}`, { stdio: 'ignore' });
  await sleep(5000);
  execSync(`docker start ${CONTAINER}`, { stdio: 'ignore' });
  await until(() => mq.isReady(), 90_000);
  await sleep(1000);
  clearInterval(timer);
  const results = await Promise.allSettled(sends);
  assert.equal(results.filter((r) => r.status === 'rejected').length, 0);
  await until(() => seen.size === i, 60_000);
  await mq.close(2000);
});

test('RetryLater retries in place and acks once', async () => {
  const queue = queueName('retry');
  const mq = rabbit();
  let calls = 0;
  mq.declare(quorum(queue));
  mq.consume(queue, async () => {
    if (++calls < 3) throw new RetryLater('dependency down');
  });
  await mq.start();
  await mq.sendToQueue(queue, { x: 1 });
  await until(() => calls === 3);
  await mq.close(2000);
  assert.equal(calls, 3);
  assert.equal(await depth(queue), 0);
});

test('other handler errors dead-letter the message', async () => {
  const queue = queueName('poison');
  const dlq = `${queue}.dl`;
  const mq = rabbit();
  mq.declare(quorum(dlq));
  mq.declare(quorum(queue, { 'x-dead-letter-exchange': '', 'x-dead-letter-routing-key': dlq }));
  mq.consume(queue, async () => {
    throw new Error('bad payload');
  });
  await mq.start();
  await mq.sendToQueue(queue, { x: 1 });
  await until(async () => (await depth(dlq)) === 1);
  await mq.close(2000);
});

test('close waits for in-flight handlers before acking', async () => {
  const queue = queueName('drain');
  const mq = rabbit();
  let started = false;
  let finished = false;
  mq.declare(quorum(queue));
  mq.consume(queue, async () => {
    started = true;
    await sleep(500);
    finished = true;
  });
  await mq.start();
  await mq.sendToQueue(queue, { x: 1 });
  await until(() => started);
  await mq.close(5000);
  assert.equal(finished, true);
  assert.equal(await depth(queue), 0);
});

test('rejects a publish to a missing exchange and keeps publishing', async () => {
  const queue = queueName('missing-exchange');
  const mq = rabbit();
  mq.declare(quorum(queue));
  await mq.start();
  await until(() => mq.isReady());
  await assert.rejects(mq.publish(`no-such-exchange-${process.pid}`, 'key', 'x'), /no exchange/);
  await until(() => mq.isReady(), 10_000);
  await mq.sendToQueue(queue, 'still works');
  await mq.close(2000);
});

test('rejects an unroutable publish instead of dropping it silently', async () => {
  const mq = rabbit();
  await mq.start();
  await assert.rejects(mq.sendToQueue(`no-such-queue-${process.pid}`, 'x'), /unroutable/);
  await mq.close(2000);
});

test('without a broker: refuses past the hold limit and shuts down promptly', async () => {
  const mq = createRabbit({ url: NO_BROKER, name: 'no-broker', log: quiet, maxHeldBytes: 5000 });
  await mq.start();
  let refused = 0;
  let closedUnconfirmed = 0;
  for (let i = 0; i < 10; i++) {
    mq.sendToQueue('q', 'y'.repeat(1000)).catch((err) => {
      if (/hold limit/.test(err.message)) refused++;
      if (/closed before the broker confirmed/.test(err.message)) closedUnconfirmed++;
    });
  }
  await sleep(50);
  assert.equal(refused, 5);
  assert.equal(mq.stats().held, 5);
  const started = Date.now();
  await mq.close(300);
  await sleep(10);
  assert.ok(Date.now() - started < 4000);
  assert.equal(closedUnconfirmed, 5);
});

test('without a broker: publishWithin rejects at its deadline and holds nothing', async () => {
  const mq = createRabbit({ url: NO_BROKER, name: 'no-broker', log: quiet });
  await mq.start();
  await assert.rejects(mq.publishWithin(300, '', 'q', 'x'), /no broker confirm within 300 ms/);
  assert.equal(mq.stats().held, 0);
  await mq.close(300);
});
