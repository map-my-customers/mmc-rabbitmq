'use strict';

const amqp = require('amqplib');

class RetryLater extends Error {
  constructor(message, options) {
    super(message, options);
    this.name = 'RetryLater';
  }
}

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
const within = (promise, ms) =>
  Promise.race([promise.catch(() => {}), new Promise((resolve) => setTimeout(resolve, ms))]);

function withHeartbeat(url, heartbeat) {
  if (typeof url !== 'string') return { heartbeat, ...url };
  const parsed = new URL(url);
  if (!parsed.searchParams.has('heartbeat')) parsed.searchParams.set('heartbeat', String(heartbeat));
  return parsed.toString();
}

function createRabbit({
  url,
  name,
  heartbeat = 15,
  maxHeldBytes = 512 * 2 ** 20,
  retryDelay = { min: 1000, max: 30_000 },
  log = console,
}) {
  const declarations = [];
  const consumers = [];
  const pending = [];
  const inFlight = new Set();
  const closedChannels = new WeakSet();
  const idleWaiters = [];
  let conn = null;
  let current = null;
  let generation = 0;
  let pubCh = null;
  let subscriptions = [];
  let channels = [];
  let connected = false;
  let closing = false;
  let heldBytes = 0;
  let nextId = 0;
  let rebuildDelay = retryDelay.min;

  function enqueue(exchange, routingKey, body, options = {}) {
    const isBuffer = Buffer.isBuffer(body);
    const content = isBuffer ? body : Buffer.from(typeof body === 'string' ? body : JSON.stringify(body));
    if (heldBytes + content.length > maxHeldBytes) {
      log.error(`mmc-rabbitmq: publish to ${exchange || routingKey} refused, ${heldBytes} bytes already held`);
      return { promise: Promise.reject(new Error('mmc-rabbitmq: hold limit reached')) };
    }
    const id = String(++nextId);
    const contentType = isBuffer || typeof body === 'string' ? undefined : 'application/json';
    const headers = { ...options.headers, 'x-mmc-rabbitmq-id': id };
    const job = { id, exchange, routingKey, content, ch: null, done: false };
    job.options = { persistent: true, mandatory: true, contentType, ...options, headers };
    job.promise = new Promise((resolve, reject) => Object.assign(job, { resolve, reject }));
    heldBytes += content.length;
    pending.push(job);
    flush();
    return job;
  }

  function finish(job, err) {
    if (job.done) return;
    job.done = true;
    pending.splice(pending.indexOf(job), 1);
    heldBytes -= job.content.length;
    if (err) job.reject(err);
    else job.resolve();
    if (!pending.length) idleWaiters.splice(0).forEach((resolve) => resolve());
  }

  function flush() {
    const ch = pubCh;
    if (!ch) return;
    for (const job of pending) {
      if (job.ch) continue;
      job.ch = ch;
      let ok;
      try {
        ok = ch.publish(job.exchange, job.routingKey, job.content, job.options, (err) => {
          if (job.done || job.ch !== ch) return;
          if (job.returned) return finish(job, new Error(`mmc-rabbitmq: unroutable, no queue bound for ${job.routingKey}`));
          if (!err) return finish(job);
          job.ch = null;
          setTimeout(flush, retryDelay.min);
        });
      } catch {
        job.ch = null;
        return;
      }
      if (!ok) return ch.once('drain', flush);
    }
  }

  function watch(ch, model, gen) {
    channels.push(ch);
    ch.on('error', (err) => {
      log.warn(`mmc-rabbitmq: channel error: ${err.message}`);
      const missing = err.code === 404 && /no exchange '([^']*)'/.exec(err.message);
      if (missing) pending.filter((j) => j.ch === ch && j.exchange === missing[1]).forEach((j) => finish(j, err));
    });
    ch.on('close', () => {
      closedChannels.add(ch);
      if (pubCh === ch) pubCh = null;
      if (!closing && gen === generation && model === current) scheduleRebuild(model, gen);
    });
  }

  function scheduleRebuild(model, gen) {
    setTimeout(() => {
      if (closing || !connected || gen !== generation || model !== current) return;
      openChannels(model).then(
        () => (rebuildDelay = retryDelay.min),
        (err) => {
          log.warn(`mmc-rabbitmq: reopening channels failed: ${err.message}`);
          rebuildDelay = Math.min(rebuildDelay * 2, retryDelay.max);
          scheduleRebuild(model, generation);
        },
      );
    }, rebuildDelay);
  }

  async function openChannels(model) {
    const gen = ++generation;
    channels.splice(0).forEach((ch) => ch.close().catch(() => {}));
    subscriptions = [];
    pending.forEach((job) => (job.ch = null));
    const pub = await model.createConfirmChannel();
    watch(pub, model, gen);
    pub.on('return', (msg) => {
      const job = pending.find((j) => j.id === msg.properties.headers?.['x-mmc-rabbitmq-id']);
      if (job) job.returned = true;
    });
    for (const declare of declarations) await declare(pub);
    for (const consumer of consumers) {
      const ch = await model.createChannel();
      watch(ch, model, gen);
      await ch.prefetch(consumer.prefetch);
      const { consumerTag } = await ch.consume(consumer.queue, (msg) => onMessage(consumer, ch, msg, model, gen));
      subscriptions.push({ ch, consumerTag });
    }
    pubCh = pub;
    flush();
  }

  function onMessage(consumer, ch, msg, model, gen) {
    if (msg === null) return scheduleRebuild(model, gen);
    const work = handle(consumer, ch, msg);
    inFlight.add(work);
    work.finally(() => inFlight.delete(work)).catch(() => {});
  }

  async function handle(consumer, ch, msg) {
    let delay = retryDelay.min;
    for (;;) {
      try {
        await consumer.handler(msg);
        return settle(ch, msg, 'ack');
      } catch (err) {
        if (!(err instanceof RetryLater)) {
          log.error(`mmc-rabbitmq: ${consumer.queue} handler failed, dead-lettering: ${err.message}`);
          return settle(ch, msg, 'reject');
        }
        if (closing || closedChannels.has(ch)) return settle(ch, msg, 'requeue');
        log.warn(`mmc-rabbitmq: ${consumer.queue} retrying in ${delay} ms: ${err.message}`);
        await sleep(delay);
        delay = Math.min(delay * 2, retryDelay.max);
      }
    }
  }

  // Ack/nack only on the delivering channel; once it is gone the broker redelivers the message.
  function settle(ch, msg, outcome) {
    if (closedChannels.has(ch)) return;
    try {
      if (outcome === 'ack') ch.ack(msg);
      else ch.nack(msg, false, outcome === 'requeue');
    } catch (err) {
      log.warn(`mmc-rabbitmq: ${outcome} skipped, channel closed: ${err.message}`);
    }
  }

  async function start() {
    conn = await amqp.connect(withHeartbeat(url, heartbeat), {
      clientProperties: { connection_name: name },
      recovery: {
        initialDelay: 500,
        maxDelay: retryDelay.max,
        maxRetries: Infinity,
        waitForConnect: false,
        setup: async (model) => {
          current = model;
          await openChannels(model);
        },
      },
    });
    conn.on('connect', () => {
      connected = true;
      log.info(`mmc-rabbitmq: connected (${name})`);
    });
    conn.on('disconnect', (err) => {
      connected = false;
      log.warn(`mmc-rabbitmq: disconnected (${name}): ${err.message}`);
    });
    conn.on('connect-failed', (err) => log.warn(`mmc-rabbitmq: connect failed (${name}): ${err.message}`));
    conn.on('error', () => {});
    return conn;
  }

  async function close(graceMs = 25_000) {
    if (closing) return;
    closing = true;
    const deadline = Date.now() + graceMs;
    const left = () => Math.max(0, deadline - Date.now());
    const cancels = subscriptions.map(({ ch, consumerTag }) => Promise.resolve().then(() => ch.cancel(consumerTag)));
    await within(Promise.allSettled(cancels), left());
    await within(Promise.allSettled([...inFlight]), left());
    if (pending.length) await within(new Promise((resolve) => idleWaiters.push(resolve)), left());
    if (pending.length) log.error(`mmc-rabbitmq: ${pending.length} publishes unconfirmed at shutdown`);
    [...pending].forEach((job) => finish(job, new Error('mmc-rabbitmq: closed before the broker confirmed')));
    // Close channels first: a connection close can overtake acks still queued on a channel.
    await within(Promise.allSettled(channels.map((ch) => Promise.resolve().then(() => ch.close()))), 2000);
    if (conn) await within(conn.close(), 2000);
  }

  return {
    start,
    close,
    declare: (fn) => declarations.push(fn),
    consume: (queue, handler, { prefetch = 1 } = {}) => consumers.push({ queue, handler, prefetch }),
    publish: (exchange, routingKey, body, options) => enqueue(exchange, routingKey, body, options).promise,
    sendToQueue: (queue, body, options) => enqueue('', queue, body, options).promise,
    publishWithin(ms, exchange, routingKey, body, options) {
      const job = enqueue(exchange, routingKey, body, options);
      if (!job.id) return job.promise;
      const timer = setTimeout(() => finish(job, new Error(`mmc-rabbitmq: no broker confirm within ${ms} ms`)), ms);
      return job.promise.finally(() => clearTimeout(timer));
    },
    isReady: () => connected && pubCh !== null,
    stats: () => ({ held: pending.length, heldBytes, inFlight: inFlight.size }),
  };
}

module.exports = { createRabbit, RetryLater };
