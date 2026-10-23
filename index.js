'use strict';

const { randomUUID } = require('node:crypto');
const amqp = require('amqplib');

class RetryLater extends Error {
  constructor(message, options) {
    super(message, options);
    this.name = 'RetryLater';
  }
}

const ID_HEADER = 'x-mmc-rabbitmq-id';
const ON_ERROR = {
  reject: 'rejected (dead-lettered if the queue has a dead-letter exchange, otherwise dropped)',
  requeue: 'requeued',
};

const describe = (err) => (err instanceof Error ? err.message || err.code || err.name : String(err));
const quietClose = (ch) => Promise.resolve().then(() => ch.close()).catch(() => {});

function within(promise, ms) {
  let timer;
  const deadline = new Promise((resolve) => (timer = setTimeout(resolve, ms)));
  return Promise.race([promise.catch(() => {}), deadline]).finally(() => clearTimeout(timer));
}

function withHeartbeat(url, heartbeat) {
  if (typeof url !== 'string') return { heartbeat, ...url };
  const parsed = new URL(url);
  if (!parsed.searchParams.has('heartbeat')) parsed.searchParams.set('heartbeat', String(heartbeat));
  return parsed.toString();
}

function toContent(body) {
  if (Buffer.isBuffer(body)) return { content: body };
  if (typeof body === 'string') return { content: Buffer.from(body) };
  const json = JSON.stringify(body);
  if (json === undefined) throw new TypeError('mmc-rabbitmq: a message body must be a Buffer, a string or a JSON value');
  return { content: Buffer.from(json), contentType: 'application/json' };
}

function rejected(err) {
  const promise = Promise.reject(err);
  promise.catch(() => {});
  return { promise };
}

function createRabbit({
  url,
  name,
  heartbeat = 60,
  maxHeldBytes = 512 * 2 ** 20,
  retryDelay = { min: 1000, max: 30_000 },
  log = console,
}) {
  const declarations = [];
  const consumers = [];
  const pending = [];
  const inFlight = new Set();
  const timers = new Set();
  const wakers = new Set();
  const readyWaiters = new Set();
  const idleWaiters = [];
  const closedChannels = new WeakSet();
  let pub = null;
  let sub = null;
  let pubCh = null;
  let pubOpening = false;
  let pubDelay = retryDelay.min;
  let declareDelay = retryDelay.min;
  let declared = false;
  let declaring = false;
  let redeclare = false;
  let draining = false;
  let refusing = false;
  let heldBytes = 0;
  let suspects = 0;
  let declareConn = null;
  let starting = null;
  let closing = null;
  let stopped = false;

  function later(fn, ms) {
    if (stopped) return;
    const timer = setTimeout(() => {
      timers.delete(timer);
      fn();
    }, ms);
    timers.add(timer);
  }

  // A RetryLater wait that close() can cut short.
  function sleep(ms) {
    return new Promise((resolve) => {
      const wake = () => {
        clearTimeout(timer);
        wakers.delete(wake);
        resolve();
      };
      const timer = setTimeout(wake, ms);
      wakers.add(wake);
    });
  }

  function isReady() {
    return pubCh !== null && declared && !pub.blocked;
  }

  function checkReady() {
    if (!isReady()) return;
    readyWaiters.forEach((resolve) => resolve());
    readyWaiters.clear();
  }

  function connect(role, onConnect) {
    const state = { conn: null, model: null, blocked: false };
    state.opened = amqp
      .connect(withHeartbeat(url, heartbeat), {
        clientProperties: { connection_name: `${name} (${role})` },
        recovery: { initialDelay: retryDelay.min, maxDelay: retryDelay.max, maxRetries: Infinity, waitForConnect: false },
      })
      .then((conn) => {
        state.conn = conn;
        conn.on('connect', (model) => {
          state.model = model;
          log.info(`mmc-rabbitmq: ${role} connection up (${name})`);
          try {
            onConnect(model);
          } catch (err) {
            log.error(`mmc-rabbitmq: ${role} setup failed: ${describe(err)}`);
          }
        });
        conn.on('disconnect', (err) => {
          state.model = null;
          state.blocked = false;
          log.warn(`mmc-rabbitmq: ${role} connection lost (${name}): ${describe(err)}`);
        });
        conn.on('connect-failed', (err) => log.warn(`mmc-rabbitmq: ${role} connect failed (${name}): ${describe(err)}`));
        conn.on('blocked', (reason) => {
          state.blocked = true;
          log.warn(`mmc-rabbitmq: broker blocked the ${role} connection: ${reason}`);
        });
        conn.on('unblocked', () => {
          state.blocked = false;
          log.info(`mmc-rabbitmq: broker unblocked the ${role} connection`);
          flush();
          checkReady();
        });
        conn.on('error', () => {});
      });
    return state;
  }

  // Publishing

  function enqueue(exchange, routingKey, body, options) {
    let content, contentType;
    try {
      ({ content, contentType } = toContent(body));
    } catch (err) {
      log.error(`mmc-rabbitmq: publish to ${exchange || routingKey} failed: ${describe(err)}`);
      return rejected(err);
    }
    if (heldBytes + content.length > maxHeldBytes) {
      if (!refusing) log.error(`mmc-rabbitmq: refusing publishes, ${heldBytes} bytes already held (limit ${maxHeldBytes})`);
      refusing = true;
      return rejected(new Error('mmc-rabbitmq: hold limit reached'));
    }
    refusing = false;
    const id = randomUUID();
    const headers = { ...options?.headers, [ID_HEADER]: id };
    const job = { id, exchange, routingKey, content, ch: null, done: false, suspect: false };
    job.options = { persistent: true, mandatory: true, contentType, messageId: id, ...options, headers };
    job.promise = new Promise((resolve, reject) => Object.assign(job, { resolve, reject }));
    job.promise.catch(() => {});
    heldBytes += content.length;
    pending.push(job);
    flush();
    return job;
  }

  function finish(job, err, quiet = false) {
    if (job.done) return;
    job.done = true;
    pending.splice(pending.indexOf(job), 1);
    heldBytes -= job.content.length;
    if (job.suspect) suspects--;
    if (err && !quiet) log.error(`mmc-rabbitmq: publish to ${job.exchange || job.routingKey} failed: ${describe(err)}`);
    if (err) job.reject(err);
    else job.resolve();
    if (!pending.length) idleWaiters.splice(0).forEach((resolve) => resolve());
  }

  function flush() {
    const ch = pubCh;
    if (!ch || draining || !declared) return;
    // After the broker closed the channel over a publish, re-send the suspects one at a time to find the bad one.
    const suspect = suspects > 0 && pending.find((job) => job.suspect);
    if (suspect) {
      if (!suspect.ch && !pending.some((job) => job.ch === ch)) send(ch, suspect);
      return;
    }
    for (const job of pending) {
      if (!job.ch && !send(ch, job)) return;
    }
  }

  function send(ch, job) {
    job.ch = ch;
    let ok;
    try {
      ok = ch.publish(job.exchange, job.routingKey, job.content, job.options, (err) => {
        if (job.done || job.ch !== ch) return;
        if (err) {
          // A nack leaves the channel open: hold the message and retry. A closed channel is handled on 'close'.
          queueMicrotask(() => {
            if (job.done || job.ch !== ch) return;
            job.ch = null;
            later(flush, retryDelay.min);
          });
          return;
        }
        if (job.returned) return finish(job, new Error(`mmc-rabbitmq: unroutable, no queue bound for ${job.routingKey}`));
        pubDelay = retryDelay.min;
        finish(job);
        if (job.suspect) flush();
      });
    } catch (err) {
      if (pubCh !== ch || closedChannels.has(ch)) {
        job.ch = null;
        return false;
      }
      // The channel is fine, so the message itself is invalid. Finish it outside the loop that is sending.
      queueMicrotask(() => finish(job, err));
      return true;
    }
    if (!ok) draining = true;
    return ok;
  }

  function openPublisher(model) {
    if (closing || pubOpening || pubCh || pub.model !== model) return;
    pubOpening = true;
    model.createConfirmChannel().then(
      (ch) => {
        pubOpening = false;
        if (closing || pub.model !== model) return quietClose(ch);
        let failure = null;
        ch.on('error', (err) => {
          failure = err;
          log.warn(`mmc-rabbitmq: broker closed the publish channel: ${describe(err)}`);
        });
        ch.on('close', () => onPublisherClosed(ch, model, failure));
        ch.on('drain', () => {
          draining = false;
          flush();
        });
        ch.on('return', (msg) => {
          const job = pending.find((j) => j.id === msg.properties.headers?.[ID_HEADER]);
          if (job) job.returned = true;
        });
        pubCh = ch;
        draining = false;
        flush();
        checkReady();
      },
      (err) => {
        pubOpening = false;
        log.warn(`mmc-rabbitmq: opening the publish channel failed: ${describe(err)}`);
        reopenPublisher(model);
      },
    );
  }

  function onPublisherClosed(ch, model, failure) {
    closedChannels.add(ch);
    if (pubCh === ch) pubCh = null;
    const sent = pending.filter((job) => job.ch === ch);
    sent.forEach((job) => (job.ch = null));
    const missing = failure?.code === 404 && /no exchange '([^']*)'/.exec(failure.message);
    if (missing) sent.filter((job) => job.exchange === missing[1]).forEach((job) => finish(job, failure));
    else if (failure && sent.length === 1) finish(sent[0], failure);
    else if (failure) {
      for (const job of sent) {
        if (!job.suspect) suspects++;
        job.suspect = true;
      }
    }
    reopenPublisher(model);
  }

  function reopenPublisher(model) {
    const delay = pubDelay;
    pubDelay = Math.min(pubDelay * 2, retryDelay.max);
    later(() => openPublisher(model), delay);
  }

  // Topology. Declarations get their own short-lived connection: a declare the broker answers by closing the
  // connection must not take publishing or consuming down with it.

  function runDeclarations() {
    if (closing) return;
    if (declaring) return void (redeclare = true);
    if (!declarations.length) return markDeclared();
    declaring = true;
    redeclare = false;
    let conn = null;
    amqp
      .connect(withHeartbeat(url, heartbeat), { timeout: 10_000, clientProperties: { connection_name: `${name} (declare)` } })
      .then(async (opened) => {
        conn = declareConn = opened;
        conn.on('error', () => {});
        const ch = await conn.createChannel();
        ch.on('error', () => {});
        for (const fn of [...declarations]) await fn(ch);
      })
      .then(
        () => {
          declareDelay = retryDelay.min;
          declaring = false;
          markDeclared();
          if (redeclare) runDeclarations();
        },
        (err) => {
          log.error(`mmc-rabbitmq: declaring topology failed: ${describe(err)}`);
          declaring = false;
          markDeclared();
          const delay = declareDelay;
          declareDelay = Math.min(declareDelay * 2, retryDelay.max);
          later(runDeclarations, delay);
        },
      )
      .finally(() => {
        if (declareConn === conn) declareConn = null;
        conn?.close().catch(() => {});
      });
  }

  function markDeclared() {
    if (declared) return;
    declared = true;
    flush();
    checkReady();
    consumers.forEach(subscribe);
  }

  // Consuming

  function subscribe(consumer) {
    const model = sub?.model;
    if (closing || !model || !declared || consumer.ch || consumer.opening) return;
    consumer.opening = true;
    model
      .createChannel()
      .finally(() => (consumer.opening = false))
      .then(async (ch) => {
        if (closing || sub.model !== model) return quietClose(ch);
        const live = new AbortController();
        consumer.ch = ch;
        consumer.signal = live.signal;
        ch.on('error', (err) => log.warn(`mmc-rabbitmq: broker closed the ${consumer.queue} channel: ${describe(err)}`));
        ch.on('close', () => {
          closedChannels.add(ch);
          live.abort();
          if (consumer.ch !== ch) return;
          consumer.ch = null;
          consumer.tag = null;
          if (!closing && sub.model === model) retry(consumer);
        });
        await ch.prefetch(consumer.prefetch);
        await startConsuming(consumer, ch);
      })
      .catch((err) => {
        log.warn(`mmc-rabbitmq: subscribing to ${consumer.queue} failed: ${describe(err)}`);
        if (!closing && sub.model === model) retry(consumer);
      });
  }

  async function startConsuming(consumer, ch) {
    const signal = consumer.signal;
    const { consumerTag } = await ch.consume(consumer.queue, (msg) => onMessage(consumer, ch, signal, msg));
    consumer.tag = consumerTag;
    consumer.delay = retryDelay.min;
  }

  function retry(consumer) {
    if (consumer.timer) return;
    const delay = consumer.delay;
    consumer.delay = Math.min(delay * 2, retryDelay.max);
    consumer.timer = true;
    later(() => {
      consumer.timer = false;
      if (closing || consumer.tag) return;
      if (!consumer.ch) return subscribe(consumer);
      startConsuming(consumer, consumer.ch).catch((err) => {
        log.warn(`mmc-rabbitmq: resubscribing to ${consumer.queue} failed: ${describe(err)}`);
        retry(consumer);
      });
    }, delay);
  }

  function onMessage(consumer, ch, signal, msg) {
    if (msg === null) {
      log.warn(`mmc-rabbitmq: broker cancelled the subscription to ${consumer.queue}, resubscribing`);
      consumer.tag = null;
      return retry(consumer);
    }
    const work = handle(consumer, ch, signal, msg);
    inFlight.add(work);
    work.finally(() => inFlight.delete(work)).catch(() => {});
  }

  async function handle({ queue, handler, onError, retryLimit }, ch, signal, msg) {
    const gone = () => closing || closedChannels.has(ch);
    const firstTry = Date.now();
    let delay = retryDelay.min;
    try {
      for (let attempt = 1; ; attempt++) {
        try {
          await handler(msg, { signal });
          return settle(ch, msg, 'ack');
        } catch (err) {
          const exhausted = attempt >= retryLimit.attempts || Date.now() - firstTry + delay > retryLimit.ms;
          if (!(err instanceof RetryLater) || exhausted) {
            const retried = err instanceof RetryLater ? ` after ${attempt} attempts` : '';
            log.error(`mmc-rabbitmq: ${queue} handler failed${retried}, ${ON_ERROR[onError]}: ${describe(err)}`);
            return settle(ch, msg, onError);
          }
          if (gone()) return settle(ch, msg, 'requeue');
          log.warn(`mmc-rabbitmq: ${queue} retrying in ${delay} ms: ${describe(err)}`);
          await sleep(delay);
          if (gone()) return settle(ch, msg, 'requeue');
          delay = Math.min(delay * 2, retryDelay.max);
        }
      }
    } catch (err) {
      log.error(`mmc-rabbitmq: ${queue} message requeued after an internal error: ${describe(err)}`);
      settle(ch, msg, 'requeue');
    }
  }

  // Ack/nack only on the delivering channel; once it is gone the broker redelivers the message.
  function settle(ch, msg, outcome) {
    if (closedChannels.has(ch)) return;
    try {
      if (outcome === 'ack') ch.ack(msg);
      else ch.nack(msg, false, outcome === 'requeue');
    } catch (err) {
      log.warn(`mmc-rabbitmq: ${outcome} skipped, channel closed: ${describe(err)}`);
    }
  }

  function onConsumeConnect() {
    consumers.forEach(subscribe);
  }

  // Lifecycle

  function start() {
    starting ??= Promise.resolve().then(() => {
      pub = connect('publish', (model) => {
        openPublisher(model);
        runDeclarations();
      });
      if (consumers.length) sub = connect('consume', onConsumeConnect);
      return Promise.all([pub.opened, sub?.opened]).then(() => {});
    });
    return starting;
  }

  function close(graceMs = 25_000) {
    closing ??= shutdown(graceMs);
    return closing;
  }

  async function shutdown(graceMs) {
    // Keep the end of the grace period for closing channels and connections, so close() stays within graceMs.
    const end = Date.now() + graceMs;
    const deadline = end - Math.min(2000, graceMs / 4);
    const left = () => Math.max(0, deadline - Date.now());
    const leftToEnd = () => Math.max(0, end - Date.now());
    wakers.forEach((wake) => wake());
    const cancels = consumers
      .filter((c) => c.ch && c.tag)
      .map((c) => Promise.resolve().then(() => c.ch.cancel(c.tag)));
    await within(Promise.allSettled(cancels), left());
    await within(Promise.allSettled([...inFlight]), left());
    if (pending.length) await within(new Promise((resolve) => idleWaiters.push(resolve)), left());
    if (pending.length) log.error(`mmc-rabbitmq: ${pending.length} publishes unconfirmed at shutdown`);
    [...pending].forEach((job) => finish(job, new Error('mmc-rabbitmq: closed before the broker confirmed'), true));
    stopped = true;
    timers.forEach(clearTimeout);
    timers.clear();
    declareConn?.close().catch(() => {});
    // Close channels first: a connection close can overtake acks still queued on a channel.
    const channels = [pubCh, ...consumers.map((c) => c.ch)].filter(Boolean);
    await within(Promise.allSettled(channels.map(quietClose)), leftToEnd());
    await within(Promise.allSettled([pub?.opened, sub?.opened]), leftToEnd());
    const conns = [pub?.conn, sub?.conn].filter(Boolean);
    await within(Promise.allSettled(conns.map((conn) => conn.close())), leftToEnd());
  }

  return {
    start,
    close,
    isReady,
    declare(fn) {
      declarations.push(fn);
      if (pub?.model) runDeclarations();
    },
    consume(queue, handler, { prefetch, onError = 'reject', retryLimit = {} } = {}) {
      if (!Number.isInteger(prefetch) || prefetch < 0) {
        throw new TypeError(`mmc-rabbitmq: consume('${queue}') needs an explicit prefetch (0 or more)`);
      }
      if (!ON_ERROR[onError]) throw new TypeError(`mmc-rabbitmq: onError must be 'reject' or 'requeue'`);
      const { attempts = Infinity, ms = Infinity } = retryLimit;
      const consumer = { queue, handler, prefetch, onError, retryLimit: { attempts, ms }, delay: retryDelay.min };
      consumers.push(consumer);
      if (starting) {
        starting.then(() => {
          if (closing) return;
          sub ??= connect('consume', onConsumeConnect);
          subscribe(consumer);
        });
      }
    },
    publish: (exchange, routingKey, body, options) => enqueue(exchange, routingKey, body, options).promise,
    sendToQueue: (queue, body, options) => enqueue('', queue, body, options).promise,
    publishWithin(ms, exchange, routingKey, body, options) {
      const job = enqueue(exchange, routingKey, body, options);
      if (!job.id) return job.promise;
      const timer = setTimeout(() => finish(job, new Error(`mmc-rabbitmq: no broker confirm within ${ms} ms`), true), ms);
      return job.promise.finally(() => clearTimeout(timer));
    },
    ready(ms) {
      if (isReady()) return Promise.resolve();
      return new Promise((resolve, reject) => {
        const done = () => {
          clearTimeout(timer);
          resolve();
        };
        const timer = setTimeout(() => {
          readyWaiters.delete(done);
          reject(new Error(`mmc-rabbitmq: not ready within ${ms} ms`));
        }, ms);
        readyWaiters.add(done);
      });
    },
    stats: () => ({
      held: pending.length,
      heldBytes,
      inFlight: inFlight.size,
      blocked: Boolean(pub?.blocked),
      consumers: consumers.length,
      subscribed: consumers.filter((c) => c.tag).length,
    }),
  };
}

module.exports = { createRabbit, RetryLater };
