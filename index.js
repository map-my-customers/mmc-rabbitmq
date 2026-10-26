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
// Unconfirmed publishes on the channel at once. amqplib's confirm bookkeeping costs O(n) per confirm, and the search
// for a publish the broker refused re-sends each of these alone.
const MAX_IN_FLIGHT = 256;
// Heap a held publish costs besides its body (job, options, headers, promise), measured on Node 24.
const JOB_OVERHEAD = 1536;
const MAX_TIMEOUT = 2 ** 31 - 1;
// How long one declaration, or opening the declare connection, may take before it counts as failed.
const DECLARE_TIMEOUT = 10_000;
const ON_ERROR = {
  reject: 'rejected (dead-lettered if the queue has a dead-letter exchange, otherwise dropped)',
  requeue: 'requeued',
};

const describe = (err) => (err instanceof Error ? err.message || err.code || err.name : String(err));
const quietClose = (ch) => Promise.resolve().then(() => ch.close()).catch(() => {});
const isTimeout = (ms) => typeof ms === 'number' && ms > 0 && ms <= MAX_TIMEOUT;
const isLimit = (value) => typeof value === 'number' && value > 0;

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
  maxHeldBytes = 64 * 2 ** 20,
  retryDelay: delays,
  log = console,
}) {
  const retryDelay = { min: 1000, max: 30_000, ...delays };
  // A NaN would pass every comparison it is in as false: no hold cap, or timers that fire at once.
  if (!(Number.isFinite(maxHeldBytes) && maxHeldBytes > 0)) {
    throw new TypeError(`mmc-rabbitmq: maxHeldBytes must be a positive number, got ${maxHeldBytes}`);
  }
  // amqplib parses the heartbeat as an integer, so 0.5 would turn heartbeats off.
  if (!(Number.isInteger(heartbeat) && heartbeat >= 0 && heartbeat <= 65535)) {
    throw new TypeError(`mmc-rabbitmq: heartbeat must be a whole number of seconds from 0 to 65535, got ${heartbeat}`);
  }
  const { min, max } = retryDelay;
  if (!(Number.isFinite(min) && Number.isFinite(max) && min > 0 && min <= max)) {
    throw new TypeError(`mmc-rabbitmq: retryDelay needs 0 < min <= max, got min=${min} max=${max}`);
  }
  const declarations = [];
  const consumers = [];
  // Every held publish by id, oldest first. `unsent` is the send order of those not on a channel or waiting out a
  // nack; `suspects` were on a channel the broker closed over one of them, and are re-sent one at a time.
  const pending = new Map();
  let unsent = new Set();
  // Where flush() stopped. A fresh iterator would walk every entry deleted since the Set last rehashed.
  let cursor = null;
  const suspects = new Set();
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
  // The publish connection whose declarations have finished. Publishes and readiness wait for it after a reconnect,
  // in case the topology did not survive the broker.
  let declaredOn = null;
  let declaring = false;
  let redeclare = false;
  let draining = false;
  let refusing = false;
  let timingOut = false;
  let heldBytes = 0;
  let heldCost = 0;
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
    return pubCh !== null && declared && declaredOn === pub.model && !pub.blocked;
  }

  function checkReady() {
    if (!isReady()) return;
    readyWaiters.forEach((waiter) => waiter.done());
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
    // Publishes during close() still go out with the drain; after it there is nothing left to send them. RetryLater,
    // so a handler still running past close() puts its message back on the queue.
    if (stopped) return rejected(new RetryLater('mmc-rabbitmq: closed'));
    let content, contentType;
    try {
      ({ content, contentType } = toContent(body));
    } catch (err) {
      log.error(`mmc-rabbitmq: publish to ${exchange || routingKey} failed: ${describe(err)}`);
      return rejected(err);
    }
    if (content.length > maxHeldBytes) {
      const err = new Error(`mmc-rabbitmq: a ${content.length}-byte message is larger than maxHeldBytes (${maxHeldBytes})`);
      log.error(`mmc-rabbitmq: publish to ${exchange || routingKey} failed: ${describe(err)}`);
      return rejected(err);
    }
    const cost = content.length + JOB_OVERHEAD;
    // The first publish is always held, so a body that fits the cap without its overhead is not refused forever.
    if (pending.size && heldCost + cost > maxHeldBytes) {
      if (!refusing) log.error(`mmc-rabbitmq: refusing publishes, ${pending.size} already held (limit ${maxHeldBytes} bytes)`);
      refusing = true;
      // A handler that awaits this publish retries its message in place instead of failing it.
      return rejected(new RetryLater('mmc-rabbitmq: hold limit reached'));
    }
    refusing = false;
    const id = randomUUID();
    const headers = { ...options?.headers, [ID_HEADER]: id };
    const job = { id, exchange, routingKey, content, cost, ch: null, done: false, nacks: 0 };
    job.options = { persistent: true, mandatory: true, contentType, messageId: id, ...options, headers };
    job.promise = new Promise((resolve, reject) => Object.assign(job, { resolve, reject }));
    job.promise.catch(() => {});
    heldBytes += content.length;
    heldCost += cost;
    pending.set(id, job);
    unsent.add(job);
    flush();
    return job;
  }

  function finish(job, err, quiet = false) {
    if (job.done) return;
    job.done = true;
    pending.delete(job.id);
    unsent.delete(job);
    suspects.delete(job);
    heldBytes -= job.content.length;
    heldCost -= job.cost;
    if (err && !quiet) log.error(`mmc-rabbitmq: publish to ${job.exchange || job.routingKey} failed: ${describe(err)}`);
    if (err) job.reject(err);
    else job.resolve();
    if (!pending.size) idleWaiters.splice(0).forEach((resolve) => resolve());
  }

  function flush() {
    const ch = pubCh;
    if (!ch || draining || declaredOn !== pub.model) return;
    if (suspects.size) {
      // Alone on the channel, so a close blames only this one; a timed-out publish still on it counts.
      const [suspect] = suspects;
      if (!ch.flying.size) send(ch, suspect);
      return;
    }
    const it = (cursor ??= unsent.values());
    while (ch.flying.size < MAX_IN_FLIGHT) {
      const { value: job, done } = it.next();
      if (done) {
        if (cursor === it) cursor = null;
        return;
      }
      if (!send(ch, job)) return;
    }
  }

  function send(ch, job) {
    let ok;
    try {
      ok = ch.publish(job.exchange, job.routingKey, job.content, job.options, (err) => onConfirm(ch, job, err));
    } catch (err) {
      // amqplib throws IllegalOperationError while it closes the channel or connection; 'close' follows.
      if (pubCh !== ch || closedChannels.has(ch) || err?.name === 'IllegalOperationError') return false;
      // The channel is fine, so the message itself is invalid. Finish it outside the loop that is sending.
      unsent.delete(job);
      queueMicrotask(() => {
        finish(job, err);
        flush();
      });
      return true;
    }
    job.ch = ch;
    ch.flying.add(job);
    unsent.delete(job);
    if (!ok) draining = true;
    return ok;
  }

  function onConfirm(ch, job, err) {
    if (err) {
      // amqplib also calls back with an error for every unconfirmed publish when the channel closes, just before
      // 'close'; onPublisherClosed handles those. Anything else is a nack: the channel is fine, the broker refused.
      queueMicrotask(() => {
        if (closedChannels.has(ch)) return;
        ch.flying.delete(job);
        job.ch = null;
        if (!job.done) park(job);
        flush();
      });
      return;
    }
    ch.flying.delete(job);
    job.ch = null;
    pubDelay = retryDelay.min;
    timingOut = false;
    if (job.returned) finish(job, new Error(`mmc-rabbitmq: unroutable, no queue bound for ${job.routingKey}`));
    else finish(job);
    flush();
  }

  // A nacked publish (for example to a full reject-publish queue) waits before it is sent again, longer each time.
  function park(job) {
    suspects.delete(job);
    job.nacks++;
    later(() => {
      if (job.done) return;
      unsent.add(job);
      flush();
    }, Math.min(retryDelay.min * 2 ** (job.nacks - 1), retryDelay.max));
  }

  // Still opens while close() drains, so publishes held through an outage go out if the broker returns in time.
  function openPublisher(model) {
    if (stopped || pubOpening || pubCh || pub.model !== model) return;
    pubOpening = true;
    model.createConfirmChannel().then(
      (ch) => {
        pubOpening = false;
        if (stopped || pub.model !== model) return quietClose(ch);
        ch.flying = new Set();
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
          const job = pending.get(msg.properties.headers?.[ID_HEADER]);
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
    // Includes publishes that timed out while on the channel: one of them may be the one the broker refused.
    const sent = [...ch.flying];
    ch.flying.clear();
    sent.forEach((job) => (job.ch = null));
    const missing = failure?.code === 404 && /no exchange '([^']*)'/.exec(failure.message);
    let identified = false;
    if (missing) {
      sent.filter((job) => job.exchange === missing[1]).forEach((job) => finish(job, failure));
      identified = true;
    } else if (failure && sent.length === 1) {
      finish(sent[0], failure);
      identified = true;
    } else if (failure) {
      sent.filter((job) => !job.done).forEach((job) => suspects.add(job));
    }
    if (identified && suspects.size) {
      // Usually one bad publish: send the rest normally again. Another bad one starts a new search.
      sent.push(...suspects);
      suspects.clear();
    }
    unsent = new Set([...sent.filter((job) => !job.done && !suspects.has(job)), ...unsent]);
    cursor = null;
    // Back off only while the cause is unknown.
    const known = !failure || identified;
    if (known) pubDelay = retryDelay.min;
    reopenPublisher(model, !known);
  }

  function reopenPublisher(model, backoff = true) {
    const delay = backoff ? pubDelay : 0;
    if (backoff) pubDelay = Math.min(pubDelay * 2, retryDelay.max);
    later(() => openPublisher(model), delay);
  }

  // Topology. Declarations get their own short-lived connection: a declare the broker answers by closing the
  // connection must not take publishing or consuming down with it.

  function runDeclarations(list = declarations) {
    if (stopped) return;
    if (declaring) return void (redeclare = true);
    const model = pub.model;
    if (!list.length) return markDeclared(model);
    declaring = true;
    redeclare = false;
    declareEach([...list]).then((failed) => {
      declaring = false;
      markDeclared(model);
      if (failed.length) {
        const delay = declareDelay;
        declareDelay = Math.min(declareDelay * 2, retryDelay.max);
        later(() => runDeclarations(failed), delay);
      } else {
        declareDelay = retryDelay.min;
      }
      if (redeclare) runDeclarations();
    });
  }

  // Each declaration gets its own channel, so one failure does not skip the rest. A declaration the broker answers
  // by closing the connection gets a fresh connection for the ones after it. Resolves to the declarations that failed.
  async function declareEach(list) {
    const failed = [];
    let conn = null;
    try {
      for (const [i, fn] of list.entries()) {
        if (stopped) return failed.concat(list.slice(i));
        try {
          if (!conn) conn = await openDeclareConnection();
        } catch (err) {
          if (!stopped) log.error(`mmc-rabbitmq: declaring topology failed: ${describe(err)}`);
          return failed.concat(list.slice(i));
        }
        if (stopped) return failed.concat(list.slice(i));
        const expired = new Error(`no answer within ${DECLARE_TIMEOUT} ms`);
        let timer;
        try {
          const timeout = new Promise((resolve, reject) => (timer = setTimeout(reject, DECLARE_TIMEOUT, expired)));
          timers.add(timer);
          await Promise.race([declareOne(conn.model, fn), timeout]);
        } catch (err) {
          failed.push(fn);
          if (!stopped) log.error(`mmc-rabbitmq: declaration ${declarations.indexOf(fn) + 1} failed: ${describe(conn.error ?? err)}`);
          // The broker may never answer on this connection; the next declaration gets a new one.
          if (err === expired) conn.model.close().catch(() => {});
          if (err === expired || conn.closed) conn = null;
        } finally {
          clearTimeout(timer);
          timers.delete(timer);
        }
      }
      return failed;
    } finally {
      if (conn && !conn.closed) conn.model.close().catch(() => {});
    }
  }

  async function declareOne(model, fn) {
    const ch = await model.createChannel();
    ch.on('error', () => {});
    await fn(ch);
    await quietClose(ch);
  }

  async function openDeclareConnection() {
    const model = await amqp.connect(withHeartbeat(url, heartbeat), {
      timeout: DECLARE_TIMEOUT,
      clientProperties: { connection_name: `${name} (declare)` },
    });
    const conn = { model, error: null, closed: false };
    declareConn = model;
    model.on('error', (err) => (conn.error = err));
    model.on('close', () => {
      conn.closed = true;
      if (declareConn === model) declareConn = null;
    });
    return conn;
  }

  // A round that a reconnect overtook does not release publishes on the new connection.
  function markDeclared(model) {
    if (model && model === pub.model) declaredOn = model;
    flush();
    if (!declared) {
      declared = true;
      consumers.forEach(subscribe);
    }
    checkReady();
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
        if (closing || sub.model !== model) return void quietClose(ch);
        const live = new AbortController();
        consumer.ch = ch;
        ch.on('error', (err) => log.warn(`mmc-rabbitmq: broker closed the ${consumer.queue} channel: ${describe(err)}`));
        ch.on('close', () => {
          closedChannels.add(ch);
          live.abort();
          if (consumer.ch !== ch) return;
          consumer.ch = null;
          consumer.tag = null;
          if (!closing && sub.model === model) retry(consumer);
        });
        // A failed subscribe closes its channel, so the next attempt starts clean on a new one.
        try {
          await ch.prefetch(consumer.prefetch);
          if (closing) return void quietClose(ch);
          const { consumerTag } = await ch.consume(consumer.queue, (msg) => onMessage(consumer, ch, live.signal, msg));
          // close() only cancels subscriptions it can see; this one completed after it looked.
          if (closing) return void ch.cancel(consumerTag).catch(() => {});
          consumer.tag = consumerTag;
          consumer.delay = retryDelay.min;
        } catch (err) {
          quietClose(ch);
          throw err;
        }
      })
      .catch((err) => {
        // A channel lost to close() or a reconnect is expected; the reconnect subscribes again.
        if (closing || sub.model !== model) return;
        log.warn(`mmc-rabbitmq: subscribing to ${consumer.queue} failed: ${describe(err)}`);
        retry(consumer);
      });
  }

  function retry(consumer) {
    if (consumer.timer) return;
    const delay = consumer.delay;
    consumer.delay = Math.min(delay * 2, retryDelay.max);
    consumer.timer = true;
    later(() => {
      consumer.timer = false;
      // Only through subscribe(): a channel that is still subscribing must not get a second consumer.
      if (!closing && !consumer.ch) subscribe(consumer);
    }, delay);
  }

  function onMessage(consumer, ch, signal, msg) {
    if (msg === null) {
      // Close the channel so in-flight handlers see their signal abort before the broker redelivers their messages.
      log.warn(`mmc-rabbitmq: broker cancelled the subscription to ${consumer.queue}, resubscribing`);
      consumer.tag = null;
      return void quietClose(ch);
    }
    // Delivered after close() started but before the cancel landed: leave it for another consumer.
    if (closing) return settle(ch, msg, 'requeue');
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
          // Includes the RetryLater that close() gives publishes it could not send: the handler did nothing wrong.
          if (err instanceof RetryLater && gone()) return settle(ch, msg, 'requeue');
          const exhausted = attempt >= retryLimit.attempts || Date.now() - firstTry + delay > retryLimit.ms;
          if (!(err instanceof RetryLater) || exhausted) {
            const retried = err instanceof RetryLater ? ` after ${attempt} attempts` : '';
            log.error(`mmc-rabbitmq: ${queue} handler failed${retried}, ${ON_ERROR[onError]}: ${describe(err)}`);
            return settle(ch, msg, onError);
          }
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
    if (closing) return Promise.reject(new Error('mmc-rabbitmq: closed'));
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
    // setTimeout turns NaN or Infinity into 1 ms, which would skip the drain.
    if (!(typeof graceMs === 'number' && graceMs >= 0)) graceMs = 25_000;
    closing ??= shutdown(Math.min(graceMs, MAX_TIMEOUT));
    return closing;
  }

  async function shutdown(graceMs) {
    readyWaiters.forEach((waiter) => waiter.fail());
    readyWaiters.clear();
    // Keep the end of the grace period for closing channels and connections, so close() stays within graceMs.
    const end = Date.now() + graceMs;
    const deadline = end - Math.min(2000, graceMs / 4);
    const left = () => Math.max(0, deadline - Date.now());
    const leftToEnd = () => Math.max(0, end - Date.now());
    // Cancel before waking RetryLater sleeps: a woken handler requeues its message, and the broker must not hand it
    // straight back to this consumer.
    const cancels = consumers
      .filter((c) => c.ch && c.tag)
      .map((c) => Promise.resolve().then(() => c.ch.cancel(c.tag)));
    await within(Promise.allSettled(cancels), left());
    wakers.forEach((wake) => wake());
    await within(Promise.allSettled([...inFlight]), left());
    if (pending.size) await within(new Promise((resolve) => idleWaiters.push(resolve)), left());
    if (pending.size) log.error(`mmc-rabbitmq: ${pending.size} publishes unconfirmed at shutdown`);
    // RetryLater, so a handler awaiting one of these requeues its message instead of failing it.
    [...pending.values()].forEach((job) => finish(job, new RetryLater('mmc-rabbitmq: closed before the broker confirmed'), true));
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
      // AMQP carries the prefetch in 16 bits; a larger value fails inside amqplib and leaves the consumer dead.
      if (!Number.isInteger(prefetch) || prefetch < 0 || prefetch > 65535) {
        throw new TypeError(`mmc-rabbitmq: consume('${queue}') needs an explicit prefetch (0 to 65535)`);
      }
      if (!ON_ERROR[onError]) throw new TypeError(`mmc-rabbitmq: onError must be 'reject' or 'requeue'`);
      const { attempts = Infinity, ms = Infinity } = retryLimit;
      if (!isLimit(attempts) || !isLimit(ms)) {
        throw new TypeError(`mmc-rabbitmq: retryLimit attempts and ms must be positive numbers, got ${attempts} and ${ms}`);
      }
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
      if (!isTimeout(ms)) {
        const err = new TypeError(`mmc-rabbitmq: publishWithin needs 0 < ms <= ${MAX_TIMEOUT}, got ${ms}`);
        log.error(`mmc-rabbitmq: publish to ${exchange || routingKey} failed: ${describe(err)}`);
        return rejected(err).promise;
      }
      const job = enqueue(exchange, routingKey, body, options);
      if (!job.id) return job.promise;
      const timer = setTimeout(() => {
        if (!timingOut) log.warn(`mmc-rabbitmq: publishes are timing out, no broker confirm within ${ms} ms`);
        timingOut = true;
        finish(job, new Error(`mmc-rabbitmq: no broker confirm within ${ms} ms`), true);
        // It may have been the next suspect, or the last one.
        flush();
      }, ms);
      const promise = job.promise.finally(() => clearTimeout(timer));
      promise.catch(() => {});
      return promise;
    },
    ready(ms) {
      if (closing) return Promise.reject(new Error('mmc-rabbitmq: closed'));
      if (!isTimeout(ms)) return Promise.reject(new TypeError(`mmc-rabbitmq: ready needs 0 < ms <= ${MAX_TIMEOUT}, got ${ms}`));
      if (isReady()) return Promise.resolve();
      return new Promise((resolve, reject) => {
        const waiter = {
          done() {
            clearTimeout(timer);
            resolve();
          },
          fail() {
            clearTimeout(timer);
            reject(new Error('mmc-rabbitmq: closed'));
          },
        };
        const timer = setTimeout(() => {
          readyWaiters.delete(waiter);
          reject(new Error(`mmc-rabbitmq: not ready within ${ms} ms`));
        }, ms);
        readyWaiters.add(waiter);
      });
    },
    stats: () => ({
      held: pending.size,
      heldBytes,
      inFlight: inFlight.size,
      blocked: Boolean(pub?.blocked),
      consumers: consumers.length,
      subscribed: consumers.filter((c) => c.tag).length,
    }),
  };
}

module.exports = { createRabbit, RetryLater };
