# mmc-rabbitmq

RabbitMQ client built on the official [amqplib](https://github.com/amqp-node/amqplib) client and its connection
recovery.

What it adds to amqplib:

- **Publishes are held until the broker confirms them**, across reconnects, up to a memory cap. A publish the
  broker refuses fails on its own; the publishes around it still go through.
- **Each consumer has its own channel.** When one breaks (a deleted queue, a bad ack, a server-side cancel), only
  that consumer re-subscribes; the others keep their in-flight messages.
- **Acks only go to the channel that delivered the message.** If that channel is gone, the broker redelivers.
- **`RetryLater`** retries a message in place while a dependency is down. Any other error rejects or requeues it,
  as the consumer chooses.
- **Publishing and consuming use separate connections**, so a broker that blocks publishers (memory or disk alarm)
  does not stall consumers.
- **Declarations run on their own short-lived connection** and are retried on failure, so a bad declaration
  cannot take publishing or consuming down.
- **`close()`** stops consuming, finishes in-flight work and waits for confirms, within its time limit.

## Install

Versions are git tags. Pin one, next to amqplib:

```json
"dependencies": {
  "amqplib": "2.2.0",
  "mmc-rabbitmq": "github:map-my-customers/mmc-rabbitmq#v1.0.0"
}
```

amqplib is a peer dependency pinned to the one version the tests run against, and the library uses the project's
copy. With npm's default settings, any other amqplib version fails `npm install` and `npm ci` with `ERESOLVE`.
`npm ci` also fails on a lock file written with `--force` or `--legacy-peer-deps`.

TypeScript projects need `@types/node`, as amqplib's own types do. amqplib bundles its types since 1.2.0, so code
that used an older amqplib untyped is now type-checked: for example, `connect()` returns a `ChannelModel`, not a
`Connection`.

## Use

```js
const { createRabbit, RetryLater } = require('mmc-rabbitmq');

async function main() {
  const mq = createRabbit({ url: process.env.RABBIT_URL, name: 'example-app' });

  // Topology this app owns, declared again after every reconnect.
  mq.declare((ch) => ch.assertQueue('example', { durable: true, arguments: { 'x-queue-type': 'quorum' } }));

  mq.consume('example', async (msg, { signal }) => {
    const body = JSON.parse(msg.content);
    if (!(await dependencyIsUp())) throw new RetryLater('dependency down');
    if (signal.aborted) return; // the delivering channel closed; the broker will redeliver this message
    await doWork(body);
  }, { prefetch: 10 });

  await mq.start();

  // Right after start(): a publish below can wait through an outage, and SIGTERM must still close.
  process.on('SIGTERM', async () => {
    await mq.close(25_000);
    process.exit(0);
  });

  // Fire and forget: the message is held until the broker confirms it. A failure is logged.
  mq.sendToQueue('example', { id: 1 });

  // Wait for the confirm before moving on.
  await mq.publish('exchange', 'routing.key', { id: 2 });

  // Wait a bounded time, e.g. inside an HTTP handler that should answer quickly.
  await mq.publishWithin(8000, 'exchange', 'routing.key', { id: 3 });
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
```

### Consumers

`consume(queue, handler, options)` takes:

| Option | Default | Meaning |
|---|---|---|
| `prefetch` | required | Unacked messages the broker sends at once, up to 65535. `0` means no limit. |
| `onError` | `'reject'` | What a handler error does. `'reject'` dead-letters the message if the queue has a dead-letter exchange and drops it otherwise. `'requeue'` puts it back; on a queue without a delivery limit, a message that always fails loops. |
| `retryLimit` | none | `{ attempts, ms }`: when `RetryLater` has been thrown this many times, or for this long, the message gets the `onError` outcome. Both are positive numbers. |

A handler receives the message and `{ signal }`. The signal aborts when the channel that delivered the message
closes; the broker then redelivers it, possibly while the first run is still going. Check it before side effects
that must not happen twice. When the broker cancels a subscription (the queue was deleted, or a consumer timeout on
RabbitMQ 4.x), the library closes that consumer's channel, so the signal aborts, and subscribes again.

`consume()` and `declare()` work before or after `start()`.

### Publishing

Every publish is `persistent` and `mandatory` unless the options say otherwise, and carries a random `messageId`
(also in the `x-mmc-rabbitmq-id` header) that stays the same if the library sends it again. Consumers can use it
to spot duplicates.

A publish rejects when the broker refuses it, when no queue is bound (unroutable), when the exchange is missing,
when the body is not a Buffer, string or JSON value, or when it is larger than `maxHeldBytes`. It never throws, and
a rejection nobody handles does not crash the process; the library logs it.

A publish the broker nacks (for example to a full `reject-publish` queue) does not reject: it is held and sent again,
waiting longer each time, up to the `retryDelay` maximum. Use `publishWithin` to bound the wait.

At most 256 publishes wait for a confirm at once; the rest are sent, in order, as confirms come back.

`maxHeldBytes` (64 MiB by default) caps the memory held for unconfirmed publishes. Each held publish counts as its
body plus 1.5 KiB for its bookkeeping, so at the default about 40,000 small messages fit. Set it below the memory the
process has to spare, or an outage long enough to fill it can get the process killed and lose what it held. Once
the cap is reached, new publishes reject with `RetryLater`, so a handler that awaits one retries its message in
place instead of failing it. The handler runs again from the start, so work it did before the publish runs again:
publish before side effects that must not repeat, or make them idempotent. `retryLimit` applies, after which the
message gets its `onError` outcome.

`publishWithin(ms, …)` rejects if the broker has not confirmed in time and stops holding the message. The message
may still be delivered, so retrying after a rejection can duplicate it.

Publishes made while `close()` is draining still go out; once `close()` has finished, publishing rejects. A publish
still unconfirmed when `close()` runs out of time rejects with `RetryLater`, so a handler awaiting it puts its message
back on the queue instead of failing it. If the
broker is down when `close()` starts, it keeps reconnecting until its time limit, so publishes held through the outage
still go out if the broker returns in time.

### Status

- `start()` returns once connecting has begun; the library keeps retrying in the background. After `close()` it
  rejects.
- `ready(ms)` waits until publishes can be sent, or rejects after `ms`, or once `close()` has started. Useful in
  scripts.
- `isReady()` is true when a publish would be sent right now: the publish channel is open, the first round of
  declarations has finished, and the broker is not blocking publishers. After a reconnect, publishes also wait the
  few milliseconds until that connection's declarations finish, in case the topology did not survive the broker.
- `stats()` reports held publishes and bytes, in-flight handlers, whether publishing is blocked, and how many
  consumers are subscribed.

Options: `heartbeat` (whole seconds from 0 to 65535, default 60), `maxHeldBytes` (default 64 MiB), `retryDelay` (`{ min, max }` in ms, default 1 s to
30 s; used for reconnecting, re-subscribing and `RetryLater` backoff) and `log` (default `console`). `createRabbit`
throws a `TypeError` for a value that is not a number in range, such as an unparsed environment variable.
`publishWithin(ms, …)` and `ready(ms)` reject with a `TypeError` unless `0 < ms <= 2147483647`. `close(graceMs)`
takes at most that long; a missing or invalid value means 25 s.

Each instance holds one publish connection, one consume connection once it has a consumer, and a short-lived
declare connection while declarations run (after each publish reconnect, and for a `declare()` after `start()`).

## Test

The tests need a broker. With Docker:

```sh
docker run -d --name rabbit -p 5672:5672 rabbitmq:3.13-management
npm test
```

`RABBIT_URL` and `RABBIT_CONTAINER` point the tests at another broker. CI runs the suite against RabbitMQ 3.13, 4.2
and 4.3, including a broker restart during traffic and a memory alarm.

## Release

1. Bump the version in the PR with `npm version --no-git-tag-version 1.2.3`, which updates `package.json` and both
   places in `package-lock.json`.
2. After the squash merge, tag the merge commit on `master` with the same version (`git tag v1.2.3 && git push origin v1.2.3`).
3. Projects move to the new tag in their own changes.

To support a new amqplib version, change it in both `peerDependencies` and `devDependencies`, run `npm install`,
commit `package-lock.json`, let CI run, and release. Projects then move amqplib and the tag together.

Tags matching `v*` cannot be moved or deleted except by an admin (break-glass), so a version always points at the
same code.
