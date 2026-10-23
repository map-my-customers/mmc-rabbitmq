# mmc-rabbitmq

RabbitMQ client built on the official [amqplib](https://github.com/amqp-node/amqplib) client and its connection
recovery.

What it adds to amqplib:

- **Publishes are held until the broker confirms them**, across reconnects, up to a memory cap.
- **Consumers are re-subscribed** after every reconnect, with an explicit prefetch.
- **Acks only go to the channel that delivered the message.** If that channel is gone, the broker redelivers.
- **`RetryLater`** retries a message in place while a dependency is down; any other error rejects it to the
  queue's dead-letter exchange.
- **`close()`** stops consuming, finishes in-flight work and waits for confirms, within a time limit.

## Install

Versions are git tags. Pin one, next to amqplib:

```json
"dependencies": {
  "amqplib": "2.2.0",
  "mmc-rabbitmq": "github:map-my-customers/mmc-rabbitmq#v1.0.0"
}
```

amqplib is a peer dependency pinned to the one version the tests run against, and the library uses the project's
copy. Any other amqplib version fails `npm install` and `npm ci` with `ERESOLVE`, even if the lock file was
written with `--force` or `--legacy-peer-deps`.

## Use

```js
const { createRabbit, RetryLater } = require('mmc-rabbitmq');

const mq = createRabbit({ url: process.env.RABBIT_URL, name: 'example-app' });

// Topology this app owns, re-declared on every reconnect.
mq.declare((ch) => ch.assertQueue('example', { durable: true, arguments: { 'x-queue-type': 'quorum' } }));

mq.consume('example', async (msg) => {
  const body = JSON.parse(msg.content);
  if (!(await dependencyIsUp())) throw new RetryLater('dependency down');
  await doWork(body);
}, { prefetch: 10 });

await mq.start();

// Fire and forget: the message is held until the broker confirms it.
mq.sendToQueue('example', { id: 1 }).catch((err) => log.error(err));

// Wait for the confirm before moving on.
await mq.publish('exchange', 'routing.key', { id: 2 });

// Wait a bounded time, e.g. inside an HTTP handler that should answer quickly.
await mq.publishWithin(8000, 'exchange', 'routing.key', payload);

process.on('SIGTERM', async () => {
  await mq.close(25_000);
  process.exit(0);
});
```

`mq.isReady()` is for readiness probes. `mq.stats()` reports held publishes and in-flight handlers.

An unroutable publish (no queue bound) and a publish to a missing exchange reject instead of being dropped.

## Test

The tests need a broker. With Docker:

```sh
docker run -d --name rabbit -p 5672:5672 rabbitmq:3.13-management
npm test
```

CI runs the suite against RabbitMQ 3.13, 4.2 and 4.3, including a broker restart during traffic.

## Release

1. Bump `version` in `package.json` in the PR.
2. After the squash merge, tag the merge commit on `master` with the same version (`git tag v1.2.3 && git push origin v1.2.3`).
3. Projects move to the new tag in their own changes.

To support a new amqplib version, change it in both `peerDependencies` and `devDependencies`, let CI run, and
release. Projects then move amqplib and the tag together.

Tags matching `v*` cannot be moved or deleted, so a version always points at the same code.
