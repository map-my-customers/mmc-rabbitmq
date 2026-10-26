import type { Channel, ConsumeMessage, Options } from 'amqplib';

export class RetryLater extends Error {
  constructor(message?: string, options?: { cause?: unknown });
}

export interface RabbitOptions {
  url: string | Options.Connect;
  name: string;
  heartbeat?: number;
  /** Memory held for unconfirmed publishes; past it, publishes reject with RetryLater. Default 64 MiB. */
  maxHeldBytes?: number;
  retryDelay?: { min: number; max: number };
  log?: Pick<Console, 'info' | 'warn' | 'error'>;
}

export interface ConsumeOptions {
  prefetch: number;
  onError?: 'reject' | 'requeue';
  retryLimit?: { attempts?: number; ms?: number };
}

export interface Delivery {
  /** Aborts when the channel that delivered the message closes; the broker will redeliver it. */
  signal: AbortSignal;
}

type Body = Buffer | string | object | number | boolean | null;

export interface Rabbit {
  start(): Promise<void>;
  ready(timeoutMs: number): Promise<void>;
  close(graceMs?: number): Promise<void>;
  declare(fn: (ch: Channel) => Promise<unknown>): void;
  consume(
    queue: string,
    handler: (msg: ConsumeMessage, delivery: Delivery) => Promise<unknown> | unknown,
    options: ConsumeOptions,
  ): void;
  publish(exchange: string, routingKey: string, body: Body, options?: Options.Publish): Promise<void>;
  sendToQueue(queue: string, body: Body, options?: Options.Publish): Promise<void>;
  publishWithin(ms: number, exchange: string, routingKey: string, body: Body, options?: Options.Publish): Promise<void>;
  isReady(): boolean;
  stats(): {
    held: number;
    heldBytes: number;
    inFlight: number;
    blocked: boolean;
    consumers: number;
    subscribed: number;
  };
}

export function createRabbit(options: RabbitOptions): Rabbit;
export type { Channel };
