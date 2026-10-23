import type { Channel, ConfirmChannel, ConsumeMessage, Options } from 'amqplib';

export class RetryLater extends Error {}

export interface RabbitOptions {
  url: string | Options.Connect;
  name: string;
  heartbeat?: number;
  maxHeldBytes?: number;
  retryDelay?: { min: number; max: number };
  log?: Pick<Console, 'info' | 'warn' | 'error'>;
}

type Body = Buffer | string | object;

export interface Rabbit {
  start(): Promise<unknown>;
  close(graceMs?: number): Promise<void>;
  declare(fn: (ch: ConfirmChannel) => Promise<unknown>): void;
  consume(queue: string, handler: (msg: ConsumeMessage) => Promise<unknown> | unknown, options?: { prefetch?: number }): void;
  publish(exchange: string, routingKey: string, body: Body, options?: Options.Publish): Promise<void>;
  sendToQueue(queue: string, body: Body, options?: Options.Publish): Promise<void>;
  publishWithin(ms: number, exchange: string, routingKey: string, body: Body, options?: Options.Publish): Promise<void>;
  isReady(): boolean;
  stats(): { held: number; heldBytes: number; inFlight: number };
}

export function createRabbit(options: RabbitOptions): Rabbit;
export type { Channel };
