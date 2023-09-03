import { Kafka, Consumer as KafkaConsumer } from 'kafkajs';
import { Readable } from 'stream';
import { ConsumerParams, Message } from './types';
import { HeartbeatManager } from './heartbeat';

export class Consumer extends Readable {
  private consumer: KafkaConsumer;

  private messages: Message[] = [];
  _read(size: number): void {
    if (size && size < 0) return;
    if (this.messages?.length > 0) {
      this.push(this.messages.slice(0, size));
    }
  }

  async [Symbol.asyncDispose](): Promise<void> {
    try {
      await this.consumer.disconnect();
      console.debug(`Disconnected consumer ${this.params.consumerConfig.groupId}`);
    } catch (err) {
      console.error(`Error disconnecting consumer ${this.params.consumerConfig.groupId} ${err}`);
    }
  }

  constructor(
    private kafka: Kafka,
    private params: ConsumerParams,
  ) {
    super({ objectMode: true });
    this.params.consumerConfig.allowAutoTopicCreation ??= false;
    this.params.consumerConfig.maxBytes ??= 2048; // 2KB
    this.consumer = this.kafka.consumer(this.params.consumerConfig);
    this.consumer.on('consumer.crash', (e) => {
      const eventString = `${typeof e.payload.error} ${e.payload.error} ${e.payload.error.stack}`;
      if (e.payload.restart) {
        // rebalancing sometimes runs out of internal retries and requires a consumer restart
        console.error(
          `Consumer ${this.params.consumerConfig.groupId} received a non-retriable error: ${eventString}`,
        );
        return this.restartConsumer();
      }
    });
    this.run().catch((err) => {
      console.error(`KafkaJS consumer.run threw error ${err}`);
      return this.restartConsumer();
    });
  }

  private async run() {
    const { consumerConfig, runConfig, topic, topics, partition, offset } = this.params;
    await this.consumer.connect();

    await this.consumer.subscribe({ topic, topics, fromBeginning: false });

    this.consumer.run({
      ...runConfig,
      autoCommit: true,
      eachBatchAutoResolve: false,
      eachBatch: async ({
        batch,
        isRunning,
        isStale,
        resolveOffset,
        heartbeat,
        commitOffsetsIfNecessary,
      }) => {
        using _ = new HeartbeatManager(heartbeat);  // eslint-disable-line
        const messagesInProgress = [];
        for (const message of batch.messages) {
          let completeResolver: () => void;
          messagesInProgress.push(new Promise<void>((resolve) => (completeResolver = resolve)));
          const complete = async () => {
            if (!isRunning() || isStale()) return;
            await commitOffsetsIfNecessary();

            resolveOffset(message.offset);
            completeResolver();
          };
          this.messages.push({
            partition: batch.partition,
            offset: message.offset,
            headers: message.headers,
            value: message.value.toString(),
            complete,
          });
        }
        await Promise.all(messagesInProgress);
      },
    });
    if (partition !== undefined && offset !== undefined) {
      console.debug(`${consumerConfig.groupId} seeking offset: ${offset}, partition: ${partition}`);
      this.consumer.seek({ topic, partition, offset: `${offset}` });
    }
  }

  private restartConsumer = async () => {
    try {
      await this.consumer.disconnect();
      await this.run();
    } catch (err) {
      console.error(`Failed to restart consumer ${this.params.consumerConfig.groupId}: ${err}`);
    }
  };
}
