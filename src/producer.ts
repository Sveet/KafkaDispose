import {
  Kafka,
  Producer as KafkaProducer,
  Message as KafkaMessage,
  RecordMetadata,
  CompressionTypes,
} from 'kafkajs';
import { Writable } from 'stream';
import { ProducerParams } from './types';

export class Producer extends Writable {
  private producer: KafkaProducer;
  constructor(
    private kafka: Kafka,
    private params: ProducerParams,
  ) {
    super({ objectMode: true });
    this.params.bufferMessages ??= 10;
    this.params.bufferTime_ms ??= 100;
    this.producer = this.kafka.producer(this.params.config);
  }

  private lastSend: number;
  private chunks: KafkaMessage[] = [];
  _write(chunk: KafkaMessage, _: BufferEncoding, callback: (error?: Error) => void): void {
    this.chunks.push(chunk);

    if (
      this.chunks.length > this.params.bufferMessages ||
      this.lastSend + this.params.bufferTime_ms > Date.now()
    ) {
      const chunks = [...this.chunks];
      this.chunks = [];
      this.send(this.params.topic, chunks)
        .then(() => callback())
        .catch(callback);
    } else {
      callback();
    }
  }
  private async send(
    topic: string,
    messages: KafkaMessage[],
    retries = 1,
  ): Promise<RecordMetadata[]> {
    try {
      const response = await this.producer.send({
        topic: topic,
        messages: messages,
        compression: CompressionTypes.GZIP,
      });
      return response;
    } catch (err) {
      if (retries > 0 && `${err}`.includes('The producer is disconnected')) {
        console.warn(
          `Sending KafkaJS messages failed because Producer was disconnected. Reconnecting (${retries}) and retrying to send.`,
        );
        this.producer = this.kafka.producer(this.params.config);
        await this.producer.connect();
        return this.send(topic, messages, retries - 1);
      } else {
        throw err;
      }
    }
  }
}
