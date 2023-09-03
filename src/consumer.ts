import { Kafka, Consumer as KafkaConsumer, ConsumerConfig, ConsumerRunConfig } from 'kafkajs'
import {Readable} from 'stream'

export type RunConfig = Pick<
  ConsumerRunConfig,
  'autoCommit' | 'autoCommitInterval' | 'autoCommitThreshold' | 'partitionsConsumedConcurrently'
>;
export type ConsumerParams = {
  kafka: Kafka
  consumerConfig: ConsumerConfig
  runConfig?: RunConfig
  topic?: string,
  topics?: string[],
  partition?: number
  offset?: number
}
export class Consumer extends Readable {
  private consumer: KafkaConsumer
  [Symbol.dispose](): void {
    this.consumer.disconnect();
  }
  constructor(private params: ConsumerParams){
    super({objectMode: true});
    
    let {kafka, consumerConfig, runConfig, topic, topics, partition, offset} = params;
    consumerConfig.allowAutoTopicCreation ??= false;
    consumerConfig.maxBytes ??= 2048; // 2KB

    runConfig ??= {};
    runConfig.autoCommit ??= true;

    if (topic && topics) {
      topics = undefined;
    }
    this.consumer = kafka.consumer(consumerConfig)
  }

  _read(size: number): void {
    
  }
}