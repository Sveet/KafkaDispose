import { Kafka, KafkaConfig, logLevel } from 'kafkajs';
import { ConsumerParams, ProducerParams } from './types';
import { Consumer } from './consumer';
import { Producer } from './producer';

export function KafkaDispose(config: KafkaConfig) {
  config.logLevel ??= logLevel.WARN;
  const kafka = new Kafka(config);
  return {
    consume: (params: ConsumerParams) => new Consumer(kafka, params),
    produce: (params: ProducerParams) => new Producer(kafka, params),
  };
}
