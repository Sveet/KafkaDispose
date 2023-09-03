import { Kafka, KafkaConfig, logLevel } from "kafkajs";
import { ConsumerParams } from "./types";
import { Consumer } from "./consumer";

export default function KafkaDispose(config: KafkaConfig) {
  config.logLevel ??= logLevel.WARN;
  const kafka = new Kafka(config);
  return {
    consume: (params: ConsumerParams)=> new Consumer(kafka, params),
    produce: ()=>{}
  }
}