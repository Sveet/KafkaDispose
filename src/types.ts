import {ConsumerConfig, ConsumerRunConfig, IHeaders} from 'kafkajs'

export type RunConfig = Pick<
  ConsumerRunConfig,
  'autoCommit' | 'autoCommitInterval' | 'autoCommitThreshold' | 'partitionsConsumedConcurrently'
>;
export type ConsumerParams = {
  consumerConfig: ConsumerConfig
  runConfig?: RunConfig
  topic?: string,
  topics?: string[],
  partition?: number
  offset?: number
}

export type Message = {
  partition: number;
  offset: string;
  headers: IHeaders;
  value: string;
  complete: () => void;
};