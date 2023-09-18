import { Message as KafkaMessage } from 'kafkajs';
import { KafkaDispose } from '../dist/index';

const { consume, produce } = KafkaDispose({ brokers: [], clientId: 'demo' });

await using consumer = consume({ topic: 'foo', consumerConfig: { groupId: 'demo-consumer' } });
await using producer = produce({ topic: 'foo' });

const chunks: KafkaMessage[] = [
  { value: 'foo\n' },
  { value: 'bar\n' },
  { value: 'baz\n' },
  { value: 'boo\n' },
];
producer.write(chunks, (err) => err ? console.error(err) : null);

consumer.pipe(process.stdout);