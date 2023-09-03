import { Message as KafkaMessage } from 'kafkajs';
import {KafkaDispose} from '../dist/index'
const {consume, produce} = KafkaDispose({brokers: [], clientId: 'demo'});

await using c = consume({topic: 'foo', consumerConfig: {groupId: 'demo-consumer'}});
await using p = produce({topic: 'foo'})

const chunks: KafkaMessage[] = [
    {value: 'foo'},
    {value: 'bar'},
    {value: 'baz'},
    {value: 'boo'},
]
p.write(chunks, (err) => err ? console.error(err) : null)

c.pipe(process.stdout);