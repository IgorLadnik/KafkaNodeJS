//const avro = require('avsc');
const kafka = require('kafka-node');
const client = new kafka.KafkaClient({ kafkaHost: 'localhost:9092' });
const registryUrl = 'http://localhost:8081';
const registry = require('avro-schema-registry')(registryUrl);
const utils = require('./utilities');

const topicName = 'test1-topic';

const address = utils.getIp();

const topic = {
    topic:  topicName,
    offset: 0
};

const version = 11;

const opts = {
    groupId: address + '-my-group',
    fromOffset: true
};

(async () => {
    // ensure we connect
    await client.once('connect', msg => {
        console.log('connect', { msg });
    });

    // wire up our stuff
    const consumer = new kafka.Consumer(client, [topic], opts);

    consumer.on('error', err => console.warn(err));

    consumer.on('message', async message => {
        const key = await registry.decode(Buffer.from(message.key));
        const value = await registry.decode(Buffer.from(message.value));

        console.log(`topic: ${message.topic}  ${key} -> ${value}`);
    });
})();


