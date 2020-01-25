//const avro = require('avsc');
const kafka = require('kafka-node');
const client = new kafka.KafkaClient({ kafkaHost: 'localhost:9092' });
const registryUrl = 'http://localhost:8081';
const registry = require('avro-schema-registry')(registryUrl);
const utils = require('./utilities');

const topicName = 'test1-topic';

const address = utils.getIp();

const topic = {
    topic:  topicName, //'actor7',
    offset: 0 //3695
};

const version = 11;

const opts = {
    // groupId: 'dummy-' + new Date().getTime(),
    groupId: address + '-my-group', //'dummy',
    fromOffset: true
};

// don't make this go up linter
(async () => {
    //const results = [];

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
        //results.push({ message, key, value });

        console.log(`topic: ${message.topic}  ${key} -> ${value}`);
    });

    //console.log('starting Timer');
    //setInterval(() => {
    //    console.log(results), 5 * 1000);
    //});
})();

