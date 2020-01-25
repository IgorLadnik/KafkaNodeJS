const registryUrl = 'http://localhost:8081';

const avro = require('avsc');
const registry = require('avro-schema-registry')(registryUrl);
const kafka = require('kafka-node');
const client = new kafka.KafkaClient({ kafkaHost: 'localhost: 9092' });

const topic = {
    topic: 'actor7',
    offset: 3695
};
const opts = {
  // groupId: 'dummy-' + new Date().getTime(),
  groupId: 'dummy',
  fromOffset: true
}

// don't make this go up linter
; (async () => {
    const results = [];

    // ensure we connect
    await client.once('connect', msg => {
        console.log('connect', { msg });
    })

    // wire up our stuff
    const consumer = new kafka.Consumer(client, [topic], opts);
    consumer.on('error', err => {
        console.warn(err);
    });
    consumer.on('message', async __message => {
        const key = await registry.decode(new Buffer(__message.key));
        const value = await registry.decode(new Buffer(__message.value));
        results.push({ __message, key, value })
    })

    console.log('starting Timer');
    setTimeout(() => {
        console.log(results);
    }, 5 * 1000);
})();
