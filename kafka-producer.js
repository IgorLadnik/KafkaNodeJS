const registryUrl = 'http://localhost:8081';

//const avro = require('avsc');
const registry = require('avro-schema-registry')(registryUrl);
const kafka = require('kafka-node');
const request = require('request');
const client = new kafka.KafkaClient({ kafkaHost: 'localhost:9092' });
const utils = require('./utilities');

var fs = require('fs');

var avroSchema = JSON.parse(fs.readFileSync('schema.json', 'utf8'));

var avro = require('avsc');
var type = avro.parse(avroSchema);

const topic = {
    topic: 'test1-topic',
    offset: 0
};

const version = 11;

const address = utils.getIp();

//const opts = {
//     groupId: 'dummy-' + new Date().getTime(),
//    groupId: address + '-my-group', //'dummy',
//    fromOffset: true
//};

const opts = {
    // Configuration for when to consider a message as acknowledged, default 1
    requireAcks: 1,
    // The amount of time in milliseconds to wait for all acks before considered, default 100ms
    ackTimeoutMs: 100,
    // Partitioner type (default = 0, random = 1, cyclic = 2, keyed = 3, custom = 4), default 0
    partitionerType: 3,
    //groupId: address + '-my-group' //'dummy',
};

const customPartitioners = undefined;
// const customPartitioners = [{...}]

// don't make this go up linter
(async () => {
    const schemas = {
        //key: await fetchSchema(registryUrl, '${topic.topic}-key', 1),
        //value: await fetchSchema(registryUrl, '${topic.topic}-value', 1)
        key: await fetchSchema(registryUrl, topic.topic, version),
        value: await fetchSchema(registryUrl, topic.topic, version)
    };

    // ensure we connect
    await client.once('connect', msg => console.log('connect', { msg }));
    console.log('connected');

    const producer = new kafka.Producer(client, opts, customPartitioners);
    console.log('Producer created', producer);
    producer.on('error', err => console.warn('Producer error', err));

    producer.on('ready', async () => {
        console.log('producer ready');

        const payloads = [];
        for (let id = 0; id < 10; id++) {
            const k = id.toString();
            const v = {
                Id: id,
                Name: 'theName',
                BatchId: 7,
                TextData: "Some-text-data",
                NumericData: 1234
            };

            var messageBuffer = type.toString({
                Id: id,
                Name: 'theName',
                BatchId: 7,
                TextData: "Some-text-data",
                NumericData: 1234
            });

            console.log(messageBuffer);

            const key = await registry.encodeKey(topic.topic, 'string', k);
            const value = await registry.encodeMessage(topic.topic, 'string', messageBuffer);

            //const key = await registry.encodeKey(topic.topic, schemas.key, k);
            //const value = await registry.encodeMessage(topic.topic, schemas.value, v);

            //payloads.push({
            //    topic: topic.topic,
            //    messages: [messageBuffer]
            //});

            //payloads.push({
            //    topic: topic.topic,
            //    messages: [new kafka.KeyedMessage(key, value)]
            //});

            //const key = id.toString();

            payloads.push({
                topic: topic.topic,
                messages: [new kafka.KeyedMessage(key, messageBuffer)]
            });
        }

        producer.send(payloads, (err, data) => {
            if (err) {
                console.warn('errored', err);
            } else {
                console.log(data);
            }
        });
    });
})();

function fetchSchema(registryUrl, topic, version) {
    return new Promise((resolve, reject) => {
        request(
            //`${registryUrl}/subjects/${topic}/versions/${version}/schema`,
            //'http://localhost:8081/subjects/__consumer_offsts/versions/11/schema',
            registryUrl,
            (err, res, body) => {
                if (res.statusCode !== 200) {
                    const error = JSON.parse(body);
                    return reject(
                        new Error(
                            `Schema registry error: ${error.error_code} - ${error.message}`
                        )
                    );
                }
                else {
                    console.log('200 - OK');
                }

                console.log('-> ' + resolve(JSON.parse(body)));
            }
        );
    });
}
