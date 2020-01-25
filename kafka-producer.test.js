const registryUrl = 'http://localhost:8081';

const avro = require('avsc');
const registry = require('avro-schema-registry')(registryUrl);
const kafka = require('kafka-node');
const request = require('request');
const client = new kafka.KafkaClient({ kafkaHost: 'localhost:9092' });

const topic = {
    topic: 'actors7'
};

const opts = {
    // Configuration for when to consider a message as acknowledged, default 1
    requireAcks: 1,
    // The amount of time in milliseconds to wait for all acks before considered, default 100ms
    ackTimeoutMs: 100,
    // Partitioner type (default = 0, random = 1, cyclic = 2, keyed = 3, custom = 4), default 0
    partitionerType: 3
};

const customPartitioners = undefined
// const customPartitioners = [{...}]

// don't make this go up linter
    ; (async () => {
        const schemas = {
            key: await fetchSchema(registryUrl, `${topic.topic}-key`, 1),
            value: await fetchSchema(registryUrl, `${topic.topic}-value`, 1)
        };

        // ensure we connect
        await client.once('connect', msg => {
            console.log('connect', { msg });
        });

        console.log('connected');

        const producer = new kafka.Producer(client, opts, customPartitioners);
        console.log('Producer created', producer);
        producer.on('error', err => {
            console.warn('Producer error', err);
        });
        // producer.on('ready', async () => {
        console.log('producer ready');

        const payloads = []
        for (let id = 0; id < 10; id++) {
            const k = {
                id: id.toString(),
                timestamp: `2017-05-26 14:${id}:57.62`
            };
            const v = {
                id,
                firstName: 'First' + id,
                lastName: 'Last' + id,
                lastUpdate: k.timestamp
            };

            const key = await registry.encodeKey(topic.topic, schemas.key, k);
            const value = await registry.encodeMessage(topic.topic, schemas.value, v);
            // const value = await registry.encodeMessage(topic.topic, schemas.value, v)

            payloads.push({
                topic: topic.topic,
                messages: [new kafka.KeyedMessage(key, value)]
            });
        }
        producer.send(payloads, function (err, data) {
            if (err) {
                console.warn('errored', err);
            } else {
                console.log(data);
            }
        });
    })();

function fetchSchema(registryUrl, topic, version) {
    return new Promise((resolve, reject) => {
        request(
            `${registryUrl}/subjects/${topic}/versions/${version}/schema`,
            (err, res, body) => {
                if (res.statusCode !== 200) {
                    const error = JSON.parse(data);
                    return reject(
                        new Error(
                            `Schema registry error: ${error.error_code} - ${error.message}`
                        )
                    );
                }
                resolve(JSON.parse(body));
            }
        );
    });
}

