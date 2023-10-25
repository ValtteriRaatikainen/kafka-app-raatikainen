import { Kafka } from 'kafkajs';
console.log("*** Consumer starts... ***");

const kafka = new Kafka({
    clientId: 'checker-server',
    brokers: ['localhost:9092']
});

const consumer = kafka.consumer({ groupId: 'checker-checker-servers1' });

const run = async () => {
    await consumer.connect();
    await consumer.subscribe({ topic: 'tobechecked', fromBeginning: true });

    await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            console.log({
                key: message.key.toString(),
                partition: message.partition,
                offset: message.offset,
                value: message.value.toString(),
            });
        },
    })
};

run().catch(console.error);