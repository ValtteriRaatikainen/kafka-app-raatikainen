import { Kafka, Partitioners } from 'kafkajs';
import { v4 as UUID } from 'uuid';

console.log("*** Producer starts... ***");

const kafka = new Kafka({
    clientId: 'my-checking-client',
    brokers: ['localhost:9092'],
});

const producer = kafka.producer({ createPartitioner: Partitioners.DefaultPartitioner });
const consumer = kafka.consumer({ groupId: 'my-group' });

async function runProducer() {
    await producer.connect();
    await consumer.connect();

    await consumer.subscribe({ topic: 'tobechecked', fromBeginning: true });

    await consumer.run({
        eachMessage: async ({ message }) => {
            const { key, value } = message;
            const idNumber = value.toString();
            const fahrenheit = parseFloat(key);
            const celsius = (fahrenheit - 32) * 5 / 9;

            // Simuloidaan muutaman sekunnin kuluttua vastausta 'convertedresult'-kanavalle
            setTimeout(async () => {
                const requestId = UUID();
                const response = {
                    requestId,
                    celsius,
                };
                await producer.send({
                    topic: 'convertedresult',
                    messages: [{ key: requestId, value: JSON.stringify(response) }],
                });
                console.log(`Message ${requestId} sent to 'convertedresult': Fahrenheit ${fahrenheit}°F -> Celsius ${celsius}°C`);
            }, 3000);
        },
    });

    // Aloita lähettäminen alkuperäiselle consumerille
    setInterval(() => {
        queueMessage();
    }, 2500);
}

runProducer().catch(console.error);

const idNumbers = [
    "311299-999X",
    "010703A999Y",
    "240588+9999",
    "NNN588+9999",
    "112233-9999",
    "300233-9999",
    "30233-9999",
];

function randomFahrenheitToCelsius() {
    const randomFahrenheit = Math.random() * (100 - (-22)) + (-22);
    const celsius = (randomFahrenheit - 32) * (5 / 9);
    return celsius;
}

function randomizeIntegerBetween(from, to) {
    return Math.floor(Math.random() * (to - from + 1)) + from;
}

async function queueMessage() {
    const uuidFraction = UUID().substring(0, 4);

    const success = await producer.send({
        topic: 'tobechecked',
        messages: [
            {
                key: randomFahrenheitToCelsius().toString(),
                value: Buffer.from(idNumbers[randomizeIntegerBetween(0, idNumbers.length - 1)]),
            },
        ],
    });

    if (success) {
        console.log(`Message ${uuidFraction} queued successfully!`);
    } else {
        console.log(`Message ${uuidFraction} queued unsuccessfully!`);
    }
}