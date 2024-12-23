import { KafkaClient, Consumer } from 'kafka-node';
import { createClient } from 'redis';

const gittest = 0;
// Kafka setup
const client = new KafkaClient({ kafkaHost: 'localhost:9092' });
const consumer = new Consumer(
  client,
  [
    { topic: 'test-topic1', partition: 0 },
    { topic: 'test-topic2', partition: 0 }
  ],
  { autoCommit: true }
);

// Redis setup
const redisClient = createClient({ url: 'redis://localhost:6379' });
redisClient.on('error', (err) => console.error('Redis error:', err));

// Ensure Redis client is connected
async function start() {
  try {
    await redisClient.connect();
    console.log('Connected to Redis');

    consumer.on('message', async (message) => {
      const topic = message.topic;
      const key = `${topic}:${message.offset}`;

      try {
        // Save each message under a unique key in Redis
        await redisClient.set(key, message.value || '');
        console.log(`Message from ${topic} stored with key ${key}`);
      } catch (error) {
        console.error(`Error storing message: ${error}`);
      }
    });

    consumer.on('error', (err) => {
      console.error('Kafka error:', err);
    });
  } catch (err) {
    console.error('Error connecting to Redis:', err);
  }
}

start();

