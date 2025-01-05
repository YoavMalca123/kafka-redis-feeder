import { KafkaClient, Consumer } from 'kafka-node';
import { createClient } from 'redis';

// Kafka setup
const client = new KafkaClient({ kafkaHost: 'localhost:9092' }); //connect to port 9092 (kafka's port)
const consumer = new Consumer(
  client,
  [
    { topic: 'test-topic1', partition: 0 },
    { topic: 'test-topic2', partition: 0 }
  ],
  { autoCommit: true }
);

//redis setup
const redisClient = createClient({ url: 'redis://localhost:6379' });
redisClient.on('error', (err) => console.error('Redis error:', err)); //check for errors while connecting to redis

async function start() {
  try {
    await redisClient.connect();
    console.log('Connected to Redis');

    consumer.on('message', async (message) => {
      const topic = message.topic;
      const key = `${topic}:${message.offset}`;  //save and give each message their own key

      try {
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

