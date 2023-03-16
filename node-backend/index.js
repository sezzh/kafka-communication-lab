const express = require('express')
const { Kafka } = require('kafkajs')

const kafka = new Kafka({
  clientId: 'node-app',
  brokers: ['kafka:9092']
})

const kafkaProducer = kafka.producer()

const kafkaConsumer = kafka.consumer({ groupId: 'node-group' })


const app = express()
const port = 3000

app.get('/', async (req, res) => {
  await kafkaProducer.connect()
  await kafkaProducer.send({
    topic: 'node-topic',
    messages: [
      { value: 'Hello KafkaJS user!' }
    ]
  })
  await kafkaProducer.disconnect()
  res.send('Hello World!')
})

app.get('/subscribe-kafka', async (req, res) => {
  await kafkaConsumer.connect()
  await kafkaConsumer.subscribe({ topic: 'node-topic', fromBeginning: true })

  await kafkaConsumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      console.log({
        value: message.value.toString(),
      })
    },
  })

  res.send('Hello subs!')
})

app.listen(port, () => {
  console.log(`Example app listening on port ${port}`)
})