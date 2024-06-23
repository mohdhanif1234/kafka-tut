import { kafka } from './client.js'

const group=process.argv[2]

const init = async () => {
    const consumer = kafka.consumer({
        groupId: group
    })

    await consumer.connect();

    await consumer.subscribe({
        topics: ['rider-updates'],
        fromBeginning: true
    })

    await consumer.run({
        eachMessage: async ({ topic, partition, message, heartbeat, pause }) => {
            console.log(`GROUP: ${group}, TOPIC: [${topic}], PARTITION :${partition}, MESSAGE: ${message.value.toString()}`)
        }
    })

}

init()