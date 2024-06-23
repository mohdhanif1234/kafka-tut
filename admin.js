import { kafka } from './client.js'

const init = async () => {
    const admin = kafka.admin();
    console.log('admin connecting...')
    await admin.connect();
    console.log('admin connection success..')

    console.log('creating topic [rider-updates]...')
    await admin.createTopics({
        topics: [{
            topic: 'rider-updates',
            numPartitions: 2
        }]
    })

    console.log('topic created success...')

    console.log('admin disconnecting...')
    await admin.disconnect()
    console.log('admin disconnected...')
}

init();