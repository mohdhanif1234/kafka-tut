import { kafka } from './client.js'
import readline from 'readline'

const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout
})

const init = async () => {
    const producer = kafka.producer();

    console.log('connecting producer....')
    await producer.connect()
    console.log('producer connected successfully...')

    rl.setPrompt('> ')
    rl.prompt();
    rl.on('line', async (line) => {
        const [riderName, location] = line.split(' ')

        await producer.send({
            topic: 'rider-updates',
            messages: [
                {
                    partition: location.toLowerCase() === 'north' ? 0 : 1,
                    key: 'location-update',
                    value: JSON.stringify({ name: riderName, loc: location })
                }
            ]
        })
    }).on('close', async () => {
        await producer.disconnect();
    })

}

// setInterval(() => {
//     init();
// }, 2000);

init()