const amqp = require('amqplib');
const MongoLogger = require('./mongo');
let queue = 'requests';
// Set your config here...
let config = {
    protocol: 'amqp',
    hostname: 'localhost',
    port: 5672,
    username: 'rumesh',
    password: 'password',
    locale: 'en_US',
    frameMax: 0,
    heartbeat: 0,
    vhost: '/',
};


async function start() {
    try {
        const conn = await createConnection(config);
        let queue = 'requests';
        let exchange = 'test-exchange';
        let key = 'python-key';
        let exchange_type = 'direct';
        console.log("Connected to AMQP server.");
        let channel = await conn.createChannel();
        await channel.assertExchange(exchange, exchange_type, {durable: true});
        await channel.assertQueue(queue, { durable: true});

        startPollingForMessages(channel);
        //startSendingMessages(channel);
    } catch (err) {
        console.error("start: Connection error:",err.message);
    }
}

async function createConnection(config) {
    const conn = await amqp.connect(config);

    conn.on("error", function(err) {
        console.error("Connection error:",err.message);
    });

    conn.on("close", function() {
        console.error("Connection closed:", err.message);
    });

    return conn;
}

function startSendingMessages(channel) {
    const SEND_INTERVAL = 5000;
    setInterval(() => {
        sendMessage(channel, queue, JSON.stringify({ timestamp: new Date().toISOString(), message: " Some message" }));
    }, SEND_INTERVAL);
}

async function sendMessage(channel, queue, messageContent) {
    console.log(`sendMessage: sending message: ${messageContent}...`);
    return channel.sendToQueue(queue, Buffer.from(messageContent))
}

async function startPollingForMessages(ch) {
    ch.consume(queue, async (msg) => {
        await onNewMessage(msg);
        ch.ack(msg);
    });
}

async function onNewMessage(msg) {
    // Do your database stuff or whatever here....
    let m = msg.content.toString();
    console.log("On new message:", m);
    // let logger = new MongoLogger();
    // logger.sendMessage(m).then(()=>{
    //     //console.log("Msg sent to db")
    // })
    console.log(m);
}

start();