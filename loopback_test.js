const mqtt = require('mqtt');
require('dotenv').config({ path: '.env' });

const options = {
    host: process.env.MQTT_BROKER,
    port: 8883,
    protocol: 'mqtts',
    username: process.env.MQTT_USERNAME,
    password: process.env.MQTT_PASSWORD,
    rejectUnauthorized: false
};

const client = mqtt.connect(options);

client.on('connect', () => {
    console.log('✅ Connected. Testing loopback...');
    client.subscribe('test/loopback', () => {
        client.publish('test/loopback', 'Hello from Test Script');
    });
});

client.on('message', (topic, message) => {
    console.log(`✅ Loopback SUCCESS! Received on ${topic}: ${message.toString()}`);
    process.exit(0);
});

client.on('error', (err) => {
    console.error('❌ Error:', err);
    process.exit(1);
});

setTimeout(() => {
    console.log('❌ Loopback TIMEOUT');
    process.exit(1);
}, 10000);
