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

console.log('🔍 Starting MQTT Diagnostic Test...');
console.log(`📡 Connecting to: ${options.host}`);

const client = mqtt.connect(options);

client.on('connect', () => {
    console.log('✅ Connected to HiveMQ Cloud');
    client.subscribe('#', (err) => {
        if (err) console.error('❌ Subscription error:', err);
        else console.log('📥 Subscribed to ALL topics (#). Waiting for messages...');
    });
});

client.on('message', (topic, message) => {
    console.log(`\n[MESSAGE RECEIVED] @ ${new Date().toLocaleTimeString()}`);
    console.log(`Topic: ${topic}`);
    console.log(`Payload: ${message.toString()}`);
});

client.on('error', (err) => {
    console.error('❌ MQTT Error:', err);
});

// Heartbeat to show script is running
setInterval(() => {
    console.log(`[${new Date().toLocaleTimeString()}] Listening...`);
}, 5000);
