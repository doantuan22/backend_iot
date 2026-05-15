const express = require('express');
const cors = require('cors');
const mqtt = require('mqtt');
const { createClient } = require('@supabase/supabase-js');
require('dotenv').config();

const app = express();
app.use(cors());
app.use(express.json());

const PORT = process.env.PORT || 3001;

// --- SUPABASE CONFIGURATION ---
const supabaseUrl = process.env.SUPABASE_URL;
const supabaseKey = process.env.SUPABASE_KEY;

if (!supabaseUrl || !supabaseKey) {
    console.error('❌ ERROR: Missing SUPABASE_URL or SUPABASE_KEY in .env');
}

const supabase = createClient(supabaseUrl, supabaseKey);

// --- MQTT CONFIGURATION ---
const mqttOptions = {
    host: process.env.MQTT_BROKER || 'broker.hivemq.com',
    port: process.env.MQTT_PORT || 8883,
    protocol: (process.env.MQTT_BROKER && process.env.MQTT_BROKER.includes('hivemq.cloud')) ? 'mqtts' : 'mqtt',
    username: process.env.MQTT_USERNAME,
    password: process.env.MQTT_PASSWORD,
    clientId: 'backend_' + Math.random().toString(16).substr(2, 8),
    rejectUnauthorized: false
};

const mqttClient = mqtt.connect(mqttOptions);

mqttClient.on('connect', () => {
    console.log('📡 Connected to MQTT Broker');
    mqttClient.subscribe('wokwi/sensors/#', (err) => {
        if (!err) console.log('📥 Subscribed to wokwi/sensors/#');
    });
});

mqttClient.on('message', (topic, message) => {
    // Optional: Log important messages
    if (topic.includes('alert')) {
        console.log(`⚠️ ALERT received on ${topic}: ${message.toString()}`);
    }
});

mqttClient.on('error', (err) => {
    console.error('❌ MQTT Error:', err.message);
});

// --- API ROUTES ---

// 1. Get images from Supabase (Limit 11 to have 1 latest + 10 history)
app.get('/api/images', async (req, res) => {
    try {
        const { data, error } = await supabase
            .from('stroke_events')
            .select('*')
            .order('timestamp', { ascending: false })
            .limit(11);

        if (error) throw error;
        res.json(data);
    } catch (err) {
        console.error('❌ Supabase Error:', err.message);
        res.status(500).json({ error: err.message });
    }
});

// 2. Control & Threshold Settings
app.post('/api/control', (req, res) => {
    const { device, action, value } = req.body;
    const topic = `wokwi/sensors/commands`;
    
    let payloadObj = {};
    
    // Explicitly handle threshold settings
    if (device === 'gas_threshold') {
        payloadObj = { command: 'set_gas_threshold', value: parseInt(value) };
    } else if (device === 'temp_threshold') {
        payloadObj = { command: 'set_temp_threshold', value: parseFloat(value) };
    } else {
        // Handle normal controls (LED, Fan, etc.)
        payloadObj = { device, action, value };
    }

    const payload = JSON.stringify(payloadObj);

    mqttClient.publish(topic, payload, { qos: 1 }, (err) => {
        if (err) {
            console.error('❌ MQTT Publish Error:', err.message);
            return res.status(500).json({ error: err.message });
        }
        console.log(`📤 Sent command: ${payload}`);
        res.json({ status: 'success', sent: payloadObj });
    });
});

// 3. Health Check
app.get('/api/health', (req, res) => {
    res.json({ 
        status: 'online', 
        mqtt: mqttClient.connected,
        supabase: !!supabase,
        uptime: process.uptime()
    });
});

app.listen(PORT, () => {
    console.log(`🚀 Professional IoT Backend running at http://localhost:${PORT}`);
});

