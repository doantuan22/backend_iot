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
    console.error('ERROR: Missing SUPABASE_URL or SUPABASE_KEY in .env');
}

const supabase = createClient(supabaseUrl, supabaseKey);

// --- MQTT CONFIGURATION ---
const SENSOR_TOPIC_FILTER = process.env.MQTT_SENSOR_TOPIC || 'wokwi/sensors/#';
const SENSOR_DATA_TOPIC = process.env.MQTT_SENSOR_DATA_TOPIC || 'wokwi/sensors/data';
const COMMAND_TOPIC = process.env.MQTT_COMMAND_TOPIC || 'wokwi/sensors/commands';

const mqttOptions = {
    host: process.env.MQTT_BROKER || 'broker.hivemq.com',
    port: Number(process.env.MQTT_PORT || 8883),
    protocol: (process.env.MQTT_BROKER && process.env.MQTT_BROKER.includes('hivemq.cloud')) ? 'mqtts' : 'mqtt',
    username: process.env.MQTT_USERNAME,
    password: process.env.MQTT_PASSWORD,
    clientId: process.env.MQTT_CLIENT_ID || ('backend_' + Math.random().toString(16).substr(2, 8)),
    rejectUnauthorized: false
};

const mqttClient = mqtt.connect(mqttOptions);

let latestSensorData = {
    temperature: null,
    humidity: null,
    gas: null,
    fire: null,
    alert: null,
    thresholds: {
        temperature: null,
        humidity: null,
        gas: null
    },
    raw: null,
    topic: null,
    updatedAt: null
};

function firstDefined(...values) {
    return values.find((value) => value !== undefined && value !== null);
}

function toNumber(value) {
    if (value === undefined || value === null || value === '') return null;
    const number = Number(value);
    return Number.isFinite(number) ? number : null;
}

function normalizeSensorPayload(data) {
    return {
        temperature: toNumber(firstDefined(data.temp, data.temperature, data.t)),
        humidity: toNumber(firstDefined(data.humi, data.humidity, data.hum, data.h)),
        gas: toNumber(firstDefined(data.gas, data.gasValue, data.gas_value)),
        fire: firstDefined(data.fire, data.flame, data.isFire),
        alert: firstDefined(data.alert),
        thresholds: {
            temperature: toNumber(firstDefined(data.th_temp, data.temp_threshold, data.tempThreshold)),
            humidity: toNumber(firstDefined(data.th_humidity, data.th_hum, data.humidity_threshold, data.humidityThreshold)),
            gas: toNumber(firstDefined(data.th_gas, data.gas_threshold, data.gasThreshold))
        },
        raw: data
    };
}

function buildControlPayload({ device, value, values }) {
    if (values && typeof values === 'object') {
        const payload = {};
        const temp = toNumber(firstDefined(values.temp, values.temperature, values.temp_threshold));
        const humidity = toNumber(firstDefined(values.humi, values.humidity, values.hum, values.humidity_threshold));
        const gas = toNumber(firstDefined(values.gas, values.gas_threshold));

        if (temp !== null) payload.set_temp = temp;
        if (humidity !== null) payload.set_humidity = humidity;
        if (gas !== null) payload.set_gas = Math.round(gas);
        return payload;
    }

    const numericValue = toNumber(value);
    if (numericValue === null) return null;

    if (device === 'gas_threshold') {
        return { set_gas: Math.round(numericValue) };
    }
    if (device === 'temp_threshold') {
        return { set_temp: numericValue };
    }
    if (device === 'humi_threshold' || device === 'humidity_threshold') {
        return { set_humidity: numericValue };
    }

    return null;
}

mqttClient.on('connect', () => {
    console.log('Connected to MQTT Broker');
    mqttClient.subscribe(SENSOR_TOPIC_FILTER, (err) => {
        if (err) {
            console.error('MQTT Subscribe Error:', err.message);
            return;
        }
        console.log(`Subscribed to ${SENSOR_TOPIC_FILTER}`);
    });
});

mqttClient.on('message', (topic, message) => {
    const text = message.toString();

    if (topic === SENSOR_DATA_TOPIC) {
        try {
            const data = JSON.parse(text);
            latestSensorData = {
                ...latestSensorData,
                ...normalizeSensorPayload(data),
                topic,
                updatedAt: new Date().toISOString()
            };
        } catch (err) {
            console.error('Invalid sensor payload:', err.message);
        }
    }

    if (topic.includes('alert')) {
        console.log(`ALERT received on ${topic}: ${text}`);
    }
});

mqttClient.on('error', (err) => {
    console.error('MQTT Error:', err.message);
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
        console.error('Supabase Error:', err.message);
        res.status(500).json({ error: err.message });
    }
});

// 2. Latest sensor data received by the backend MQTT client
app.get('/api/sensors/latest', (req, res) => {
    res.json(latestSensorData);
});

// 3. Control & Threshold Settings
app.post('/api/control', (req, res) => {
    const payloadObj = buildControlPayload(req.body);

    if (!payloadObj || Object.keys(payloadObj).length === 0) {
        return res.status(400).json({ error: 'Invalid or unsupported threshold command' });
    }

    if (!mqttClient.connected) {
        return res.status(503).json({ error: 'MQTT client is not connected' });
    }

    const payload = JSON.stringify(payloadObj);

    mqttClient.publish(COMMAND_TOPIC, payload, { qos: 1 }, (err) => {
        if (err) {
            console.error('MQTT Publish Error:', err.message);
            return res.status(500).json({ error: err.message });
        }
        console.log(`Sent command to ${COMMAND_TOPIC}: ${payload}`);
        res.json({ status: 'success', topic: COMMAND_TOPIC, sent: payloadObj });
    });
});

// 4. Health Check
app.get('/api/health', (req, res) => {
    res.json({
        status: 'online',
        mqtt: mqttClient.connected,
        supabase: !!supabase,
        latestSensorData,
        uptime: process.uptime()
    });
});

app.listen(PORT, () => {
    console.log(`IoT Backend running at http://localhost:${PORT}`);
});
