const express = require('express');
const cors = require('cors');
const mqtt = require('mqtt');
const { createClient } = require('@supabase/supabase-js');
require('dotenv').config();

const app = express();
app.use(cors());
app.use(express.json());

const PORT = process.env.PORT || 3001;

const supabaseUrl = process.env.SUPABASE_URL;
const supabaseKey = process.env.SUPABASE_KEY;
const supabase = supabaseUrl && supabaseKey ? createClient(supabaseUrl, supabaseKey) : null;

if (!supabase) {
    console.error('ERROR: Missing SUPABASE_URL or SUPABASE_KEY in .env');
}

const SENSOR_TOPIC_FILTER = process.env.MQTT_SENSOR_TOPIC || 'wokwi/sensors/#';
const SENSOR_DATA_TOPIC = process.env.MQTT_SENSOR_DATA_TOPIC || 'wokwi/sensors/data';
const COMMAND_TOPIC = process.env.MQTT_COMMAND_TOPIC || 'wokwi/sensors/commands';

const mqttOptions = {
    host: process.env.MQTT_BROKER || 'broker.hivemq.com',
    port: Number(process.env.MQTT_PORT || 8883),
    protocol: (process.env.MQTT_BROKER && process.env.MQTT_BROKER.includes('hivemq.cloud')) ? 'mqtts' : 'mqtt',
    username: process.env.MQTT_USERNAME,
    password: process.env.MQTT_PASSWORD,
    clientId: process.env.MQTT_CLIENT_ID || ('backend_' + Math.random().toString(16).slice(2, 10)),
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

let latestAlert = null;
const alertHistory = [];

let latestSupabaseStatus = {
    connected: false,
    error: supabase ? null : 'Thiếu SUPABASE_URL hoặc SUPABASE_KEY',
    checkedAt: null
};
let lastSupabaseCheckMs = 0;

const INACTIVE_ALERT_VALUES = new Set(['0', 'false', 'no', 'none', 'ok', 'safe', 'normal', 'off']);
const ACTIVE_ALERT_VALUES = new Set(['1', 'true', 'yes', 'danger', 'alert', 'warning', 'fire', 'gas', 'temp', 'temperature', 'humidity']);

function firstDefined(...values) {
    return values.find((value) => value !== undefined && value !== null);
}

function toNumber(value) {
    if (value === undefined || value === null || value === '') return null;
    const number = Number(value);
    return Number.isFinite(number) ? number : null;
}

function normalizeSensorPayload(data = {}) {
    return {
        temperature: toNumber(firstDefined(data.temp, data.temperature, data.t)),
        humidity: toNumber(firstDefined(data.humi, data.humidity, data.hum, data.h)),
        gas: toNumber(firstDefined(data.gas, data.gasValue, data.gas_value)),
        fire: firstDefined(data.fire, data.flame, data.isFire),
        alert: firstDefined(data.alert, data.warning, data.alarm),
        thresholds: {
            temperature: toNumber(firstDefined(data.th_temp, data.temp_threshold, data.tempThreshold)),
            humidity: toNumber(firstDefined(data.th_humidity, data.th_hum, data.humidity_threshold, data.humidityThreshold)),
            gas: toNumber(firstDefined(data.th_gas, data.gas_threshold, data.gasThreshold))
        },
        raw: data
    };
}

function alertText(value) {
    return String(value || '').trim().toLowerCase();
}

function hasAlertValue(value) {
    return value !== undefined && value !== null && String(value).trim() !== '';
}

function isInactiveAlertValue(value) {
    return hasAlertValue(value) && INACTIVE_ALERT_VALUES.has(alertText(value));
}

function isActiveAlertValue(value) {
    if (!hasAlertValue(value)) return false;
    const text = alertText(value);
    return ACTIVE_ALERT_VALUES.has(text) || !INACTIVE_ALERT_VALUES.has(text);
}

function isTruthy(value) {
    return value === true || value === 1 || ACTIVE_ALERT_VALUES.has(alertText(value));
}

function addIssue(issues, type, message) {
    issues.push({ type, message });
}

function buildAlertFromPayload(topic, data, rawData) {
    const issues = [];
    const thresholds = data.thresholds || {};
    const hasExplicitAlert = hasAlertValue(data.alert);
    const alertIsActive = isActiveAlertValue(data.alert);

    if (hasExplicitAlert && !alertIsActive) return null;

    if (isTruthy(data.fire)) {
        addIssue(issues, 'fire', 'Phát hiện lửa hoặc tín hiệu cháy');
    }
    if (data.temperature !== null && thresholds.temperature !== null && data.temperature >= thresholds.temperature) {
        addIssue(issues, 'temp', `Nhiệt độ ${data.temperature}°C vượt ngưỡng ${thresholds.temperature}°C`);
    }
    if (data.gas !== null && thresholds.gas !== null && data.gas >= thresholds.gas) {
        addIssue(issues, 'gas', `Gas ${Math.round(data.gas)} ppm vượt ngưỡng ${Math.round(thresholds.gas)} ppm`);
    }
    if (data.humidity !== null && thresholds.humidity !== null && data.humidity >= thresholds.humidity) {
        addIssue(issues, 'humi', `Độ ẩm ${data.humidity}% vượt ngưỡng ${thresholds.humidity}%`);
    }
    if (alertIsActive) {
        addIssue(issues, 'sensor', `Wokwi gửi cảnh báo: ${data.alert}`);
    }
    if (topic.includes('alert') && issues.length === 0) {
        addIssue(issues, 'sensor', `Có cảnh báo từ topic ${topic}`);
    }

    if (issues.length === 0) return null;

    return {
        topic,
        message: issues.map((issue) => issue.message).join('. '),
        issues,
        raw: rawData,
        updatedAt: new Date().toISOString()
    };
}

function rememberAlert(alert) {
    if (!alert) return;
    latestAlert = alert;
    alertHistory.unshift(alert);
    if (alertHistory.length > 50) alertHistory.pop();
    console.log(`ALERT received on ${alert.topic}: ${alert.message}`);
}

function clearLatestAlertIfNormal(data) {
    if (isInactiveAlertValue(data.alert)) {
        latestAlert = null;
    }
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

    if (device === 'gas_threshold') return { set_gas: Math.round(numericValue) };
    if (device === 'temp_threshold') return { set_temp: numericValue };
    if (device === 'humi_threshold' || device === 'humidity_threshold') return { set_humidity: numericValue };

    return null;
}

async function checkSupabaseStatus(force = false) {
    if (!supabase) {
        latestSupabaseStatus = {
            connected: false,
            error: 'Thiếu SUPABASE_URL hoặc SUPABASE_KEY',
            checkedAt: new Date().toISOString()
        };
        return latestSupabaseStatus;
    }

    const now = Date.now();
    if (!force && latestSupabaseStatus.checkedAt && now - lastSupabaseCheckMs < 10000) {
        return latestSupabaseStatus;
    }

    lastSupabaseCheckMs = now;
    try {
        const { error } = await supabase
            .from('stroke_events')
            .select('*', { count: 'exact', head: true });

        if (error) throw error;

        latestSupabaseStatus = {
            connected: true,
            error: null,
            checkedAt: new Date().toISOString()
        };
    } catch (err) {
        latestSupabaseStatus = {
            connected: false,
            error: err.message,
            checkedAt: new Date().toISOString()
        };
        console.error('Supabase Health Error:', err.message);
    }

    return latestSupabaseStatus;
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
            const payload = JSON.parse(text);
            const normalizedData = normalizeSensorPayload(payload);
            latestSensorData = {
                ...latestSensorData,
                ...normalizedData,
                topic,
                updatedAt: new Date().toISOString()
            };

            const alert = buildAlertFromPayload(topic, normalizedData, payload);
            if (alert) {
                rememberAlert(alert);
            } else {
                clearLatestAlertIfNormal(normalizedData);
            }
        } catch (err) {
            console.error('Invalid sensor payload:', err.message);
        }
        return;
    }

    if (topic.includes('alert')) {
        try {
            const payload = JSON.parse(text);
            const normalizedData = normalizeSensorPayload(payload);
            const alert = buildAlertFromPayload(topic, normalizedData, payload);
            if (alert) {
                rememberAlert(alert);
            } else {
                clearLatestAlertIfNormal(normalizedData);
            }
        } catch (err) {
            rememberAlert({
                topic,
                message: text,
                issues: [{ type: 'sensor', message: text }],
                raw: text,
                updatedAt: new Date().toISOString()
            });
        }
    }
});

mqttClient.on('error', (err) => {
    console.error('MQTT Error:', err.message);
});

app.get('/api/images', async (req, res) => {
    res.set('Cache-Control', 'no-store, no-cache, must-revalidate, proxy-revalidate');

    if (!supabase) {
        return res.status(500).json({ error: 'Supabase chưa được cấu hình' });
    }

    try {
        const { data, error } = await supabase
            .from('stroke_events')
            .select('*')
            .order('timestamp', { ascending: false, nullsFirst: false })
            .order('id', { ascending: false })
            .limit(11);

        if (error) throw error;

        latestSupabaseStatus = {
            connected: true,
            error: null,
            checkedAt: new Date().toISOString()
        };
        res.json(data);
    } catch (err) {
        latestSupabaseStatus = {
            connected: false,
            error: err.message,
            checkedAt: new Date().toISOString()
        };
        console.error('Supabase Error:', err.message);
        res.status(500).json({ error: err.message });
    }
});

app.get('/api/sensors/latest', (req, res) => {
    res.json(latestSensorData);
});

app.get('/api/alerts/latest', (req, res) => {
    res.json({ latestAlert, alertHistory });
});

app.get('/api/supabase/health', async (req, res) => {
    res.json(await checkSupabaseStatus(true));
});

app.post('/api/control', (req, res) => {
    const payloadObj = buildControlPayload(req.body);

    if (!payloadObj || Object.keys(payloadObj).length === 0) {
        return res.status(400).json({ error: 'Lệnh ngưỡng không hợp lệ hoặc chưa được hỗ trợ' });
    }

    if (!mqttClient.connected) {
        return res.status(503).json({ error: 'MQTT client chưa kết nối' });
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

app.get('/api/health', async (req, res) => {
    const supabaseStatus = await checkSupabaseStatus();
    res.json({
        status: 'online',
        mqtt: mqttClient.connected,
        supabase: supabaseStatus.connected,
        supabaseStatus,
        latestSensorData,
        latestAlert,
        uptime: process.uptime()
    });
});

app.listen(PORT, () => {
    console.log(`IoT Backend running at http://localhost:${PORT}`);
});
