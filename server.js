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
const supabaseKey = process.env.SUPABASE_SERVICE_ROLE_KEY || process.env.SUPABASE_KEY;
const usesSupabaseServiceRole = !!process.env.SUPABASE_SERVICE_ROLE_KEY;
const supabase = supabaseUrl && supabaseKey ? createClient(supabaseUrl, supabaseKey) : null;

if (!supabase) {
    console.error('ERROR: Missing SUPABASE_URL or SUPABASE_KEY in .env');
} else if (!usesSupabaseServiceRole) {
    console.warn('WARNING: SUPABASE_SERVICE_ROLE_KEY is not set. Inserts/deletes can fail when Supabase RLS is enabled.');
}

const SENSOR_TOPIC_FILTER = process.env.MQTT_SENSOR_TOPIC || 'wokwi/sensors/#';
const SENSOR_DATA_TOPIC = process.env.MQTT_SENSOR_DATA_TOPIC || 'wokwi/sensors/data';
const COMMAND_TOPIC = process.env.MQTT_COMMAND_TOPIC || 'wokwi/sensors/commands';
const SENSOR_DATA_TABLE = process.env.SENSOR_DATA_TABLE || 'sensor_data';
const SENSOR_PERSIST_INTERVAL_MS = Number(process.env.SENSOR_PERSIST_INTERVAL_MS || 5 * 60 * 1000);
const STROKE_EVENTS_TABLE = process.env.STROKE_EVENTS_TABLE || 'stroke_events';
const SURVEILLANCE_BUCKET = process.env.SURVEILLANCE_BUCKET || 'surveillance-images';
const DATABASE_TABLES = {
    stroke_event: { table: STROKE_EVENTS_TABLE, timeColumn: 'timestamp', storageBucket: SURVEILLANCE_BUCKET },
    stroke_events: { table: STROKE_EVENTS_TABLE, timeColumn: 'timestamp', storageBucket: SURVEILLANCE_BUCKET },
    sensor_data: { table: SENSOR_DATA_TABLE, timeColumn: 'created_at' }
};

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
let lastSensorPersistMs = 0;
let sensorPersistInFlight = false;
let latestSensorPersistStatus = {
    saved: false,
    skipped: false,
    reason: 'No MQTT sensor data yet',
    rows: 0,
    lastSavedAt: null,
    checkedAt: null,
    error: null
};

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

function formatNumber(value, suffix = '') {
    if (value === null || value === undefined) return '';
    return `${Number.isInteger(value) ? value : Number(value.toFixed(1))}${suffix}`;
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

function buildWarningForSensor(sensorName, data) {
    const warnings = [];
    const thresholds = data.thresholds || {};

    if (isTruthy(data.fire)) {
        warnings.push('Phat hien lua hoac tin hieu chay');
    }

    if (sensorName === 'temperature' && data.temperature !== null && thresholds.temperature !== null && data.temperature >= thresholds.temperature) {
        warnings.push(`Nhiet do ${formatNumber(data.temperature, 'C')} vuot nguong ${formatNumber(thresholds.temperature, 'C')}`);
    }

    if (sensorName === 'humidity' && data.humidity !== null && thresholds.humidity !== null && data.humidity >= thresholds.humidity) {
        warnings.push(`Do am ${formatNumber(data.humidity, '%')} vuot nguong ${formatNumber(thresholds.humidity, '%')}`);
    }

    if (sensorName === 'gas' && data.gas !== null && thresholds.gas !== null && data.gas >= thresholds.gas) {
        warnings.push(`Gas ${Math.round(data.gas)} ppm vuot nguong ${Math.round(thresholds.gas)} ppm`);
    }

    if (isActiveAlertValue(data.alert)) {
        warnings.push(`Wokwi gui canh bao: ${data.alert}`);
    }

    return warnings.length > 0 ? warnings.join('. ') : null;
}

function buildSensorDataRows(data) {
    const fireDetected = isTruthy(data.fire);

    return [
        {
            sensor_name: 'temperature',
            temperature: data.temperature,
            humidity: null,
            gas_value: null,
            fire_detected: fireDetected,
            warning: buildWarningForSensor('temperature', data)
        },
        {
            sensor_name: 'humidity',
            temperature: null,
            humidity: data.humidity,
            gas_value: null,
            fire_detected: fireDetected,
            warning: buildWarningForSensor('humidity', data)
        },
        {
            sensor_name: 'gas',
            temperature: null,
            humidity: null,
            gas_value: data.gas,
            fire_detected: fireDetected,
            warning: buildWarningForSensor('gas', data)
        }
    ];
}

function markSensorPersistStatus(update) {
    latestSensorPersistStatus = {
        ...latestSensorPersistStatus,
        ...update,
        checkedAt: new Date().toISOString()
    };
}

async function persistSensorDataIfDue(data) {
    if (!supabase) {
        markSensorPersistStatus({
            saved: false,
            skipped: true,
            reason: 'Supabase is not configured',
            rows: 0,
            error: 'Missing SUPABASE_URL or SUPABASE_KEY'
        });
        return latestSensorPersistStatus;
    }

    const now = Date.now();
    const elapsedMs = now - lastSensorPersistMs;

    if (sensorPersistInFlight) {
        markSensorPersistStatus({
            saved: false,
            skipped: true,
            reason: 'Previous sensor batch is still being saved',
            rows: 0,
            error: null
        });
        return latestSensorPersistStatus;
    }

    if (lastSensorPersistMs && elapsedMs < SENSOR_PERSIST_INTERVAL_MS) {
        markSensorPersistStatus({
            saved: false,
            skipped: true,
            reason: `Wait ${Math.ceil((SENSOR_PERSIST_INTERVAL_MS - elapsedMs) / 1000)} more seconds`,
            rows: 0,
            error: null
        });
        return latestSensorPersistStatus;
    }

    sensorPersistInFlight = true;
    try {
        const rows = buildSensorDataRows(data);
        const { error } = await supabase.from(SENSOR_DATA_TABLE).insert(rows);

        if (error) throw error;

        lastSensorPersistMs = Date.now();
        markSensorPersistStatus({
            saved: true,
            skipped: false,
            reason: null,
            rows: rows.length,
            lastSavedAt: new Date(lastSensorPersistMs).toISOString(),
            error: null
        });
        console.log(`Saved ${rows.length} sensor rows to ${SENSOR_DATA_TABLE}`);
    } catch (err) {
        markSensorPersistStatus({
            saved: false,
            skipped: false,
            reason: null,
            rows: 0,
            error: err.message
        });
        console.error('Sensor Data Supabase Error:', err.message);
    } finally {
        sensorPersistInFlight = false;
    }

    return latestSensorPersistStatus;
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

function getDatabaseConfig(tableName) {
    return DATABASE_TABLES[String(tableName || '').toLowerCase()] || null;
}

function applyDateFilters(query, timeColumn, start, end) {
    let nextQuery = query;
    if (start) nextQuery = nextQuery.gte(timeColumn, start);
    if (end) nextQuery = nextQuery.lte(timeColumn, end);
    return nextQuery;
}

function getStrokeImageUrl(row = {}) {
    return firstDefined(row.image_url, row.imageUrl, row.url, row.image, row.path, row.file_path, row.filePath);
}

function getStoragePathFromUrl(value) {
    if (!value) return null;

    const text = String(value).trim();
    if (!text) return null;

    if (!/^https?:\/\//i.test(text)) {
        return text.replace(/^\/+/, '');
    }

    try {
        const url = new URL(text);
        const decodedPath = decodeURIComponent(url.pathname);
        const publicMarker = `/storage/v1/object/public/${SURVEILLANCE_BUCKET}/`;
        const signedMarker = `/storage/v1/object/sign/${SURVEILLANCE_BUCKET}/`;
        const marker = decodedPath.includes(publicMarker) ? publicMarker : signedMarker;
        const index = decodedPath.indexOf(marker);

        if (index === -1) return null;
        return decodedPath.slice(index + marker.length).replace(/^\/+/, '');
    } catch (err) {
        return null;
    }
}

async function deleteStrokeEventImage(row) {
    const imagePath = getStoragePathFromUrl(getStrokeImageUrl(row));

    if (!imagePath) {
        return { deleted: false, path: null, reason: 'No storage path found on row' };
    }

    const { error } = await supabase.storage.from(SURVEILLANCE_BUCKET).remove([imagePath]);
    if (error) throw error;

    return { deleted: true, path: imagePath, reason: null };
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
            .from(SENSOR_DATA_TABLE)
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

            persistSensorDataIfDue(normalizedData);
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
            .from(STROKE_EVENTS_TABLE)
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
    res.json({
        ...latestSensorData,
        persistStatus: latestSensorPersistStatus
    });
});

app.get('/api/sensors/history', async (req, res) => {
    res.set('Cache-Control', 'no-store, no-cache, must-revalidate, proxy-revalidate');

    if (!supabase) {
        return res.status(500).json({ error: 'Supabase chua duoc cau hinh' });
    }

    const limit = Math.min(Math.max(Number(req.query.limit || 30), 1), 300);

    try {
        const { data, error } = await supabase
            .from(SENSOR_DATA_TABLE)
            .select('*')
            .order('created_at', { ascending: false })
            .order('id', { ascending: false })
            .limit(limit);

        if (error) throw error;

        res.json(data);
    } catch (err) {
        console.error('Sensor History Supabase Error:', err.message);
        res.status(500).json({ error: err.message });
    }
});

app.get('/api/database/:table', async (req, res) => {
    res.set('Cache-Control', 'no-store, no-cache, must-revalidate, proxy-revalidate');

    if (!supabase) {
        return res.status(500).json({ error: 'Supabase chua duoc cau hinh' });
    }

    const config = getDatabaseConfig(req.params.table);
    if (!config) {
        return res.status(400).json({ error: 'Bang du lieu khong duoc ho tro' });
    }

    const limit = Math.min(Math.max(Number(req.query.limit || 100), 1), 500);
    const { start, end } = req.query;

    try {
        let query = supabase
            .from(config.table)
            .select('*', { count: 'exact' })
            .order(config.timeColumn, { ascending: false, nullsFirst: false })
            .order('id', { ascending: false })
            .limit(limit);

        query = applyDateFilters(query, config.timeColumn, start, end);

        const { data, error, count } = await query;
        if (error) throw error;

        res.json({
            table: config.table,
            timeColumn: config.timeColumn,
            rows: data || [],
            count: count || 0
        });
    } catch (err) {
        console.error('Database Query Error:', err.message);
        res.status(500).json({ error: err.message });
    }
});

app.delete('/api/database/:table/:id', async (req, res) => {
    if (!supabase) {
        return res.status(500).json({ error: 'Supabase chua duoc cau hinh' });
    }

    const config = getDatabaseConfig(req.params.table);
    if (!config) {
        return res.status(400).json({ error: 'Bang du lieu khong duoc ho tro' });
    }

    const id = Number(req.params.id);
    if (!Number.isFinite(id)) {
        return res.status(400).json({ error: 'ID khong hop le' });
    }

    try {
        let storageResult = null;

        if (config.table === STROKE_EVENTS_TABLE) {
            const { data: row, error: fetchError } = await supabase
                .from(config.table)
                .select('*')
                .eq('id', id)
                .single();

            if (fetchError) throw fetchError;
            storageResult = await deleteStrokeEventImage(row);
        }

        const { data, error } = await supabase
            .from(config.table)
            .delete()
            .eq('id', id)
            .select('id');

        if (error) throw error;

        res.json({
            deleted: data?.length || 0,
            storage: storageResult
        });
    } catch (err) {
        console.error('Database Delete Error:', err.message);
        res.status(500).json({ error: err.message });
    }
});

app.delete('/api/database/sensor_data', async (req, res) => {
    if (!supabase) {
        return res.status(500).json({ error: 'Supabase chua duoc cau hinh' });
    }

    const { start, end } = req.body || {};
    if (!start && !end) {
        return res.status(400).json({ error: 'Can chon it nhat mot moc thoi gian de xoa sensor_data' });
    }

    try {
        let query = supabase
            .from(SENSOR_DATA_TABLE)
            .delete()
            .select('id');

        query = applyDateFilters(query, 'created_at', start, end);

        const { data, error } = await query;
        if (error) throw error;

        res.json({
            table: SENSOR_DATA_TABLE,
            deleted: data?.length || 0
        });
    } catch (err) {
        console.error('Sensor Bulk Delete Error:', err.message);
        res.status(500).json({ error: err.message });
    }
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
        latestSensorPersistStatus,
        latestAlert,
        uptime: process.uptime()
    });
});

app.listen(PORT, () => {
    console.log(`IoT Backend running at http://localhost:${PORT}`);
});
