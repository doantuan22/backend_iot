const express = require('express');
const cors = require('cors');
const mqtt = require('mqtt');
const crypto = require('crypto');
const { createClient } = require('@supabase/supabase-js');
require('dotenv').config();

const app = express();
app.use(cors());
app.use(express.json());

app.get('/health', (req, res) => {
    res.status(200).send('OK');
});

const PORT = process.env.PORT || 3001;
const AUTH_SECRET = process.env.AUTH_SECRET || 'iot-demo-auth-secret';
const AUTH_TOKEN_TTL_MS = Number(process.env.AUTH_TOKEN_TTL_MS || 8 * 60 * 60 * 1000);
const authUsers = {
    admin: {
        username: process.env.ADMIN_USERNAME || 'admin',
        password: process.env.ADMIN_PASSWORD || 'admin123',
        role: 'admin'
    },
    user: {
        username: process.env.USER_USERNAME || 'user',
        password: process.env.USER_PASSWORD || 'user123',
        role: 'user'
    }
};

const supabaseUrl = process.env.SUPABASE_URL;
const supabaseKey = process.env.SUPABASE_SERVICE_ROLE_KEY || process.env.SUPABASE_KEY;
const usesSupabaseServiceRole = !!process.env.SUPABASE_SERVICE_ROLE_KEY;
const supabase = supabaseUrl && supabaseKey ? createClient(supabaseUrl, supabaseKey) : null;

if (!supabase) {
    console.error('ERROR: Missing SUPABASE_URL or SUPABASE_KEY in .env');
} else if (!usesSupabaseServiceRole) {
    console.warn('WARNING: SUPABASE_SERVICE_ROLE_KEY is not set. Inserts/deletes can fail when Supabase RLS is enabled.');
}

function normalizeMqttTopicFilter(value, fallback) {
    const topic = String(value || fallback || '').trim();
    if (!topic) return fallback;
    if (topic.includes('#') || topic.includes('+')) return topic;
    if (topic.endsWith('/')) return `${topic}#`;
    return topic;
}

function normalizeMqttPublishTopic(value, fallback) {
    const topic = String(value || fallback || '').trim();
    if (!topic || topic.includes('#') || topic.includes('+')) return fallback;
    return topic;
}

const SENSOR_TOPIC_FILTER = normalizeMqttTopicFilter(process.env.MQTT_SENSOR_TOPIC, 'wokwi/sensors/#');
const SENSOR_DATA_TOPIC = process.env.MQTT_SENSOR_DATA_TOPIC || 'wokwi/sensors/data';
const COMMAND_TOPIC = normalizeMqttPublishTopic(process.env.MQTT_COMMAND_TOPIC, 'wokwi/sensors/commands');
const SENSOR_DATA_TABLE = process.env.SENSOR_DATA_TABLE || 'sensor_data';
const SENSOR_PERSIST_INTERVAL_MS = Number(process.env.SENSOR_PERSIST_INTERVAL_MS || 5 * 60 * 1000);
const SUPABASE_HEALTH_TIMEOUT_MS = Number(process.env.SUPABASE_HEALTH_TIMEOUT_MS || 3000);
const STROKE_EVENTS_TABLE = process.env.STROKE_EVENTS_TABLE || 'stroke_events';
const AIRPORT_EVENTS_TABLE = process.env.AIRPORT_EVENTS_TABLE || 'airport_events';
const BAGGAGE_TRACKS_TABLE = process.env.BAGGAGE_TRACKS_TABLE || 'baggage_tracks';
const SURVEILLANCE_BUCKET = process.env.SURVEILLANCE_BUCKET || 'surveillance-images';
const AI_EVENT_TABLE_CONFIGS = [
    {
        key: 'stroke_events',
        aliases: ['stroke_event', 'stroke_events', 'stroke'],
        table: STROKE_EVENTS_TABLE,
        label: 'Stroke_event',
        timeColumns: ['timestamp', 'created_at'],
        statsTimeColumns: ['timestamp', 'created_at'],
        idColumn: 'id',
        storageBucket: SURVEILLANCE_BUCKET,
        imageSource: true,
        statsMode: 'event_rows'
    },
    {
        key: 'airport_events',
        aliases: ['airport_event', 'airport_events'],
        table: AIRPORT_EVENTS_TABLE,
        label: 'airport_events',
        timeColumns: ['created_at'],
        statsTimeColumns: ['created_at'],
        idColumn: 'id',
        storageBucket: SURVEILLANCE_BUCKET,
        imageSource: true,
        statsMode: 'event_rows'
    },
    {
        key: 'baggage_tracks',
        aliases: ['baggage_track', 'baggage_tracks'],
        table: BAGGAGE_TRACKS_TABLE,
        label: 'baggage_tracks',
        timeColumns: ['updated_at', 'last_seen_at', 'owner_gone_at', 'first_seen_at'],
        statsTimeColumns: ['owner_gone_at'],
        idColumn: 'track_id',
        imageSource: false,
        statsMode: 'alerted_tracks'
    }
].map((config) => ({
    ...config,
    timeColumn: config.timeColumns[0],
    isAiEvent: true
}));
const DATABASE_TABLES = AI_EVENT_TABLE_CONFIGS.reduce((tables, config) => {
    for (const alias of config.aliases) {
        tables[alias] = config;
    }
    return tables;
}, {
    sensor_data: { table: SENSOR_DATA_TABLE, label: 'sensor_data', timeColumn: 'created_at', idColumn: 'id' }
});

const mqttOptions = {
    host: process.env.MQTT_BROKER || 'broker.hivemq.com',
    port: Number(process.env.MQTT_PORT || 8883),
    protocol: process.env.MQTT_PROTOCOL || ((process.env.MQTT_BROKER && process.env.MQTT_BROKER.includes('hivemq.cloud')) ? 'mqtts' : 'mqtt'),
    username: process.env.MQTT_USERNAME,
    password: process.env.MQTT_PASSWORD,
    clientId: process.env.MQTT_CLIENT_ID || ('backend_' + Math.random().toString(16).slice(2, 10)),
    rejectUnauthorized: false,
    keepalive: 60,
    reconnectPeriod: 3000,
    connectTimeout: 30000,
    clean: true,
    resubscribe: true
};

const mqttRuntimeConfig = {
    host: mqttOptions.host,
    port: mqttOptions.port,
    protocol: mqttOptions.protocol,
    usernameConfigured: !!mqttOptions.username,
    sensorTopicFilter: SENSOR_TOPIC_FILTER,
    sensorDataTopic: SENSOR_DATA_TOPIC,
    commandTopic: COMMAND_TOPIC
};

const mqttClient = mqtt.connect(mqttOptions);

function base64urlEncode(value) {
    return Buffer.from(value).toString('base64url');
}

function base64urlJson(value) {
    return base64urlEncode(JSON.stringify(value));
}

function signTokenPayload(payloadPart) {
    return crypto.createHmac('sha256', AUTH_SECRET).update(payloadPart).digest('base64url');
}

function createAuthToken(user) {
    const payload = {
        username: user.username,
        role: user.role,
        exp: Date.now() + AUTH_TOKEN_TTL_MS
    };
    const payloadPart = base64urlJson(payload);
    return `${payloadPart}.${signTokenPayload(payloadPart)}`;
}

function verifyAuthToken(token) {
    const [payloadPart, signature] = String(token || '').split('.');
    if (!payloadPart || !signature) return null;

    const expectedSignature = signTokenPayload(payloadPart);
    const signatureBuffer = Buffer.from(signature);
    const expectedBuffer = Buffer.from(expectedSignature);
    if (signatureBuffer.length !== expectedBuffer.length || !crypto.timingSafeEqual(signatureBuffer, expectedBuffer)) {
        return null;
    }

    try {
        const payload = JSON.parse(Buffer.from(payloadPart, 'base64url').toString('utf8'));
        if (!payload.exp || Date.now() > payload.exp) return null;
        if (!['admin', 'user'].includes(payload.role)) return null;
        return { username: payload.username, role: payload.role };
    } catch (err) {
        return null;
    }
}

function getAuthUser(req) {
    const header = req.get('Authorization') || '';
    const match = header.match(/^Bearer\s+(.+)$/i);
    return match ? verifyAuthToken(match[1]) : null;
}

function requireAuth(req, res, next) {
    const user = getAuthUser(req);
    if (!user) {
        return res.status(401).json({ error: 'Cần đăng nhập để tiếp tục' });
    }
    req.authUser = user;
    next();
}

function requireAdmin(req, res, next) {
    requireAuth(req, res, () => {
        if (req.authUser.role !== 'admin') {
            return res.status(403).json({ error: 'Tài khoản user không có quyền truy cập database' });
        }
        next();
    });
}

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
let sensorPersistQueue = Promise.resolve();
let lastPersistedAlertKey = '';
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
const VIETNAM_TIME_OFFSET_MS = 7 * 60 * 60 * 1000;

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

function vietnamTimestamp(date = new Date()) {
    return new Date(date.getTime() + VIETNAM_TIME_OFFSET_MS).toISOString().replace('Z', '');
}

function vietnamRangeTimestamp(date) {
    return vietnamTimestamp(date);
}

function parseVietnamTimestamp(value) {
    if (value instanceof Date) return value.getTime();
    if (value === undefined || value === null || value === '') return NaN;
    if (typeof value === 'number') return value < 10000000000 ? value * 1000 : value;

    const raw = String(value).trim();
    if (!raw) return NaN;
    if (/^\d+$/.test(raw)) {
        const number = Number(raw);
        return number < 10000000000 ? number * 1000 : number;
    }

    const normalized = raw.includes('T') ? raw : raw.replace(' ', 'T');
    const hasTimezone = /(?:Z|[+-]\d{2}:?\d{2})$/i.test(normalized);
    const vietnamTime = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}/.test(normalized) && !hasTimezone
        ? `${normalized}+07:00`
        : raw;

    return Date.parse(vietnamTime);
}

function getAiEventTime(row = {}, config = null) {
    const configuredColumns = config?.timeColumns || [];
    return firstDefined(
        ...configuredColumns.map((column) => row[column]),
        row.ai_timestamp,
        row.timestamp,
        row.created_at,
        row.createdAt,
        row.updated_at,
        row.updatedAt,
        row.last_seen_at,
        row.lastSeenAt,
        row.owner_gone_at,
        row.ownerGoneAt,
        row.first_seen_at,
        row.firstSeenAt
    );
}

function getAiEventId(row = {}, config = null) {
    return firstDefined(row[config?.idColumn], row.id, row.track_id, row.trackId);
}

function aiEventSortTime(row = {}) {
    const time = parseVietnamTimestamp(getAiEventTime(row));
    return Number.isFinite(time) ? time : 0;
}

function aiEventSortId(row = {}) {
    const id = Number(getAiEventId(row));
    return Number.isFinite(id) ? id : 0;
}

function aiEventDedupeKey(row = {}) {
    return [
        row.source_table || row.ai_table || 'ai_event',
        firstDefined(getAiEventId(row), getAiEventImageUrl(row), JSON.stringify(row))
    ].join(':');
}

function dedupeAiEvents(rows = []) {
    const map = new Map();
    for (const row of rows) {
        const key = aiEventDedupeKey(row);
        if (!map.has(key)) map.set(key, row);
    }
    return [...map.values()];
}

function normalizeAiEventRow(row = {}, config, timeColumn = '') {
    const normalized = {
        ...row,
        ai_timestamp: firstDefined(row.ai_timestamp, row[timeColumn], getAiEventTime(row, config)),
        source_key: config.key,
        source_table: config.table,
        source_label: config.label,
        ai_table: config.table
    };
    if (normalized.id === undefined || normalized.id === null || normalized.id === '') {
        normalized.id = getAiEventId(row, config);
    }
    return normalized;
}

function hasAiEventImage(row = {}) {
    return !!getAiEventImageUrl(row);
}

function isAiEventStatsRow(row = {}, config = {}) {
    if (config.statsMode === 'event_rows') return true;
    if (config.statsMode === 'alerted_tracks') {
        return row.alerted === true || String(row.alerted).toLowerCase() === 'true';
    }
    return hasAiEventImage(row);
}

function strokeEventSortTime(row = {}) {
    return aiEventSortTime(row);
}

function strokeEventSortId(row = {}) {
    return aiEventSortId(row);
}

function dedupeStrokeEvents(rows = []) {
    return dedupeAiEvents(rows);
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

function hasSensorContent(data) {
    const thresholds = data.thresholds || {};
    return data.temperature !== null
        || data.humidity !== null
        || data.gas !== null
        || hasAlertValue(data.fire)
        || hasAlertValue(data.alert)
        || thresholds.temperature !== null
        || thresholds.humidity !== null
        || thresholds.gas !== null;
}

function handleSensorPayload(topic, payload, receivedAt) {
    const normalizedData = normalizeSensorPayload(payload);
    if (!hasSensorContent(normalizedData)) return false;

    normalizedData.receivedAt = receivedAt;
    latestSensorData = {
        ...latestSensorData,
        ...normalizedData,
        topic,
        updatedAt: receivedAt
    };

    const alert = buildAlertFromPayload(topic, normalizedData, payload);
    if (alert) {
        rememberAlert(alert);
        persistSensorAlertIfFirst(alert, normalizedData);
    } else {
        clearLatestAlertIfNormal(normalizedData);
        persistSensorDataIfDue(normalizedData);
    }

    return true;
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
    const createdAt = data.receivedAt || vietnamTimestamp();

    return [
        {
            created_at: createdAt,
            sensor_name: 'temperature',
            temperature: data.temperature,
            humidity: null,
            gas_value: null,
            fire_detected: fireDetected,
            warning: buildWarningForSensor('temperature', data)
        },
        {
            created_at: createdAt,
            sensor_name: 'humidity',
            temperature: null,
            humidity: data.humidity,
            gas_value: null,
            fire_detected: fireDetected,
            warning: buildWarningForSensor('humidity', data)
        },
        {
            created_at: createdAt,
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

function sensorAlertPersistKey(alert = {}, data = {}) {
    const issueTypes = Array.isArray(alert.issues)
        ? alert.issues.map((issue) => issue.type || 'sensor').sort().join('|')
        : 'sensor';
    const alertValue = alertText(firstDefined(data.alert, data.warning, data.alarm, alert.message));
    return [alert.topic || '', issueTypes, alertValue].join('::');
}

function enqueueSensorPersist(data, options) {
    sensorPersistQueue = sensorPersistQueue
        .catch(() => null)
        .then(() => persistSensorDataBatch(data, options));
    return sensorPersistQueue;
}

async function persistSensorDataBatch(data, options = {}) {
    const {
        reason = 'Periodic sensor snapshot',
        updatePeriodicClock = true,
        alertKey = ''
    } = options;

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

    const rows = buildSensorDataRows(data);
    const savableRows = rows.filter((row) =>
        row.temperature !== null
        || row.humidity !== null
        || row.gas_value !== null
        || row.fire_detected
        || row.warning
    );

    if (savableRows.length === 0) {
        markSensorPersistStatus({
            saved: false,
            skipped: true,
            reason: 'No sensor value or alert to save',
            rows: 0,
            error: null
        });
        return latestSensorPersistStatus;
    }

    sensorPersistInFlight = true;
    try {
        const { error } = await supabase.from(SENSOR_DATA_TABLE).insert(savableRows);

        if (error) throw error;

        const savedAt = Date.now();
        if (updatePeriodicClock) lastSensorPersistMs = savedAt;
        if (alertKey) lastPersistedAlertKey = alertKey;
        markSensorPersistStatus({
            saved: true,
            skipped: false,
            reason,
            rows: savableRows.length,
            lastSavedAt: new Date(savedAt).toISOString(),
            error: null
        });
        console.log(`Saved ${savableRows.length} sensor rows to ${SENSOR_DATA_TABLE} (${reason})`);
    } catch (err) {
        markSensorPersistStatus({
            saved: false,
            skipped: false,
            reason,
            rows: 0,
            error: err.message
        });
        console.error('Sensor Data Supabase Error:', err.message);
    } finally {
        sensorPersistInFlight = false;
    }

    return latestSensorPersistStatus;
}

function persistSensorDataIfDue(data) {
    const now = Date.now();
    const elapsedMs = now - lastSensorPersistMs;

    if (lastSensorPersistMs && elapsedMs < SENSOR_PERSIST_INTERVAL_MS) {
        markSensorPersistStatus({
            saved: false,
            skipped: true,
            reason: `Wait ${Math.ceil((SENSOR_PERSIST_INTERVAL_MS - elapsedMs) / 1000)} more seconds`,
            rows: 0,
            error: null
        });
        return Promise.resolve(latestSensorPersistStatus);
    }

    lastSensorPersistMs = now;
    return enqueueSensorPersist(data, {
        reason: 'Periodic sensor snapshot',
        updatePeriodicClock: true
    });
}

function persistSensorAlertIfFirst(alert, data) {
    const alertKey = sensorAlertPersistKey(alert, data);

    if (!alertKey || alertKey === lastPersistedAlertKey) {
        return persistSensorDataIfDue(data);
    }

    lastPersistedAlertKey = alertKey;
    lastSensorPersistMs = Date.now();
    return enqueueSensorPersist(data, {
        reason: 'Immediate first alert snapshot',
        updatePeriodicClock: true,
        alertKey
    }).then((status) => {
        if (status.error && lastPersistedAlertKey === alertKey) lastPersistedAlertKey = '';
        return status;
    });
}

function rememberAlert(alert) {
    if (!alert) return;
    latestAlert = alert;
    alertHistory.unshift(alert);
    if (alertHistory.length > 50) alertHistory.pop();
    console.log(`ALERT received on ${alert.topic}: ${alert.message}`);
}

function clearLatestAlertIfNormal(data) {
    if (!isActiveAlertValue(data.alert)) {
        latestAlert = null;
        lastPersistedAlertKey = '';
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

function aiEventColumnsForQuery(config, options = {}) {
    const useAllTimeColumns = !!options.useAllTimeColumns;
    const useStatsTimeColumns = !!options.useStatsTimeColumns;
    const columns = useStatsTimeColumns
        ? (config.statsTimeColumns || config.timeColumns)
        : useAllTimeColumns
            ? config.timeColumns
            : [config.timeColumn || config.timeColumns?.[0]];
    return [...new Set(columns.filter(Boolean))];
}

async function fetchAiEventRows(config, options = {}) {
    const {
        limit = 100,
        start = '',
        end = '',
        ascending = false,
        requireImage = false,
        requireStatsAlert = false,
        useAllTimeColumns = false,
        useStatsTimeColumns = false
    } = options;
    if (requireImage && config.imageSource === false) return [];

    const rows = [];
    const timeColumns = aiEventColumnsForQuery(config, { useAllTimeColumns, useStatsTimeColumns });
    let firstError = null;
    let successCount = 0;

    for (const timeColumn of timeColumns) {
        try {
            let query = supabase
                .from(config.table)
                .select('*')
                .order(timeColumn, { ascending, nullsFirst: false })
                .limit(limit);

            if (config.idColumn && config.idColumn !== timeColumn) {
                query = query.order(config.idColumn, { ascending });
            }

            query = applyDateFilters(query, timeColumn, start, end);

            const { data, error } = await query;
            if (error) throw error;

            successCount += 1;
            rows.push(...(data || []).map((row) => normalizeAiEventRow(row, config, timeColumn)));
        } catch (err) {
            if (!firstError) firstError = err;
            console.warn(`AI event query skipped for ${config.table}.${timeColumn}:`, err.message);
        }
    }

    if (successCount === 0 && firstError) throw firstError;

    return dedupeAiEvents(rows)
        .filter((row) => !requireImage || hasAiEventImage(row))
        .filter((row) => !requireStatsAlert || isAiEventStatsRow(row, config));
}

async function fetchAiEventRowsFromAll(options = {}) {
    const results = await Promise.all(AI_EVENT_TABLE_CONFIGS.map(async (config) => {
        try {
            return { config, rows: await fetchAiEventRows(config, options), error: null };
        } catch (err) {
            console.warn(`AI event table skipped for ${config.table}:`, err.message);
            return { config, rows: [], error: err };
        }
    }));

    const hasSuccessfulQuery = results.some((result) => !result.error);
    const firstError = results.find((result) => result.error)?.error;
    if (!hasSuccessfulQuery && firstError) throw firstError;

    return results.flatMap((result) => result.rows);
}

async function fetchAiEventRowsByTable(options = {}) {
    const results = await Promise.all(AI_EVENT_TABLE_CONFIGS.map(async (config) => {
        try {
            return { config, rows: await fetchAiEventRows(config, options), error: null };
        } catch (err) {
            console.warn(`AI event table skipped for ${config.table}:`, err.message);
            return { config, rows: [], error: err };
        }
    }));

    const hasSuccessfulQuery = results.some((result) => !result.error);
    const firstError = results.find((result) => result.error)?.error;
    if (!hasSuccessfulQuery && firstError) throw firstError;

    return results.reduce((grouped, result) => {
        grouped[result.config.key] = result.rows;
        return grouped;
    }, {});
}

function startOfLocalDay(date = new Date()) {
    return new Date(date.getFullYear(), date.getMonth(), date.getDate());
}

function addDays(date, days) {
    const nextDate = new Date(date);
    nextDate.setDate(nextDate.getDate() + days);
    return nextDate;
}

function dateKey(dateLike) {
    const date = new Date(dateLike);
    if (Number.isNaN(date.getTime())) return '';
    const year = date.getFullYear();
    const month = String(date.getMonth() + 1).padStart(2, '0');
    const day = String(date.getDate()).padStart(2, '0');
    return `${year}-${month}-${day}`;
}

function timeLabel(dateLike) {
    const date = new Date(dateLike);
    if (Number.isNaN(date.getTime())) return '';
    return date.toLocaleTimeString('vi-VN', { hour12: false, hour: '2-digit', minute: '2-digit' });
}

function dayLabel(key) {
    const [year, month, day] = String(key).split('-').map(Number);
    if (!year || !month || !day) return String(key || '--');
    return new Date(year, month - 1, day).toLocaleDateString('vi-VN', { day: '2-digit', month: '2-digit' });
}

function getStatsRange(rangeValue) {
    const range = String(rangeValue || 'today').toLowerCase();
    const now = new Date();
    const todayStart = startOfLocalDay(now);

    if (range === '7d' || range === '7days') {
        return { key: '7d', days: 7, start: addDays(todayStart, -6), end: now };
    }
    if (range === '30d' || range === '30days') {
        return { key: '30d', days: 30, start: addDays(todayStart, -29), end: now };
    }
    return { key: 'today', days: 1, start: todayStart, end: now };
}

function buildDayBuckets(days, end = new Date()) {
    const buckets = [];
    const start = addDays(startOfLocalDay(end), -(days - 1));
    for (let index = 0; index < days; index += 1) {
        const date = addDays(start, index);
        const key = dateKey(date);
        buckets.push({ key, label: dayLabel(key), count: 0 });
    }
    return buckets;
}

function average(values) {
    const numbers = values.filter((value) => Number.isFinite(value));
    if (numbers.length === 0) return null;
    return numbers.reduce((sum, value) => sum + value, 0) / numbers.length;
}

function rowNumber(row, keys) {
    for (const key of keys) {
        const number = toNumber(row?.[key]);
        if (number !== null) return number;
    }
    return null;
}

function appendCurrentSensorRows(rows, range) {
    if (range.key !== 'today' || !latestSensorData.updatedAt) return rows;

    const updatedAt = new Date(latestSensorData.updatedAt);
    if (Number.isNaN(updatedAt.getTime()) || updatedAt < range.start || updatedAt > range.end) return rows;

    const activeAlert = isActiveAlertValue(latestSensorData.alert) ? latestSensorData.alert : null;
    const currentRows = [
        latestSensorData.temperature !== null ? { created_at: latestSensorData.updatedAt, sensor_name: 'temperature', temperature: latestSensorData.temperature, fire_detected: isTruthy(latestSensorData.fire), warning: activeAlert } : null,
        latestSensorData.humidity !== null ? { created_at: latestSensorData.updatedAt, sensor_name: 'humidity', humidity: latestSensorData.humidity, fire_detected: isTruthy(latestSensorData.fire), warning: activeAlert } : null,
        latestSensorData.gas !== null ? { created_at: latestSensorData.updatedAt, sensor_name: 'gas', gas_value: latestSensorData.gas, fire_detected: isTruthy(latestSensorData.fire), warning: activeAlert } : null
    ].filter(Boolean);

    return rows.concat(currentRows);
}

function aggregateSensorStats(rows, range) {
    const pointsByKey = new Map();
    const tempValues = [];
    const humiValues = [];
    const gasValues = [];
    const alertKeys = new Set();

    for (const row of rows) {
        const rowTime = firstDefined(row.created_at, row.timestamp);
        const date = new Date(rowTime);
        if (Number.isNaN(date.getTime())) continue;

        const temp = rowNumber(row, ['temperature', 'temp']);
        const humi = rowNumber(row, ['humidity', 'humi', 'hum']);
        const gas = rowNumber(row, ['gas_value', 'gas']);
        const hasMetric = temp !== null || humi !== null || gas !== null;
        const hasAlert = !!(row.fire_detected || row.warning);

        if (!hasMetric && !hasAlert) continue;

        const minuteKey = date.toISOString().slice(0, 16);
        const groupKey = range.key === 'today' ? minuteKey : dateKey(date);
        if (!pointsByKey.has(groupKey)) {
            pointsByKey.set(groupKey, {
                key: groupKey,
                label: range.key === 'today' ? timeLabel(date) : dayLabel(dateKey(date)),
                time: date.toISOString(),
                temp: [],
                humi: [],
                gas: [],
                hasAlert: false
            });
        }

        const point = pointsByKey.get(groupKey);

        if (temp !== null) {
            point.temp.push(temp);
            tempValues.push(temp);
        }
        if (humi !== null) {
            point.humi.push(humi);
            humiValues.push(humi);
        }
        if (gas !== null) {
            point.gas.push(gas);
            gasValues.push(gas);
        }

        if (hasAlert) {
            point.hasAlert = true;
            alertKeys.add(groupKey);
        }
    }

    let points = Array.from(pointsByKey.values())
        .sort((a, b) => new Date(a.time) - new Date(b.time))
        .map((point) => ({
            label: point.label,
            time: point.time,
            temperature: average(point.temp),
            humidity: average(point.humi),
            gas: average(point.gas),
            alert: point.hasAlert
        }));

    if (range.key !== 'today') {
        const existing = new Map(points.map((point) => [dateKey(point.time), point]));
        const buckets = buildDayBuckets(range.days, range.end);
        points = buckets.map((bucket, index) => existing.get(bucket.key) || {
            label: bucket.label,
            time: addDays(range.start, index).toISOString(),
            temperature: null,
            humidity: null,
            gas: null,
            alert: false
        });
    }

    return {
        points,
        summary: {
            avgTemperature: average(tempValues),
            avgHumidity: average(humiValues),
            avgGas: average(gasValues),
            alertCount: alertKeys.size,
            sampleCount: points.length
        }
    };
}

function aggregateStrokeStats(rows, range) {
    const buckets = buildDayBuckets(range.days, range.end);
    const bucketMap = new Map(buckets.map((bucket) => [bucket.key, bucket]));

    for (const row of rows) {
        const key = dateKey(getAiEventTime(row));
        const bucket = bucketMap.get(key);
        if (bucket) bucket.count += 1;
    }

    const maxDay = buckets.reduce((best, item) => item.count > best.count ? item : best, buckets[0] || { label: '--', count: 0 });
    const total = buckets.reduce((sum, item) => sum + item.count, 0);

    return {
        days: buckets,
        summary: {
            total,
            maxDay,
            averagePerDay: buckets.length ? total / buckets.length : 0
        }
    };
}

function aggregateAiEventStats(rowsByTable, range) {
    const allRows = [];
    const tables = AI_EVENT_TABLE_CONFIGS.map((config) => {
        const rows = rowsByTable[config.key] || [];
        allRows.push(...rows);
        return {
            key: config.key,
            table: config.table,
            label: config.label,
            count: rows.length
        };
    });
    const dailyStats = aggregateStrokeStats(allRows, range);
    const maxTable = tables.reduce(
        (best, item) => item.count > best.count ? item : best,
        tables[0] || { label: '--', count: 0 }
    );

    return {
        ...dailyStats,
        tables,
        summary: {
            ...dailyStats.summary,
            total: tables.reduce((sum, item) => sum + item.count, 0),
            maxTable,
            tableCount: tables.length
        }
    };
}

function objectFromJson(value) {
    if (!value) return null;
    if (typeof value === 'object') return value;
    if (typeof value !== 'string') return null;

    try {
        const parsed = JSON.parse(value);
        return parsed && typeof parsed === 'object' ? parsed : null;
    } catch (err) {
        return null;
    }
}

function getAiEventImageUrl(row = {}) {
    const metadata = objectFromJson(row.metadata);
    const bbox = objectFromJson(row.bbox);
    return firstDefined(
        row.image_url,
        row.imageUrl,
        row.imageURL,
        row.snapshot_url,
        row.snapshotUrl,
        row.frame_url,
        row.frameUrl,
        row.photo_url,
        row.photoUrl,
        row.thumbnail_url,
        row.thumbnailUrl,
        row.url,
        row.image,
        row.path,
        row.file_path,
        row.filePath,
        metadata?.image_url,
        metadata?.imageUrl,
        metadata?.snapshot_url,
        metadata?.snapshotUrl,
        metadata?.frame_url,
        metadata?.frameUrl,
        metadata?.url,
        metadata?.path,
        bbox?.image_url,
        bbox?.imageUrl,
        bbox?.url,
        bbox?.path
    );
}

function getStrokeImageUrl(row = {}) {
    return getAiEventImageUrl(row);
}

function getStoragePathFromUrl(value, bucket = SURVEILLANCE_BUCKET) {
    if (!value) return null;

    const text = String(value).trim();
    if (!text) return null;

    if (!/^https?:\/\//i.test(text)) {
        return text.replace(/^\/+/, '');
    }

    try {
        const url = new URL(text);
        const decodedPath = decodeURIComponent(url.pathname);
        const publicMarker = `/storage/v1/object/public/${bucket}/`;
        const signedMarker = `/storage/v1/object/sign/${bucket}/`;
        const marker = decodedPath.includes(publicMarker) ? publicMarker : signedMarker;
        const index = decodedPath.indexOf(marker);

        if (index === -1) return null;
        return decodedPath.slice(index + marker.length).replace(/^\/+/, '');
    } catch (err) {
        return null;
    }
}

async function deleteAiEventImage(row, config = {}) {
    const bucket = config.storageBucket || SURVEILLANCE_BUCKET;
    const imagePath = getStoragePathFromUrl(getAiEventImageUrl(row), bucket);

    if (!imagePath) {
        return { deleted: false, path: null, reason: 'No storage path found on row' };
    }

    const { error } = await supabase.storage.from(bucket).remove([imagePath]);
    if (error) throw error;

    return { deleted: true, path: imagePath, reason: null };
}

async function deleteStrokeEventImage(row) {
    return deleteAiEventImage(row, { storageBucket: SURVEILLANCE_BUCKET });
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
        const healthQuery = supabase
            .from(SENSOR_DATA_TABLE)
            .select('*', { count: 'exact', head: true });
        const timeout = new Promise((_, reject) => {
            setTimeout(() => reject(new Error(`Supabase health timeout after ${SUPABASE_HEALTH_TIMEOUT_MS}ms`)), SUPABASE_HEALTH_TIMEOUT_MS);
        });
        const { error } = await Promise.race([healthQuery, timeout]);

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
    const receivedAt = vietnamTimestamp();

    let payload = null;
    try {
        payload = JSON.parse(text);
    } catch (err) {
        if (topic === SENSOR_DATA_TOPIC) {
            console.error('Invalid sensor payload:', err.message);
            return;
        }
    }

    if (payload && handleSensorPayload(topic, payload, receivedAt)) {
        return;
    }

    if (topic.includes('alert')) {
        try {
            const normalizedData = normalizeSensorPayload(payload);
            normalizedData.receivedAt = receivedAt;
            const alert = buildAlertFromPayload(topic, normalizedData, payload);
            if (alert) {
                rememberAlert(alert);
                persistSensorAlertIfFirst(alert, normalizedData);
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

mqttClient.on('reconnect', () => {
    console.warn('MQTT reconnecting...');
});

mqttClient.on('close', () => {
    console.warn('MQTT connection closed');
});

mqttClient.on('offline', () => {
    console.warn('MQTT client offline');
});

app.post('/api/auth/login', (req, res) => {
    const username = String(req.body?.username || '').trim();
    const password = String(req.body?.password || '');
    const user = Object.values(authUsers).find((candidate) =>
        candidate.username === username && candidate.password === password
    );

    if (!user) {
        return res.status(401).json({ error: 'Sai tên đăng nhập hoặc mật khẩu' });
    }

    res.json({
        token: createAuthToken(user),
        user: {
            username: user.username,
            role: user.role
        }
    });
});

app.get('/api/auth/me', requireAuth, (req, res) => {
    res.json({ user: req.authUser });
});

app.get('/api/images', async (req, res) => {
    res.set('Cache-Control', 'no-store, no-cache, must-revalidate, proxy-revalidate');

    if (!supabase) {
        return res.status(500).json({ error: 'Supabase chưa được cấu hình' });
    }

    try {
        const data = (await fetchAiEventRowsFromAll({
            limit: 11,
            requireImage: true,
            useAllTimeColumns: true
        }))
            .sort((a, b) => aiEventSortTime(b) - aiEventSortTime(a) || aiEventSortId(b) - aiEventSortId(a))
            .slice(0, 11);

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

app.get('/api/history', async (req, res) => {
    res.set('Cache-Control', 'no-store, no-cache, must-revalidate, proxy-revalidate');

    if (!supabase) {
        return res.status(500).json({ error: 'Supabase chua duoc cau hinh' });
    }

    const type = String(req.query.type || 'sensor').toLowerCase();
    const range = getStatsRange(req.query.range);
    const start = range.start.toISOString();
    const end = range.end.toISOString();
    const sensorStart = vietnamRangeTimestamp(range.start);
    const sensorEnd = vietnamRangeTimestamp(range.end);
    const limit = Math.min(Math.max(Number(req.query.limit || 300), 1), 1000);

    try {
        if (type === 'images' || type === 'stroke_event' || type === 'stroke_events' || type === 'ai' || type === 'ai_alert' || type === 'ai_alerts') {
            const rows = (await fetchAiEventRowsFromAll({
                limit,
                start,
                end,
                requireImage: true
            }))
                .sort((a, b) => aiEventSortTime(b) - aiEventSortTime(a) || aiEventSortId(b) - aiEventSortId(a))
                .slice(0, limit);

            return res.json({
                type: 'images',
                range: range.key,
                start,
                end,
                rows,
                count: rows.length
            });
        }

        const { data, error } = await supabase
            .from(SENSOR_DATA_TABLE)
            .select('*')
            .gte('created_at', sensorStart)
            .lte('created_at', sensorEnd)
            .order('created_at', { ascending: false, nullsFirst: false })
            .order('id', { ascending: false })
            .limit(limit);

        if (error) throw error;

        const rows = (data || []).filter((row) => row.fire_detected || (row.warning && String(row.warning).trim()));
        return res.json({
            type: 'sensor',
            range: range.key,
            start,
            end,
            rows,
            count: rows.length
        });
    } catch (err) {
        console.error('History Query Error:', err.message);
        res.status(500).json({ error: err.message });
    }
});

app.get('/api/statistics', async (req, res) => {
    res.set('Cache-Control', 'no-store, no-cache, must-revalidate, proxy-revalidate');

    if (!supabase) {
        return res.status(500).json({ error: 'Supabase chua duoc cau hinh' });
    }

    const type = String(req.query.type || 'sensor').toLowerCase();
    const range = getStatsRange(req.query.range);
    const start = range.start.toISOString();
    const end = range.end.toISOString();
    const sensorStart = vietnamRangeTimestamp(range.start);
    const sensorEnd = vietnamRangeTimestamp(range.end);

    try {
        if (type === 'stroke_event' || type === 'stroke_events' || type === 'stroke' || type === 'ai' || type === 'ai_alert' || type === 'ai_alerts') {
            const rowsByTable = await fetchAiEventRowsByTable({
                limit: 5000,
                start,
                end,
                ascending: true,
                requireStatsAlert: true,
                useStatsTimeColumns: true
            });

            return res.json({
                type: 'stroke_event',
                range: range.key,
                start,
                end,
                ...aggregateAiEventStats(rowsByTable, range)
            });
        }

        const { data, error } = await supabase
            .from(SENSOR_DATA_TABLE)
            .select('*')
            .gte('created_at', sensorStart)
            .lte('created_at', sensorEnd)
            .order('created_at', { ascending: true, nullsFirst: false })
            .order('id', { ascending: true })
            .limit(5000);

        if (error) throw error;

        const rows = appendCurrentSensorRows(data || [], range);
        return res.json({
            type: 'sensor',
            range: range.key,
            start,
            end,
            ...aggregateSensorStats(rows, range)
        });
    } catch (err) {
        console.error('Statistics Error:', err.message);
        res.status(500).json({ error: err.message });
    }
});

app.get('/api/database/:table', requireAdmin, async (req, res) => {
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
        const idColumn = config.idColumn || 'id';
        let query = supabase
            .from(config.table)
            .select('*', { count: 'exact' })
            .order(config.timeColumn, { ascending: false, nullsFirst: false })
            .limit(limit);

        if (idColumn && idColumn !== config.timeColumn) {
            query = query.order(idColumn, { ascending: false });
        }

        query = applyDateFilters(query, config.timeColumn, start, end);

        const { data, error, count } = await query;
        if (error) throw error;

        res.json({
            table: config.table,
            label: config.label || config.table,
            timeColumn: config.timeColumn,
            idColumn,
            isAiEvent: !!config.isAiEvent,
            rows: data || [],
            count: count || 0
        });
    } catch (err) {
        console.error('Database Query Error:', err.message);
        res.status(500).json({ error: err.message });
    }
});

app.delete('/api/database/:table/:id', requireAdmin, async (req, res) => {
    if (!supabase) {
        return res.status(500).json({ error: 'Supabase chua duoc cau hinh' });
    }

    const config = getDatabaseConfig(req.params.table);
    if (!config) {
        return res.status(400).json({ error: 'Bang du lieu khong duoc ho tro' });
    }

    const id = String(req.params.id || '').trim();
    if (!id) {
        return res.status(400).json({ error: 'ID khong hop le' });
    }

    try {
        let storageResult = null;
        const idColumn = config.idColumn || 'id';

        if (config.isAiEvent && config.storageBucket) {
            const { data: row, error: fetchError } = await supabase
                .from(config.table)
                .select('*')
                .eq(idColumn, id)
                .single();

            if (fetchError) {
                storageResult = { deleted: false, path: null, reason: fetchError.message };
            } else {
                try {
                    storageResult = await deleteAiEventImage(row, config);
                } catch (storageError) {
                    storageResult = {
                        deleted: false,
                        path: getStoragePathFromUrl(getAiEventImageUrl(row), config.storageBucket),
                        reason: storageError.message
                    };
                    console.warn('Storage delete failed, deleting database row anyway:', storageError.message);
                }
            }
        }

        const { data, error } = await supabase
            .from(config.table)
            .delete()
            .eq(idColumn, id)
            .select(idColumn);

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

app.delete('/api/database/sensor_data', requireAdmin, async (req, res) => {
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
        mqttConfig: mqttRuntimeConfig,
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
