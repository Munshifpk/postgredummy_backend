const path = require('path');
const dotenv = require('dotenv');
const WebSocket = require('ws');

const rootDir = path.join(__dirname, '..');

// Load root Neon environment first, then fallback to local backend env files.
dotenv.config({ path: path.join(rootDir, '.env.local') });
dotenv.config({ path: path.join(rootDir, '.env') });
dotenv.config({ path: path.join(__dirname, '.env') });

const express = require('express');
const cors = require('cors');
const { Pool } = require('pg');

const app = express();

app.use(cors());
app.use(express.json());

const pool = process.env.DATABASE_URL
    ? new Pool({
        connectionString: process.env.DATABASE_URL,
        ssl: { rejectUnauthorized: false },
    })
    : new Pool({
        host: process.env.DB_HOST || 'localhost',
        port: Number(process.env.DB_PORT || 5432),
        database: process.env.DB_NAME || 'student_management',
        user: process.env.DB_USER || 'postgres',
        password: String(process.env.DB_PASSWORD || ''),
    });

async function ensureSchema() {
    await pool.query(`
        CREATE TABLE IF NOT EXISTS students (
            student_id SERIAL PRIMARY KEY,
            full_name TEXT NOT NULL,
            email TEXT NOT NULL UNIQUE,
            age INTEGER,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
    `);
}

async function checkNeonAuth() {
    if (!process.env.NEON_AUTH_JWKS_URL) {
        return { ok: false, error: 'NEON_AUTH_JWKS_URL not configured' };
    }

    try {
        const response = await fetch(process.env.NEON_AUTH_JWKS_URL, {
            headers: { Accept: 'application/json' },
        });
        if (!response.ok) {
            return { ok: false, error: `JWKS HTTP ${response.status}` };
        }
        const json = await response.json();
        const keyCount = Array.isArray(json?.keys) ? json.keys.length : 0;
        return { ok: keyCount > 0, keys: keyCount };
    } catch (error) {
        return { ok: false, error: error.message };
    }
}

function getHelloFunctionUrl() {
    if (process.env.NEON_FUNCTION_HELLO_URL) {
        return process.env.NEON_FUNCTION_HELLO_URL;
    }
    if (!process.env.NEON_BRANCH_ID || !process.env.NEON_FUNCTION_HOST_SUFFIX) {
        return null;
    }
    return `https://${process.env.NEON_BRANCH_ID}-hello${process.env.NEON_FUNCTION_HOST_SUFFIX}/`;
}

async function checkNeonFunction() {
    const functionUrl = getHelloFunctionUrl();
    if (!functionUrl) {
        return { ok: false, error: 'Function URL cannot be constructed' };
    }

    try {
        const response = await fetch(functionUrl);
        const body = await response.text();
        return {
            ok: response.ok,
            url: functionUrl,
            status: response.status,
            body,
        };
    } catch (error) {
        return { ok: false, url: functionUrl, error: error.message };
    }
}

async function checkNeonRealtime() {
    if (!process.env.NEON_REALTIME_URL) {
        return { ok: false, error: 'NEON_REALTIME_URL not configured' };
    }

    return new Promise((resolve) => {
        const headers = {};
        if (process.env.NEON_REALTIME_SECRET) {
            headers.Authorization = `Bearer ${process.env.NEON_REALTIME_SECRET}`;
        }

        const socket = new WebSocket(process.env.NEON_REALTIME_URL, { headers });
        let settled = false;

        const done = (result) => {
            if (settled) return;
            settled = true;
            try {
                socket.close();
            } catch (_) { }
            resolve(result);
        };

        socket.on('open', () => done({ ok: true }));
        socket.on('error', (error) => done({ ok: false, error: error.message }));

        setTimeout(() => {
            done({ ok: false, error: 'Timed out connecting to realtime' });
        }, 5000);
    });
}

// Health check endpoint
app.get('/api/health', async (req, res) => {
    try {
        await pool.query('SELECT 1');
        res.json({ status: 'ok', database: 'connected' });
    } catch (error) {
        res.status(500).json({ status: 'error', message: error.message });
    }
});

app.get('/api/neon/health', async (req, res) => {
    const postgres = { ok: false };

    try {
        const dbResult = await pool.query('SELECT current_database() AS database_name, NOW() AS now');
        postgres.ok = true;
        postgres.database = dbResult.rows[0]?.database_name;
        postgres.now = dbResult.rows[0]?.now;
    } catch (error) {
        postgres.error = error.message;
    }

    const auth = await checkNeonAuth();
    const functions = await checkNeonFunction();
    const realtime = await checkNeonRealtime();

    const allOk = postgres.ok && auth.ok && functions.ok && realtime.ok;
    res.status(allOk ? 200 : 500).json({
        status: allOk ? 'ok' : 'error',
        services: {
            postgres,
            auth,
            functions,
            realtime,
        },
    });
});

// Get all students
app.get('/api/students', async (req, res) => {
    try {
        const result = await pool.query(
            'SELECT student_id, full_name, email, age, created_at FROM students ORDER BY student_id ASC'
        );
        res.json(result.rows);
    } catch (error) {
        console.error('Error fetching students:', error.message);
        res.status(500).json({ error: 'Unable to fetch students' });
    }
});

// Get student by ID
app.get('/api/students/:id', async (req, res) => {
    try {
        const { id } = req.params;
        const result = await pool.query(
            'SELECT student_id, full_name, email, age, created_at FROM students WHERE student_id = $1',
            [id]
        );
        if (result.rows.length === 0) {
            return res.status(404).json({ error: 'Student not found' });
        }
        res.json(result.rows[0]);
    } catch (error) {
        console.error('Error fetching student:', error.message);
        res.status(500).json({ error: 'Unable to fetch student' });
    }
});

// Create a new student
app.post('/api/students', async (req, res) => {
    try {
        const { full_name, email, age } = req.body;

        if (!full_name || !full_name.trim()) {
            return res.status(400).json({ error: 'Full name is required' });
        }
        if (!email || !email.trim()) {
            return res.status(400).json({ error: 'Email is required' });
        }

        const parsedAge = age !== undefined && age !== null && age !== ''
            ? parseInt(age, 10)
            : null;

        const result = await pool.query(
            'INSERT INTO students (full_name, email, age, created_at) VALUES ($1, $2, $3, NOW()) RETURNING student_id, full_name, email, age, created_at',
            [full_name.trim(), email.trim(), parsedAge]
        );

        res.status(201).json(result.rows[0]);
    } catch (error) {
        console.error('Error creating student:', error.message);
        if (error.code === '23505') { // Unique constraint violation (e.g. duplicate email)
            return res.status(409).json({ error: 'A student with this email already exists' });
        }
        res.status(500).json({ error: 'Unable to create student' });
    }
});

// Update a student
app.put('/api/students/:id', async (req, res) => {
    try {
        const { id } = req.params;
        const { full_name, email, age } = req.body;

        if (!full_name || !full_name.trim()) {
            return res.status(400).json({ error: 'Full name is required' });
        }
        if (!email || !email.trim()) {
            return res.status(400).json({ error: 'Email is required' });
        }

        const parsedAge = age !== undefined && age !== null && age !== ''
            ? parseInt(age, 10)
            : null;

        const result = await pool.query(
            'UPDATE students SET full_name = $1, email = $2, age = $3 WHERE student_id = $4 RETURNING student_id, full_name, email, age, created_at',
            [full_name.trim(), email.trim(), parsedAge, id]
        );

        if (result.rows.length === 0) {
            return res.status(404).json({ error: 'Student not found' });
        }

        res.json(result.rows[0]);
    } catch (error) {
        console.error('Error updating student:', error.message);
        if (error.code === '23505') {
            return res.status(409).json({ error: 'A student with this email already exists' });
        }
        res.status(500).json({ error: 'Unable to update student' });
    }
});

// Delete a student
app.delete('/api/students/:id', async (req, res) => {
    try {
        const { id } = req.params;
        const result = await pool.query(
            'DELETE FROM students WHERE student_id = $1 RETURNING student_id, full_name',
            [id]
        );

        if (result.rows.length === 0) {
            return res.status(404).json({ error: 'Student not found' });
        }

        res.json({ message: 'Student deleted successfully', student: result.rows[0] });
    } catch (error) {
        console.error('Error deleting student:', error.message);
        res.status(500).json({ error: 'Unable to delete student' });
    }
});

const PORT = process.env.PORT || 3000;

ensureSchema()
    .then(() => {
        app.listen(PORT, () => {
            console.log(`Student API running on http://localhost:${PORT}`);
        });
    })
    .catch((error) => {
        console.error('Failed to initialize database schema:', error.message);
        process.exit(1);
    });
