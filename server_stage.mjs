import express from 'express';
import { WebSocketServer } from 'ws';
import { createServer } from 'http';
import { networkInterfaces } from 'os';
import crypto from 'crypto';
import { mkdir, readFile, writeFile, rename, appendFile } from 'fs/promises';
import { existsSync } from 'fs';
import { spawn, execSync } from 'child_process';
import { join, dirname } from 'path';
import { fileURLToPath } from 'url';
import { getAllTargets, pokeTarget, memflowPollResponses, memflowMarkAsRead, memflowReadInbox, memflowWriteResponse, memflowCheckReceipts, memflowGetActiveAgents } from './connectors/index.mjs';
import { getRunningProductType } from './connectors/antigravity.mjs';

const APP_VERSION = '2.0.0';
const __dirname = dirname(fileURLToPath(import.meta.url));
const DATA_DIR = join(__dirname, 'data');
const LOGS_DIR = join(__dirname, '.logs');
const LOG_FILE = join(LOGS_DIR, `ag-bridge-${new Date().toISOString().split('T')[0]}.log`);
const STATE_FILE = join(DATA_DIR, 'state.json');
const APPROVALS_FILE = join(DATA_DIR, 'approvals.json');
const POLICY_FILE = join(__dirname, 'policy.json');
let POLICY = { version: 2, profiles: { relaxed: { allow: [".*"] }, balanced: { allow: [] } }, globalDeny: [] };

// --- Config ---
const args = process.argv.slice(2);
const getArg = (name) => {
    const idx = args.indexOf(name);
    return idx !== -1 ? args[idx + 1] : null;
};
const hasArg = (name) => args.includes(name);

const PORT = parseInt(getArg('--port') || process.env.PORT || '8787');
const HOST = getArg('--host') || '0.0.0.0';

export const app = express();
export const server = createServer(app);
// Don't bind 'server' here so we can handle upgrade manually for auth
export const wss = new WebSocketServer({ noServer: true });

// --- Poke Logic ---
let pokeInFlight = false;
let lastPokeAt = 0;
let retryTimer = null;
let retryAttempts = 0;

async function runPokeScript() {
    // Find ALL "new" messages for the agent
    const pendingMsgs = STATE.messages
        .filter(m => m.to === 'agent' && m.status === 'new')
        .sort((a, b) => new Date(a.createdAt) - new Date(b.createdAt));

    if (pendingMsgs.length === 0) {
        log('POKE', 'No new messages to deliver.');
        return { ok: true, method: 'no_messages' };
    }

    // 1. Resolve project name from the message targetId
    // Resolve project: prefer the message's own targetId, then the selected project,
    // then the first open workspace, then 'global' as a last resort.
    const rawTarget = pendingMsgs[0].targetId
        || STATE.targetProject?.projectName
        || STATE.targetProject?.title
        || null;
    // If still null, use the first live process-scan workspace as the implicit target
    let finalProjectName;
    if (rawTarget && rawTarget !== 'global') {
        finalProjectName = typeof rawTarget === 'string'
            ? rawTarget
            : (rawTarget?.projectName || rawTarget?.title || 'global');
    } else {
        // Auto-pick: use targetProject if set, otherwise use the first IDE workspace
        const auto = STATE.targetProject;
        if (auto) {
            finalProjectName = typeof auto === 'string' ? auto : (auto.projectName || auto.title || 'global');
        } else {
            finalProjectName = 'global';
        }
    }

    // =====================================================
    // STRATAFLOW GATE: Project Confirmation & Validation
    // =====================================================
    const activeAgents = await memflowGetActiveAgents();
    const activeProjects = [...new Set(activeAgents.map(a => a.project))];
    const isTargetActive = activeAgents.some(a => a.project === finalProjectName);

    if (finalProjectName !== 'global' && !isTargetActive) {
        log('STRATAFLOW', `Target project '${finalProjectName}' is inactive. Intercepting message.`);
        
        const projectList = activeProjects.length > 0 
            ? activeProjects.map(p => `- ${p}`).join('\n')
            : "- (No active agents found in registry)";
        
        const gateMessage = `[Strataflow Gate] The target project '${finalProjectName}' is currently inactive or hasn't sent a heartbeat recently.\n\nWhich active project should handle this?\n${projectList}`;

        await memflowWriteResponse(gateMessage, {
            project: finalProjectName,
            from: 'system',
            channel: 'system'
        });

        // Mark messages as intercepted so they don't loop
        pendingMsgs.forEach(m => {
            m.status = 'intercepted';
            broadcast('message_ack', { id: m.id, status: 'intercepted', receipt: 'Intercepted' });
        });
        saveState();
        return { ok: true, method: 'strataflow_gate' };
    }

    // 2. Build combined message text
    const msgText = pendingMsgs.map(m => m.text).join('\n\n')
        + `

<system_note>You are working in the [${finalProjectName}] workspace. Restrict ALL actions to this project only.

COMMUNICATION RULES (follow these exactly):
1. Send responses via the mobile_respond MCP tool — do NOT reply in the local chat panel.
2. Send status updates via: POST http://127.0.0.1:8787/agent/status  body: {"state":"working","task":"what you are doing right now","project":"${finalProjectName}"}
3. When idle or done: POST http://127.0.0.1:8787/agent/status  body: {"state":"idle","task":"","project":"${finalProjectName}"}

APPROVAL RULES (IMPORTANT — follow for every tool use that requires user permission):
- Before running any shell command, writing files, or making network requests that you would normally ask the user to approve, FIRST call:
  POST http://127.0.0.1:8787/agent/request-approval
  body: {"kind":"command","details":{"cmd":"<the exact command>","risk":"low|medium|high"},"project":"${finalProjectName}"}
- The response will have {"ok":true,"approval":{"id":"appr_...","status":"approved"|"pending"}}
- If status is "pending", poll every 3 seconds: GET http://127.0.0.1:8787/agent/approval-status/<id>
- ONLY proceed when status is "approved". If "denied", stop and tell the user via mobile_respond.
- If autonomous mode is active, status will immediately be "approved" and you can proceed without polling.
</system_note>`;

    log('POKE', `Delivering ${pendingMsgs.length} messages (${msgText.length} chars) for project: ${finalProjectName}`);

    const pokeMetadata = { project: finalProjectName, from: 'user', to: 'agent', channel: 'work' };
    let delivered = false;

    // =====================================================
    // STEP 1: MemFlow Write (PRIMARY — this IS delivery)
    // Write directly to MongoDB inbox — agent picks up via mobile_read_inbox MCP tool.
    // DO NOT route through pokeTarget() — memflow has no CONNECTOR_ID/poke().
    // =====================================================
    try {
        const mfResult = await memflowReadInbox(msgText, {
            ...pokeMetadata,
            id: `inbox_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`,
        });
        if (mfResult.ok) {
            delivered = true;
            log('POKE', `MemFlow delivery: SUCCESS (${mfResult.method || 'mongodb'}) id=${mfResult.id}`);
            // Messages written to MongoDB — mark as 'sent' and store the inbox ID for receipt tracking
            pendingMsgs.forEach(m => {
                m.status = 'sent';
                m.memflowInboxId = mfResult.id;  // Track for delivery receipt sweeper
                broadcast('message_ack', { id: m.id, status: 'sent', receipt: '✓ Sent' });
            });
            saveState();
            log('POKE', `Marked ${pendingMsgs.length} messages as sent via MemFlow.`);
        } else {
            log('POKE', 'MemFlow delivery: write returned not ok', mfResult);
        }
    } catch (e) {
        log('POKE', `MemFlow delivery error: ${e.message}`);
    }

    // =====================================================
    // STEP 2: CDP Notification (OPTIONAL — just wakes agent)
    // =====================================================
    try {
        const targets = await getTargetsForProductMode();
        if (targets.length > 0) {
            // Normalize project name for matching: treat hyphens == underscores
            // The process-scan slug decoder produces underscores; folder names use hyphens.
            const norm = (s) => (s || '').toLowerCase().replace(/[-_]/g, '-');
            const normProject = norm(finalProjectName);

            const exactMatch = targets.find(t =>
                norm(t.projectName) === normProject ||
                (t.title && norm(t.title).includes(normProject))
            );

            if (!exactMatch) {
                // No matching window found — try cross-window broadcast wake
                const anyTarget = targets.find(t => t.port);
                if (anyTarget) {
                    log('POKE', `CDP notify: no window for '${finalProjectName}' — broadcasting to '${anyTarget.projectName || anyTarget.title}'`);
                    const crossResult = await pokeTarget(anyTarget, msgText, pokeMetadata);
                    if (crossResult.ok) {
                        log('POKE', `CDP cross-window notify: SUCCESS via '${anyTarget.projectName || anyTarget.title}'`);
                    } else {
                        log('POKE', `CDP cross-window notify: ${crossResult.reason || crossResult.error || 'failed'} (non-fatal)`);
                    }
                } else {
                    log('POKE', `CDP notify: no window for '${finalProjectName}' and no other IDE windows available`);
                }
            } else {
                log('POKE', `CDP notify -> ${exactMatch.title || exactMatch.id} (port ${exactMatch.port})`);
                const cdpResult = await pokeTarget(exactMatch, msgText, pokeMetadata);
                if (cdpResult.ok) {
                    log('POKE', `CDP notify: SUCCESS (${cdpResult.method})`);
                    if (!delivered) {
                        delivered = true;
                        pendingMsgs.forEach(m => {
                            m.status = 'poked';
                            broadcast('message_ack', { id: m.id, status: 'poked' });
                        });
                        saveState();
                        log('POKE', `Marked ${pendingMsgs.length} messages as poked via CDP fallback.`);
                    }
                } else {
                    log('POKE', `CDP notify: ${cdpResult.reason || cdpResult.error || 'failed'} (non-fatal, MemFlow is primary)`);
                }
            }
        } else {
            log('POKE', 'CDP notify: No IDE targets found (non-fatal)');
        }
    } catch (e) {
        log('POKE', `CDP notify error (non-fatal): ${e.message}`);
    }

    // Update agent status — scoped to the project that was just poked
    if (delivered) {
        updateAgentProject(finalProjectName, { state: 'working' });
        saveState();
        broadcast('agent_status', { ...STATE.agent, project: finalProjectName });
        stopRetry();
    }

    return delivered
        ? { ok: true, method: 'memflow_primary' }
        : { ok: false, error: 'all_delivery_failed' };
}

function stopRetry() {
    if (retryTimer) {
        clearInterval(retryTimer);
        retryTimer = null;
    }
    retryAttempts = 0;
}

function startRetry() {
    if (retryTimer) return;
    retryAttempts = 0;
    log('POKE', 'Agent busy. Starting retry loop...');
    retryTimer = setInterval(async () => {
        retryAttempts++;
        if (retryAttempts > 24) { // 2 minutes
            log('POKE', 'Retry limit reached. Giving up.');
            stopRetry();
            return;
        }
        await tryPoke(true);
    }, 5000);
}

async function tryPoke(isRetry = false) {
    if (pokeInFlight) return;

    // Throttle 2s
    if (Date.now() - lastPokeAt < 2000) return;

    pokeInFlight = true;
    lastPokeAt = Date.now();

    if (!isRetry) log('POKE', 'Attempting message delivery...');
    let res;
    try {
        res = await runPokeScript();
    } catch (e) {
        log('POKE', 'Fatal error during poke', e.message);
        res = { ok: false, error: 'fatal_error', details: e.message };
    } finally {
        pokeInFlight = false;
    }

    if (res.ok) {
        log('POKE', `Delivery complete: ${res.method}`);
        // runPokeScript already handles agent state and stopRetry
    } else {
        log('POKE', 'Delivery failed', res);
        if (res.error === 'all_delivery_failed' || res.error === 'fatal_error') {
            log('POKE', 'Message could not be delivered to MemFlow or IDE. Starting retry loop...');
            startRetry();
        } else {
            // Don't retry if MemFlow is working — the message is persisted there
            stopRetry();
        }
    }
}

function schedulePoke() {
    if (pokeInFlight) return;

    // Dedupe: If we are already retrying, we don't need to kickstart it.
    // However, if we aren't retrying, and a poke is not in flight, we should try.
    // The throttle in tryPoke handles the "too fast" case.
    if (retryTimer) {
        log('POKE', 'Skipping schedulePoke: Retry loop already active.');
        return;
    }

    tryPoke(false);
}

// --- State ---
// Persistent State
let STATE = {
    version: 1,
    strictMode: true,
    bridgeProductMode: 'vibe',
    approvals: [],
    messages: [],
    agent: { state: 'idle', lastSeen: null, task: '', note: '' },
    checkpoints: [],
    tokens: [], // Changed from optional to persisted for UX stability
    devices: [], // Rich device metadata: { id, token, label, claimedAt, claimedFrom, userAgent, lastSeenAt, lastSeenFrom, enabled }
    pairingCode: null
};

// Ephemeral State
let PAIRING_CODE = generateCode();
let TOKENS = new Set(); // Quick-lookup set for auth checks (only enabled tokens)
let DEVICES = []; // Full device objects — persisted as STATE.devices

// Autonomous Mode — session-scoped only, resets to false on every server restart.
// When true, incoming approval requests are auto-approved without waiting for
// a human decision. The Maitrix panel toggle writes this via POST /config/autonomous.
let AUTONOMOUS_MODE = false;

// Per-project agent state — ephemeral, never persisted.
// Keyed by project name (e.g. 'ag_bridge'). The agent POSTs to /agent/status
// with { state, task, note, project } so each workspace gets its own current task.
// The dashboard GET /agent/status uses STATE.targetProject to return the right one.
let AGENT_BY_PROJECT = {}; // { 'ag_bridge': { state, task, note, lastSeen }, ... }

function updateAgentProject(project, patch) {
    const key = project && project !== 'global' ? project : '__global__';
    const existing = AGENT_BY_PROJECT[key] || { state: 'idle', task: '', note: '', lastSeen: null };
    AGENT_BY_PROJECT[key] = {
        ...existing,
        ...patch,
        lastSeen: new Date().toISOString(),
    };
    // Also keep the global STATE.agent in sync for legacy consumers
    STATE.agent = { ...STATE.agent, ...patch, lastSeen: AGENT_BY_PROJECT[key].lastSeen };
    return AGENT_BY_PROJECT[key];
}

function getAgentStateForTarget() {
    // Resolve the currently selected target project name
    let key = '__global__';
    if (STATE.targetProject) {
        if (typeof STATE.targetProject === 'string') key = STATE.targetProject;
        else key = STATE.targetProject.projectName || STATE.targetProject.title || '__global__';
    }
    // Return project-scoped state if we have it, otherwise fall back to global
    return AGENT_BY_PROJECT[key] || AGENT_BY_PROJECT['__global__'] || STATE.agent;
}

let cachedProductType = null; // null = not yet detected

function normalizeProductMode(mode) {
    return mode === 'ide' || mode === 'vibe' ? mode : null;
}

function getBridgeProductMode() {
    return normalizeProductMode(STATE.bridgeProductMode)
        || normalizeProductMode(cachedProductType)
        || 'vibe';
}

async function getTargetsForProductMode(mode = getBridgeProductMode()) {
    const normalized = normalizeProductMode(mode) || getBridgeProductMode();
    return (await getAllTargets()).filter(t => !t.productType || t.productType === normalized);
}

async function updateCachedProductType() {
    try {
        const targets = await getAllTargets();
        if (targets && targets.length > 0) {
            const targetProj = STATE.targetProject;
            const targetProjName = typeof targetProj === 'string' ? targetProj : (targetProj?.projectName || targetProj?.title);
            if (targetProjName && targetProjName !== 'global') {
                const match = targets.find(t => t.projectName === targetProjName || (t.title && t.title.includes(targetProjName)));
                if (match && match.productType) {
                    cachedProductType = match.productType;
                    return;
                }
            }
            const preferredMode = normalizeProductMode(STATE.bridgeProductMode);
            const preferredType = preferredMode ? targets.find(t => t.productType === preferredMode) : null;
            if (preferredType) {
                cachedProductType = preferredType.productType;
                return;
            }
            const withType = targets.find(t => t.productType);
            if (withType) {
                cachedProductType = withType.productType;
                return;
            }
        }
    } catch (e) {
        log('PRODUCT_DETECT', 'Error scanning targets:', e.message);
    }
    // Fallback: check if any Antigravity product process is running directly
    try {
        const running = await getRunningProductType();
        if (running) cachedProductType = running;
    } catch (e) {
        log('PRODUCT_DETECT', 'Error in process fallback:', e.message);
    }
}
updateCachedProductType();
setInterval(updateCachedProductType, 5000);

// --- Helpers ---
function generateCode() {
    const code = Math.floor(100000 + Math.random() * 900000).toString();
    try {
        const { writeFileSync } = require('fs');
        writeFileSync(join(DATA_DIR, 'code.txt'), code);
    } catch (e) {
        // use dynamic import for fs/promises if sync fails in module
        import('fs/promises').then(fs => fs.writeFile(join(DATA_DIR, 'code.txt'), code)).catch(()=>{});
    }
    return code;
}

function generateToken() {
    return crypto.randomBytes(16).toString('hex');
}

function getLocalIPs() {
    const nets = networkInterfaces();
    const results = new Set();
    for (const name of Object.keys(nets)) {
        for (const net of nets[name]) {
            // Skip internal (non-127.0.0.1) and non-IPv4
            if (net.family === 'IPv4' && !net.internal) {
                // Filter out Tailscale IPs (100.x.x.x) from the "Local" list
                if (!net.address.startsWith('100.')) {
                    results.add(net.address);
                }
            }
        }
    }
    return Array.from(results);
}

function getTailscaleInfo() {
    // Try standard PATH first, then fallback to Mac App Store location
    const cmds = [
        'tailscale status --json',
        '/Applications/Tailscale.app/Contents/MacOS/Tailscale status --json'
    ];
    
    for (const cmd of cmds) {
        try {
            const stdout = execSync(cmd, { encoding: 'utf-8', stdio: ['ignore', 'pipe', 'ignore'] });
            const status = JSON.parse(stdout);
            if (status.BackendState === 'Running') {
                const dnsName = status.Self.DNSName;
                const name = dnsName ? dnsName.replace(/\.$/, '') : null;
                const ips = status.TailscaleIPs || [];
                return { name, ips };
            }
        } catch (e) {
            // continue to next cmd
        }
    }
    return null;
}

function broadcast(event, payload) {
    let finalPayload = payload;
    if (event === 'agent_status') {
        finalPayload = { ...payload, product: cachedProductType, productMode: getBridgeProductMode() };
    }
    const msg = JSON.stringify({
        event,
        payload: finalPayload,
        ts: new Date().toISOString()
    });
    for (const client of wss.clients) {
        if (client.readyState === 1) { // OPEN
            client.send(msg);
        }
    }
}

// --- Logging ---
async function log(component, message, data = null) {
    const ts = new Date().toISOString();
    const line = `[${ts}] [${component}] ${message} ${data ? JSON.stringify(data) : ''}`;
    console.log(line);
    try {
        await appendFile(LOG_FILE, line + '\n');
    } catch (e) { /* ignore log errors */ }
}

// --- Persistence ---
let saveTimeout = null;
async function saveState() {
    if (saveTimeout) clearTimeout(saveTimeout);

    saveTimeout = setTimeout(async () => {
        try {
            const data = {
                version: STATE.version,
                strictMode: STATE.strictMode,
                bridgeProductMode: getBridgeProductMode(),
                // approvals: STATE.approvals, // Scoped to approvals.json now
                messages: STATE.messages,
                agent: STATE.agent,
                checkpoints: STATE.checkpoints,
                tokens: Array.from(TOKENS), // Legacy compat: keep flat token list
                devices: DEVICES, // Rich device metadata
                pairingCode: STATE.pairingCode
            };
            const tempFile = `${STATE_FILE}.tmp`;
            await writeFile(tempFile, JSON.stringify(data, null, 2));
            await rename(tempFile, STATE_FILE);
            log('PERSIST', 'State saved (config/msgs).');
        } catch (err) {
            log('PERSIST', 'Failed to save state:', err.message);
        }
    }, 250);
}

async function saveApprovals() {
    try {
        const tempFile = `${APPROVALS_FILE}.tmp`;
        await writeFile(tempFile, JSON.stringify(STATE.approvals, null, 2));
        await rename(tempFile, APPROVALS_FILE);
        log('PERSIST', `Approvals saved (${STATE.approvals.length}).`);
    } catch (err) {
        log('PERSIST', 'Failed to save approvals:', err.message);
    }
}

async function loadPolicy() {
    try {
        const raw = await readFile(POLICY_FILE, 'utf-8');
        POLICY = JSON.parse(raw);
        log('POLICY', 'Loaded policy.json');
    } catch (err) {
        log('POLICY', 'policy.json not found or invalid. Using defaults.');
    }
}

async function loadState() {
    try {
        await mkdir(DATA_DIR, { recursive: true });
        const raw = await readFile(STATE_FILE, 'utf-8');
        const data = JSON.parse(raw);

        if (data.version) STATE.version = data.version;
        if (typeof data.strictMode === 'boolean') STATE.strictMode = data.strictMode;
        if (normalizeProductMode(data.bridgeProductMode)) STATE.bridgeProductMode = data.bridgeProductMode;
        // if (Array.isArray(data.approvals)) STATE.approvals = data.approvals; // Legacy load
        if (Array.isArray(data.messages)) STATE.messages = data.messages;
        if (data.agent) STATE.agent = data.agent;
        if (Array.isArray(data.checkpoints)) STATE.checkpoints = data.checkpoints;
        if (Array.isArray(data.tokens)) {
            STATE.tokens = data.tokens;
            // Load rich device metadata (or migrate from flat tokens)
            if (Array.isArray(data.devices) && data.devices.length > 0) {
                DEVICES = data.devices;
            } else {
                // Migration: convert plain string tokens → device objects
                log('PERSIST', `Migrating ${data.tokens.length} legacy tokens to device objects`);
                DEVICES = data.tokens.map((tok, idx) => ({
                    id: `dev_${crypto.randomBytes(4).toString('hex')}`,
                    token: tok,
                    label: `Device ${idx + 1} (migrated)`,
                    claimedAt: new Date().toISOString(),
                    claimedFrom: 'unknown (pre-migration)',
                    userAgent: 'unknown',
                    lastSeenAt: null,
                    lastSeenFrom: null,
                    enabled: true
                }));
            }
            // Build TOKENS set from enabled devices only
            TOKENS = new Set(DEVICES.filter(d => d.enabled).map(d => d.token));
        }
        if (data.pairingCode) {
            STATE.pairingCode = data.pairingCode;
            PAIRING_CODE = data.pairingCode;
            try {
                const { writeFileSync } = await import('fs');
                writeFileSync(join(DATA_DIR, 'code.txt'), PAIRING_CODE);
            } catch (e) {
                // ignore
            }
        } else {
            STATE.pairingCode = PAIRING_CODE; // whatever was generated at startup
            saveState();
        }

        // Load Approvals (Separate File)
        try {
            const rawApprovals = await readFile(APPROVALS_FILE, 'utf-8');
            const approvalsData = JSON.parse(rawApprovals);
            if (Array.isArray(approvalsData)) {
                STATE.approvals = approvalsData;
            }
        } catch (e) {
            if (e.code === 'ENOENT') {
                // Migration: Check if state.json had approvals
                if (Array.isArray(data.approvals) && data.approvals.length > 0) {
                    log('PERSIST', 'Migrating approvals from state.json to approvals.json');
                    STATE.approvals = data.approvals;
                    await saveApprovals();
                }
            } else {
                log('PERSIST', 'Failed to load approvals.json', e.message);
            }
        }

        console.log(`[PERSIST] State loaded. ${STATE.approvals.length} approvals, ${TOKENS.size} tokens.`);
    } catch (err) {
        if (err.code === 'ENOENT') {
            log('PERSIST', 'No state file found. Starting fresh.');
            await saveState();
        } else {
            log('PERSIST', 'Failed to load state:', err.message);
            // Logic to rename bad file could go here, but simple logging is fine for v0.2
            const badFile = `${STATE_FILE}.bad.${Date.now()}`;
            try {
                await rename(STATE_FILE, badFile);
                log('PERSIST', `Corrupt state file renamed to ${badFile}`);
            } catch (e) { /* ignore */ }
        }
    }
}

function checkPolicy(cmd) {
    if (!cmd) return { allowed: false, error: 'missing_command' };

    // 1. Global Deny (Always wins)
    for (const pattern of POLICY.globalDeny || []) {
        if (new RegExp(pattern).test(cmd)) {
            return { allowed: false, error: 'global_denied' };
        }
    }

    // 2. Determine Profile (Map v0.5 boolean to v0.6 profiles)
    // strictMode=true -> 'balanced', strictMode=false -> 'relaxed'
    // Future: STATE.securityProfile could hold 'paranoid' etc.
    const profileName = STATE.strictMode ? 'balanced' : 'relaxed';
    const profile = POLICY.profiles?.[profileName];

    if (!profile) {
        // Fallback safety: if profile invalid, BLOCK ALL unless relaxed was intended?
        // Better to be safe.
        return { allowed: false, error: 'invalid_policy_profile' };
    }

    // 3. Profile Deny
    for (const pattern of profile.deny || []) {
        if (new RegExp(pattern).test(cmd)) {
            return { allowed: false, error: 'profile_denied' };
        }
    }

    // 4. Profile Allow
    for (const pattern of profile.allow || []) {
        if (new RegExp(pattern).test(cmd)) {
            return { allowed: true };
        }
    }

    return { allowed: false, error: 'command_not_allowlisted' };
}

// --- Middleware ---
app.use(express.json());

// HTTPS redirect: if request arrives on raw port 8787 without going through
// tailscale serve (which injects X-Forwarded-Proto: https), redirect to https.
// Localhost and MCP clients are exempt.
let _tailscaleHostname = null;
function getTailscaleHostname() {
    if (_tailscaleHostname) return _tailscaleHostname;
    const ts = getTailscaleInfo();
    _tailscaleHostname = ts ? ts.name : null;
    return _tailscaleHostname;
}
app.use((req, res, next) => {
    const ip = req.ip || req.connection.remoteAddress || '';
    const isLocal = ip === '::1' || ip === '127.0.0.1' || ip.startsWith('::ffff:127.');
    const isHttps = req.headers['x-forwarded-proto'] === 'https';
    // Only redirect non-local, non-HTTPS GET/HEAD requests (not API calls with tokens)
    if (!isLocal && !isHttps && (req.method === 'GET' || req.method === 'HEAD')) {
        const hostname = getTailscaleHostname();
        if (hostname) {
            const url = `https://${hostname}${req.originalUrl}`;
            log('HTTPS', `Redirecting ${ip} to ${url}`);
            return res.redirect(301, url);
        }
    }
    next();
});

app.use(express.static('public'));

const requireAuth = (req, res, next) => {
    const ip = req.ip || req.connection.remoteAddress;
    const token = req.headers['x-ag-token'];

    // Allow localhost (MCP server) to bypass auth
    if (ip === '::1' || ip === '127.0.0.1' || ip === '::ffff:127.0.0.1') {
        req.authSource = token ? 'user' : 'agent';
        return next();
    }

    if (!token || !TOKENS.has(token)) {
        return res.status(401).json({ error: 'Unauthorized' });
    }

    // Track device usage metadata
    const device = DEVICES.find(d => d.token === token);
    if (device) {
        device.lastSeenAt = new Date().toISOString();
        device.lastSeenFrom = ip;
        // Capture user agent on first real request if not already set
        if (req.headers['user-agent'] && (!device.userAgent || device.userAgent === 'unknown')) {
            device.userAgent = req.headers['user-agent'];
        }
    }

    req.authSource = 'user';
    next();
};

const checkAuth = requireAuth; // Alias for consistency with new endpoints

// --- HTTP Endpoints ---

// Public
app.get('/health', (req, res) => {
    res.json({ ok: true, name: "ag-bridge-cpu", version: APP_VERSION, ts: new Date().toISOString() });
});

app.get('/debug/code', (req, res) => {
    const ip = req.ip || req.connection.remoteAddress;
    if (ip === '::1' || ip === '127.0.0.1' || ip === '::ffff:127.0.0.1') {
        res.json({ code: PAIRING_CODE });
    } else {
        res.status(403).json({ error: 'localhost_only' });
    }
});

const claimAttempts = new Map();
app.post('/pair/claim', (req, res) => {
    const ip = req.ip || req.connection.remoteAddress;
    const now = Date.now();
    const attempt = claimAttempts.get(ip) || { count: 0, time: now };
    
    // Reset after 10 minutes
    if (now - attempt.time > 10 * 60 * 1000) {
        attempt.count = 0;
        attempt.time = now;
    }
    
    attempt.count++;
    claimAttempts.set(ip, attempt);
    
    if (attempt.count > 10) {
        return res.status(429).json({ error: 'too_many_attempts', message: 'Please wait 10 minutes before trying again.' });
    }

    const { code } = req.body;
    if (!code || code !== PAIRING_CODE) {
        return res.status(403).json({ error: 'invalid_code' });
    }
    // Success - clear attempts
    claimAttempts.delete(ip);
    
    const token = generateToken();
    const ua = req.headers['user-agent'] || 'unknown';
    const deviceLabel = ua.includes('iPhone') ? 'iPhone'
        : ua.includes('iPad') ? 'iPad'
        : ua.includes('Android') ? 'Android'
        : ua.includes('Mac') ? 'Mac Browser'
        : ua.includes('Windows') ? 'Windows Browser'
        : 'Unknown Device';

    const device = {
        id: `dev_${crypto.randomBytes(4).toString('hex')}`,
        token,
        label: `${deviceLabel} (${new Date().toLocaleDateString()})`,
        claimedAt: new Date().toISOString(),
        claimedFrom: ip,
        userAgent: ua,
        lastSeenAt: new Date().toISOString(),
        lastSeenFrom: ip,
        enabled: true
    };

    DEVICES.push(device);
    TOKENS.add(token);
    saveState(); // Save new device
    console.log(`[AUTH] New device paired: ${device.id} (${device.label}) from ${ip}`);
    res.json({ token });
});

// Protected
app.get('/config', requireAuth, (req, res) => {
    res.json({
        ok: true,
        strictMode: STATE.strictMode,
        autonomousMode: AUTONOMOUS_MODE,
        bridgeProductMode: getBridgeProductMode(),
        detectedProduct: cachedProductType,
        ts: new Date().toISOString()
    });
});

app.post('/config/strict-mode', requireAuth, (req, res) => {
    const { strictMode } = req.body;
    if (typeof strictMode !== 'boolean') {
        return res.status(400).json({ error: 'invalid_input' });
    }
    STATE.strictMode = strictMode;
    saveState();
    console.log(`[CONFIG] Strict Mode set to ${strictMode}`);
    broadcast('config_changed', { strictMode });
    res.json({ ok: true, strictMode });
});

app.post('/config/product-mode', requireAuth, (req, res) => {
    const mode = normalizeProductMode(req.body?.mode);
    if (!mode) {
        return res.status(400).json({ error: 'invalid_input', hint: '{ mode: "ide" | "vibe" }' });
    }
    STATE.bridgeProductMode = mode;
    saveState();
    log('CONFIG', `Bridge product mode set to ${mode}`);
    broadcast('config_changed', { bridgeProductMode: mode, detectedProduct: cachedProductType });
    res.json({ ok: true, bridgeProductMode: mode, detectedProduct: cachedProductType });
});

// Autonomous Mode toggle — session-scoped, never written to disk.
// Used by the Maitrix mobile panel to let the agent run without interruption.
app.post('/config/autonomous', requireAuth, (req, res) => {
    const { enabled } = req.body;
    if (typeof enabled !== 'boolean') {
        return res.status(400).json({ error: 'invalid_input', hint: '{ enabled: true|false }' });
    }
    AUTONOMOUS_MODE = enabled;
    console.log(`[CONFIG] Autonomous Mode ${enabled ? 'ENABLED' : 'DISABLED'}`);
    broadcast('config_changed', { autonomousMode: AUTONOMOUS_MODE });
    res.json({ ok: true, autonomousMode: AUTONOMOUS_MODE });
});

// Accept All — bulk-approve every pending approval in a single tap.
// Primary action for the Maitrix mobile panel.
app.post('/approvals/accept-all', requireAuth, async (req, res) => {
    const pending = STATE.approvals.filter(a => a.status === 'pending');
    if (pending.length === 0) {
        return res.json({ ok: true, approved: 0, message: 'No pending approvals' });
    }

    const now = new Date().toISOString();
    for (const approval of pending) {
        approval.status = 'approved';
        approval.decidedAt = now;
        approval.bulkApproved = true;

        const msg = STATE.messages.find(m => m.approvalId === approval.id);
        if (msg) msg.approvalStatus = 'approved';

        broadcast('approval_decided', { id: approval.id, status: 'approved', bulk: true });
        if (msg) broadcast('message_update', msg);
        console.log(`[APPROVAL] ${approval.id} BULK APPROVED`);
    }

    saveApprovals();
    saveState();

    res.json({ ok: true, approved: pending.length });
});

// Migrated to usage of single /status endpoint below
// app.get('/status', requireAuth, ...);

app.get('/approvals', requireAuth, (req, res) => {
    const sorted = [...STATE.approvals].sort((a, b) => new Date(b.createdAt) - new Date(a.createdAt));
    res.json({ approvals: sorted });
});

app.post('/approvals/:id/approve', requireAuth, async (req, res) => {
    const { id } = req.params;
    const { always } = req.body || {};
    const approval = STATE.approvals.find(a => a.id === id);
    if (!approval) return res.status(404).json({ error: 'not_found' });

    if (approval.status !== 'pending') {
        return res.status(409).json({ error: 'already_decided', approval });
    }

    approval.status = 'approved';
    approval.decidedAt = new Date().toISOString();
    approval.approvedBy = req.authSource || 'user';
    
    // Allow Always Logic
    if (always && approval.details && approval.details.cmd) {
        const cmdPattern = "^" + approval.details.cmd.replace(/[.*+?^${}()|[\]\\]/g, '\\$&') + "$";
        const profileName = STATE.strictMode ? 'balanced' : 'relaxed';
        if (!POLICY.profiles) POLICY.profiles = { balanced: { allow: [] }, relaxed: { allow: [] } };
        if (!POLICY.profiles[profileName]) POLICY.profiles[profileName] = { allow: [] };
        if (!POLICY.profiles[profileName].allow.includes(cmdPattern)) {
            POLICY.profiles[profileName].allow.push(cmdPattern);
            await writeFile(POLICY_FILE, JSON.stringify(POLICY, null, 2));
            console.log(`[POLICY] Added ${cmdPattern} to ${profileName} allowlist`);
        }
    }

    const msg = STATE.messages.find(m => m.approvalId === id);
    if (msg) {
        msg.approvalStatus = 'approved';
        msg.approvedBy = req.authSource || 'user';
        saveState();
    }

    saveApprovals();

    console.log(`[APPROVAL] ${id} APPROVED by ${approval.approvedBy}`);
    broadcast('approval_decided', { id, status: 'approved', approvedBy: approval.approvedBy });
    if (msg) broadcast('message_update', msg);
    res.json({ ok: true, approval });
});

app.post('/approvals/:id/deny', requireAuth, (req, res) => {
    const { id } = req.params;
    const approval = STATE.approvals.find(a => a.id === id);
    if (!approval) return res.status(404).json({ error: 'not_found' });

    if (approval.status !== 'pending') {
        return res.status(409).json({ error: 'already_decided', approval });
    }

    approval.status = 'denied';
    approval.decidedAt = new Date().toISOString();
    approval.approvedBy = req.authSource || 'user';
    
    const msg = STATE.messages.find(m => m.approvalId === id);
    if (msg) {
        msg.approvalStatus = 'denied';
        msg.approvedBy = req.authSource || 'user';
        saveState();
    }

    saveApprovals();

    console.log(`[APPROVAL] ${id} DENIED by ${approval.approvedBy}`);
    broadcast('approval_decided', { id, status: 'denied', approvedBy: approval.approvedBy });
    if (msg) broadcast('message_update', msg);
    res.json({ ok: true, approval });
});

app.post('/debug/create-approval', requireAuth, (req, res) => {
    const { kind, details } = req.body;
    const newApproval = {
        id: `appr_${crypto.randomBytes(4).toString('hex')}`,
        createdAt: new Date().toISOString(),
        kind: kind || 'command',
        details: details || { cmd: 'echo "Hello World"', risk: 'low' },
        status: 'pending',
        decidedAt: null
    };

    STATE.approvals.push(newApproval);
    saveApprovals();

    const targetId = STATE.targetProject?.id || STATE.targetProject?.title || (typeof STATE.targetProject === 'string' ? STATE.targetProject : 'global');
    const msgText = kind === 'command' ? `Approval Required: ${details?.cmd}` : `Approval Required: ${kind}`;
    const msg = {
        id: 'msg_appr_' + newApproval.id,
        createdAt: newApproval.createdAt,
        from: 'agent',
        to: 'user',
        channel: 'approval',
        text: msgText,
        status: 'new',
        targetId: targetId,
        approvalId: newApproval.id
    };
    STATE.messages.push(msg);
    if (STATE.messages.length > 200) STATE.messages.shift();
    saveState();

    console.log(`[DEBUG] Created test approval ${newApproval.id}`);
    
    // Write approval request to MemFlow outbox
    memflowWriteResponse(msgText, {
        id: newApproval.id,
        project: targetId,
        channel: 'approval',
        from: 'agent',
        actorId: `agent_${targetId}`
    }).catch(err => console.error('[MEMFLOW] Failed to write debug approval to MemFlow:', err));

    broadcast('approval_requested', newApproval);
    broadcast('message_new', msg);
    res.json(newApproval);
});

// --- New v0.3 Endpoints ---

// POST /messages/send
app.post('/messages/send', checkAuth, async (req, res) => {
    console.log('[DEBUG] HEX DUMP /messages/send body:', JSON.stringify(req.body));
    const { to, channel, text, project, senderAlias } = req.body;
    let { from } = req.body;
    from = from || 'user'; // Default to user if missing

    if (!to || !text) return res.status(400).json({ ok: false, error: 'missing_fields' });
    
    // Auto-align targetProject when user submits a message from the mobile UI
    if (from === 'user' && project) {
        const targets = await getTargetsForProductMode();
        const found = targets.find(t => t.id === project || t.title?.includes(project) || t.url?.includes(project) || t.projectName === project);
        STATE.targetProject = found || { title: project, projectName: project, connectorId: 'antigravity' };
        saveState();
    }

    // Allow user to forcefully unstick the agent UI state via Quick Actions
    if (text.toUpperCase().includes('ABORT') || text.toUpperCase().includes('STOP')) {
        STATE.agent.state = 'idle';
        STATE.agent.task = 'Execution forcefully aborted.';
        STATE.agent.note = '';
        broadcast('agent_status', STATE.agent);
    }

    let defaultTargetId = 'global';
    if (STATE.targetProject) {
        if (typeof STATE.targetProject === 'string') {
            defaultTargetId = STATE.targetProject;
        } else if (STATE.targetProject.projectName) {
            defaultTargetId = STATE.targetProject.projectName;
        } else if (STATE.targetProject.title) {
            defaultTargetId = STATE.targetProject.title.replace(' - Visual Studio Code', '').split(' — ')[0].trim();
        }
    }
    
    let targetId = project || defaultTargetId;
    
    // Self-healing lane alignment: Inherit the last user message's targetId if agent replies without project context
    if (from === 'agent' && !project) {
        const lastUserMsg = STATE.messages.slice().reverse().find(m => m.from === 'user');
        if (lastUserMsg && lastUserMsg.targetId) {
            targetId = lastUserMsg.targetId;
        }
    }

    const prefix = senderAlias ? `[${senderAlias}] ` : '[Mobile] ';

    const msg = {
        id: 'msg_' + Date.now().toString(36) + Math.random().toString(36).substr(2, 5),
        createdAt: new Date().toISOString(),
        from,
        to, // 'agent' or 'user'
        channel: channel || 'general',
        text: (from === 'user' ? prefix : '') + text,
        status: 'new',
        targetId
    };

    STATE.messages.push(msg);
    // Cap history at 200
    if (STATE.messages.length > 200) STATE.messages.shift();
    saveState();

    broadcast('message_new', msg);

    // Trigger Poke if msg is for agent
    if (to === 'agent') {
        schedulePoke();
    } else if (to === 'user' && from === 'agent') {
        // If agent uses the HTTP API instead of MCP tool, we still need to write it to MemFlow!
        memflowWriteResponse(text, {
            id: msg.id.replace(/^msg_(mf_)?/, ''),
            project: targetId,
            channel: channel || 'work',
            from: 'agent',
            actorId: `agent_${targetId}`
        }).catch(err => console.error('[MEMFLOW] Failed to write API response to MemFlow:', err));
    }

    res.json({ ok: true, message: msg });
});

// GET /messages/inbox
app.get('/messages/inbox', checkAuth, (req, res) => {
    const { to, status, limit, filterByProject } = req.query;
    let items = STATE.messages;

    if (to) items = items.filter(m => m.to === to);
    if (status) items = items.filter(m => m.status === status);
    if (filterByProject) {
        items = items.filter(m => {
            if (!m.targetId) return false;
            const fp = String(filterByProject).trim();
            const mt = String(m.targetId).trim();
            // Strict exact match only — no fuzzy endsWith to prevent cross-project leakage
            return mt === fp;
        });
    }

    // Sort newest first
    items = [...items].sort((a, b) => new Date(b.createdAt) - new Date(a.createdAt));

    if (limit) items = items.slice(0, parseInt(limit));

    res.json({ ok: true, messages: items });
});

// POST /messages/:id/ack
app.post('/messages/:id/ack', checkAuth, (req, res) => {
    const { id } = req.params;
    const { status } = req.body; // 'read' or 'done'

    const msg = STATE.messages.find(m => m.id === id);
    if (!msg) return res.status(404).json({ ok: false, error: 'not_found' });

    msg.status = status || 'read';
    saveState();

    broadcast('message_ack', { id, status: msg.status });
    res.json({ ok: true });
});

// POST /agent/heartbeat — called programmatically by agent tooling
// POST /agent/status   — called by the system prompt injected into the IDE
// Both accept: { state, task, note, project }
// 'project' tags which workspace this update belongs to.
function handleAgentUpdate(req, res) {
    const { state, task, note, project } = req.body;

    // Resolve the project name from the body, or fall back to current target
    const targetKey = project
        || (STATE.targetProject && (STATE.targetProject.projectName || STATE.targetProject.title))
        || '__global__';

    const updated = updateAgentProject(targetKey, {
        state: state || undefined,
        task: task !== undefined ? task : undefined,
        note: note !== undefined ? note : undefined,
    });

    saveState();
    broadcast('agent_status', { ...updated, project: targetKey });
    res.json({ ok: true, agent: { ...updated, project: targetKey } });
}

app.post('/agent/heartbeat', checkAuth, handleAgentUpdate);
app.post('/agent/status',    checkAuth, handleAgentUpdate);

// GET /agent/status — returns task scoped to the currently selected target project
app.get('/agent/status', checkAuth, (req, res) => {
    const scoped = getAgentStateForTarget();
    const targetKey = STATE.targetProject
        ? (STATE.targetProject.projectName || STATE.targetProject.title || null)
        : null;
    res.json({
        ok: true,
        agent: {
            ...scoped,
            project: targetKey,
            product: cachedProductType,
            productMode: getBridgeProductMode(),
            autonomousMode: AUTONOMOUS_MODE
        },
        connection: {
            wsClients: wss.clients.size,          // how many browsers are connected
            mongodbPollActive: !!memflowPollTimer,  // is the MongoDB poll running?
            lastPollSuccess: new Date(lastPollSuccess).toISOString()
        }
    });
});

// GET /ping/echo — round-trip test: server receives, broadcasts WS event back to all clients
// The dashboard listens for 'ping_ack' and measures RTT.
app.get('/ping/echo', checkAuth, (req, res) => {
    const ts = Date.now();
    broadcast('ping_ack', { ts, serverTime: new Date().toISOString(), wsClients: wss.clients.size });
    res.json({ ok: true, ts, wsClients: wss.clients.size });
});


// GET /projects
app.get('/projects', checkAuth, async (req, res) => {
    try {
        // Assume projects are stored in the parent directory of ag_bridge
        const projectsDir = dirname(__dirname);
        const { readdir, stat } = await import('fs/promises');
        const path = await import('path');
        const items = await readdir(projectsDir, { withFileTypes: true });
        const projects = items
            .filter(dirent => dirent.isDirectory() && !dirent.name.startsWith('.'))
            .map(dirent => dirent.name);
            
        // Calculate recent activity
        const projectActivity = {};
        for (const msg of STATE.messages) {
            if (msg.targetId) {
                const tId = msg.targetId.split('/').pop();
                if (!projectActivity[tId] || new Date(msg.createdAt) > new Date(projectActivity[tId])) {
                    projectActivity[tId] = msg.createdAt;
                }
            }
        }
        
        // Fallback to actual file system modification time if no messages exist
        for (const proj of projects) {
            if (!projectActivity[proj]) {
                try {
                    const stats = await stat(path.join(projectsDir, proj));
                    projectActivity[proj] = stats.mtime.toISOString();
                } catch(e) {}
            }
        }

        // Sort projects by recent activity (newest first)
        projects.sort((a, b) => {
            const timeA = projectActivity[a] ? new Date(projectActivity[a]).getTime() : 0;
            const timeB = projectActivity[b] ? new Date(projectActivity[b]).getTime() : 0;
            return timeB - timeA;
        });
            
        // Use Plugin Architecture to scan for Active Windows. Product mode is a
        // hard preference so the UI can switch between Antigravity IDE and Vibe.
        const bridgeProductMode = getBridgeProductMode();
        const allActiveWindows = await getAllTargets();
        const activeWindows = allActiveWindows.filter(w =>
            !w.productType || w.productType === bridgeProductMode
        );
        
        // Sort active windows by EXISTING activity BEFORE stamping them.
        // Stamping all with the same "now" makes them equal — sort first, stamp second.
        activeWindows.sort((a, b) => {
            const keyA = a.projectName || a.title;
            const keyB = b.projectName || b.title;
            const tA = projectActivity[keyA] ? new Date(projectActivity[keyA]).getTime() : 0;
            const tB = projectActivity[keyB] ? new Date(projectActivity[keyB]).getTime() : 0;
            return tB - tA;
        });
        
        // Now stamp with "now" so the activity dict reflects open windows,
        // but use tiny descending offsets to preserve sort order after stamping.
        const nowMs = Date.now();
        activeWindows.forEach((w, i) => {
            const key = w.projectName || w.title;
            if (key && key !== 'Launchpad') {
                projectActivity[key] = new Date(nowMs - i).toISOString(); // -i ms offset preserves order
            }
        });
        
        // ── History-inferred projects (fallback only) ─────────────────────────
        // Only add inferred-from-history entries when the live connector scan
        // returned ZERO results. CDP-only Antigravity.app windows are live targets
        // too, so they should not trigger stale history backfill.
        const hasLiveTargets = activeWindows.length > 0;
        const oneDayAgo = Date.now() - (24 * 60 * 60 * 1000);

        if (!hasLiveTargets) {
            for (const [proj, timeStr] of Object.entries(projectActivity)) {
                if (new Date(timeStr).getTime() > oneDayAgo) {
                    if (proj && proj !== 'global' && proj !== '.memflow') {
                        const exists = activeWindows.find(w =>
                            w.id === proj || w.title === proj || (w.projectName && w.projectName === proj)
                        );
                        if (!exists && projects.includes(proj)) {
                            activeWindows.push({
                                id: proj,
                                title: proj,
                                connectorId: 'antigravity',
                                productType: cachedProductType || 'ide',
                                type: 'inferred_offline',
                                source: 'history',
                            });
                        }
                    }
                }
            }
        }

        
        // Deduplicate activeWindows by projectName (keep the most recently active tab per project)
        const seenProjects = new Map();
        for (const w of activeWindows) {
            const key = w.projectName || w.title;
            if (!seenProjects.has(key)) {
                seenProjects.set(key, w);
            }
        }
        const dedupedWindows = Array.from(seenProjects.values());

        // Sort active windows by recent activity — use projectName as the lookup key
        dedupedWindows.sort((a, b) => {
            const keyA = a.projectName || a.title;
            const keyB = b.projectName || b.title;
            const tA = projectActivity[keyA] ? new Date(projectActivity[keyA]).getTime() : 0;
            const tB = projectActivity[keyB] ? new Date(projectActivity[keyB]).getTime() : 0;
            return tB - tA;
        });
        
        const normProject = (value) => String(value || '').trim().toLowerCase();
        const openProjectNames = new Set();
        for (const w of dedupedWindows) {
            const key = w.projectName || w.title;
            if (key && key !== 'Launchpad' && projects.includes(key)) {
                openProjectNames.add(normProject(key));
            }
        }

        let activeAgents = [];
        try {
            activeAgents = await memflowGetActiveAgents();
        } catch (_) {}
        const heartbeatingProjectNames = new Set(
            activeAgents
                .map(a => a && a.project)
                .filter(p => p && projects.includes(p))
                .map(normProject)
        );

        // Build projectsGrouped: every known project is selectable; live/open
        // projects are marked active independently of whether they have messages.
        const projectsGroupedMap = {};
        // Initialize all known project folders
        for (const proj of projects) {
            const isOpen = openProjectNames.has(normProject(proj));
            const hasActiveAgent = heartbeatingProjectNames.has(normProject(proj));
            projectsGroupedMap[proj] = {
                name: proj,
                conversations: [],
                lastActivity: projectActivity[proj] || null,
                messageCount: 0,
                hasActiveConversations: isOpen || hasActiveAgent,
                isOpen,
                hasActiveAgent,
                isSelectable: true,
                canRemoteWork: true,
                status: isOpen ? 'open' : (hasActiveAgent ? 'agent' : 'closed')
            };
        }
        // Count messages per project
        for (const msg of STATE.messages) {
            if (msg.targetId) {
                const tId = msg.targetId.split('/').pop();
                if (projectsGroupedMap[tId]) {
                    projectsGroupedMap[tId].messageCount++;
                }
            }
        }
        // Group targets into projects
        for (const w of dedupedWindows) {
            // Try to match target to a project folder
            const pName = w.projectName || w.title || '';
            let projKey = null;
            // 1. Exact match by projectName or title
            if (pName && projectsGroupedMap[pName]) {
                projKey = pName;
            }
            // 2. Check if title contains a project folder name
            if (!projKey && w.title) {
                for (const folder of projects) {
                    if (w.title === folder || w.title.startsWith(folder + ' —') || w.title.startsWith(folder + ' -') || w.title.includes('/' + folder + '/')) {
                        projKey = folder;
                        break;
                    }
                }
            }
            const bucket = projKey || '📡 Global';
            if (!projectsGroupedMap[bucket]) {
                const isGlobalBucket = bucket === '📡 Global';
                projectsGroupedMap[bucket] = {
                    name: bucket,
                    conversations: [],
                    lastActivity: null,
                    messageCount: 0,
                    hasActiveConversations: false,
                    isOpen: false,
                    hasActiveAgent: false,
                    isSelectable: !isGlobalBucket,
                    canRemoteWork: !isGlobalBucket,
                    status: isGlobalBucket ? 'monitor' : 'closed'
                };
            }
            projectsGroupedMap[bucket].conversations.push({
                conversationId: w.conversationId || null,
                id: w.id || null,
                title: w.title || w.id,
                projectName: projKey || pName,
                port: w.port || null,
                url: w.url || null,
                isConversation: w.isConversation || false
            });
            projectsGroupedMap[bucket].hasActiveConversations = true;
            projectsGroupedMap[bucket].isOpen = true;
            projectsGroupedMap[bucket].status = bucket === '📡 Global' ? 'monitor' : 'open';
            // Update lastActivity if this window's activity is newer
            const wKey = w.projectName || w.title;
            const wActivity = projectActivity[wKey] || null;
            if (wActivity) {
                if (!projectsGroupedMap[bucket].lastActivity || new Date(wActivity) > new Date(projectsGroupedMap[bucket].lastActivity)) {
                    projectsGroupedMap[bucket].lastActivity = wActivity;
                }
            }
        }
        // Sort: active-conversation projects first (newest activity on top), inactive at bottom
        const projectsGrouped = Object.values(projectsGroupedMap).sort((a, b) => {
            if (a.hasActiveConversations !== b.hasActiveConversations) {
                return a.hasActiveConversations ? -1 : 1;
            }
            const tA = a.lastActivity ? new Date(a.lastActivity).getTime() : 0;
            const tB = b.lastActivity ? new Date(b.lastActivity).getTime() : 0;
            return tB - tA;
        });

        res.json({
            ok: true,
            projectsGrouped,
            projects,
            activeWindows: dedupedWindows,
            allActiveWindows,
            bridgeProductMode,
            detectedProduct: cachedProductType,
            activity: projectActivity,
            selectedProject: STATE.targetProject
        });
    } catch (err) {
        res.status(500).json({ ok: false, error: err.message });
    }
});

// POST /projects/select
app.post('/projects/select', checkAuth, async (req, res) => {
    const { project } = req.body;
    if (!project) {
        STATE.targetProject = null;
    } else if (typeof project === 'string') {
        const targets = await getTargetsForProductMode();
        const found = targets.find(t => t.id === project || t.title?.includes(project) || t.url?.includes(project) || t.projectName === project);
        STATE.targetProject = found || { title: project, projectName: project, connectorId: 'antigravity', productType: getBridgeProductMode() };
    } else {
        STATE.targetProject = project;
    }
    saveState();
    res.json({ ok: true, selectedProject: STATE.targetProject });
});

// GET /status (Observability)
app.get('/status', requireAuth, (req, res) => {
    const pending = STATE.approvals.filter(a => a.status === 'pending').length;
    res.json({
        ok: true,
        version: APP_VERSION,
        ts: new Date().toISOString(),
        pendingApprovals: pending,
        totalApprovals: STATE.approvals.length,
        strictMode: STATE.strictMode,
        productMode: getBridgeProductMode(),
        detectedProduct: cachedProductType,
        cdp: {
            enabled: true, // v0.x assumption
            poke_in_flight: pokeInFlight,
            retry_active: !!retryTimer
        },
        agent: {
            state: STATE.agent.state,
            last_seen: STATE.agent.lastSeen,
            product: cachedProductType,
            productMode: getBridgeProductMode()
        },
        server: {
            uptime: process.uptime(),
            clients: wss.clients.size
        }
    });
});

// --- Device Management Endpoints ---

// GET /admin/devices — list all paired devices (token masked)
app.get('/admin/devices', checkAuth, (req, res) => {
    const masked = DEVICES.map(d => ({
        id: d.id,
        label: d.label,
        claimedAt: d.claimedAt,
        claimedFrom: d.claimedFrom,
        userAgent: d.userAgent,
        lastSeenAt: d.lastSeenAt,
        lastSeenFrom: d.lastSeenFrom,
        enabled: d.enabled,
        tokenPreview: d.token ? `${d.token.slice(0, 4)}…${d.token.slice(-4)}` : '?',
        isCurrentDevice: d.token === req.headers['x-ag-token']
    }));
    res.json({ ok: true, devices: masked, total: masked.length });
});

// POST /admin/devices/:id/revoke — disable a token
app.post('/admin/devices/:id/revoke', checkAuth, (req, res) => {
    const device = DEVICES.find(d => d.id === req.params.id);
    if (!device) return res.status(404).json({ error: 'not_found' });

    // Prevent revoking your own token
    if (device.token === req.headers['x-ag-token']) {
        return res.status(400).json({ error: 'cannot_revoke_self', message: 'Cannot revoke your own active session.' });
    }

    device.enabled = false;
    TOKENS.delete(device.token);
    saveState();

    // Force-close any WebSocket connections using this token
    wss.clients.forEach(ws => {
        if (ws._agToken === device.token) ws.terminate();
    });

    log('AUTH', `Device ${device.id} (${device.label}) REVOKED`);
    res.json({ ok: true, device: { id: device.id, label: device.label, enabled: false } });
});

// POST /admin/devices/:id/enable — re-enable a revoked token
app.post('/admin/devices/:id/enable', checkAuth, (req, res) => {
    const device = DEVICES.find(d => d.id === req.params.id);
    if (!device) return res.status(404).json({ error: 'not_found' });

    device.enabled = true;
    TOKENS.add(device.token);
    saveState();

    log('AUTH', `Device ${device.id} (${device.label}) RE-ENABLED`);
    res.json({ ok: true, device: { id: device.id, label: device.label, enabled: true } });
});

// POST /admin/devices/:id/rename — set a friendly label
app.post('/admin/devices/:id/rename', checkAuth, (req, res) => {
    const device = DEVICES.find(d => d.id === req.params.id);
    if (!device) return res.status(404).json({ error: 'not_found' });

    const { label } = req.body;
    if (!label || typeof label !== 'string' || label.trim().length === 0) {
        return res.status(400).json({ error: 'invalid_label' });
    }

    device.label = label.trim().slice(0, 50); // Max 50 chars
    saveState();

    log('AUTH', `Device ${device.id} renamed to "${device.label}"`);
    res.json({ ok: true, device: { id: device.id, label: device.label } });
});

// DELETE /admin/devices/:id — permanently remove a device
app.delete('/admin/devices/:id', checkAuth, (req, res) => {
    const idx = DEVICES.findIndex(d => d.id === req.params.id);
    if (idx === -1) return res.status(404).json({ error: 'not_found' });

    const device = DEVICES[idx];
    // Prevent deleting your own token
    if (device.token === req.headers['x-ag-token']) {
        return res.status(400).json({ error: 'cannot_delete_self' });
    }

    TOKENS.delete(device.token);
    DEVICES.splice(idx, 1);
    saveState();

    // Force-close any WebSocket connections using this token
    wss.clients.forEach(ws => {
        if (ws._agToken === device.token) ws.terminate();
    });

    log('AUTH', `Device ${device.id} (${device.label}) DELETED`);
    res.json({ ok: true, deleted: device.id });
});

// POST /admin/restart
// Exit with code 1 so launchd's KeepAlive restarts us automatically.
// Do NOT use process.exit(0) (clean exit) — launchd won't restart on that.
app.post('/admin/restart', checkAuth, (req, res) => {
    log('RESTART', 'Restart requested from mobile UI.');
    res.json({ ok: true, message: 'Restarting via launchd...' });
    // Give the response time to flush before exiting
    setTimeout(() => {
        process.exit(1); // Non-zero triggers launchd KeepAlive restart
    }, 800);
});

// POST /checkpoint
app.post('/checkpoint', checkAuth, (req, res) => {
    const cp = {
        id: 'cp_' + Date.now(),
        ts: new Date().toISOString(),
        ...req.body
    };

    STATE.checkpoints.push(cp);
    saveState();

    broadcast('checkpoint_new', cp);
    res.json({ ok: true, checkpoint: cp });
});

// --- Agent-facing routes (no auth required — localhost only, called by IDE agent) ---

// POST /agent/request-approval
// The IDE agent calls this instead of the IDE's built-in permission dialog.
// Returns immediately with { approval } — status is 'approved' if autonomous mode is on,
// otherwise 'pending' and the agent should poll /agent/approval-status/:id.
app.post('/agent/request-approval', (req, res) => {
    const { kind, details, risk, project, clientTag } = req.body;

    // Policy check
    if (kind === 'command' || !kind) {
        const cmd = details?.cmd;
        if (cmd) {
            const check = checkPolicy(cmd);
            if (!check.allowed) {
                console.warn(`[POLICY] Blocked command: "${cmd}"`);
                return res.status(403).json({ error: check.error, blocked: true });
            }
        }
    }

    const targetProject = project
        || (STATE.targetProject && (STATE.targetProject.projectName || STATE.targetProject.title))
        || 'global';

    const newApproval = {
        id: `appr_${crypto.randomBytes(4).toString('hex')}`,
        createdAt: new Date().toISOString(),
        kind: kind || 'command',
        details: details || {},
        status: 'pending',
        decidedAt: null,
        meta: { risk: risk || 'medium', clientTag: clientTag || targetProject, source: 'agent' }
    };

    // Autonomous mode — skip the queue
    if (AUTONOMOUS_MODE) {
        newApproval.status = 'approved';
        newApproval.decidedAt = new Date().toISOString();
        newApproval.autoApproved = true;
        newApproval.approvedBy = 'auto';
        STATE.approvals.push(newApproval);
        saveApprovals();
        console.log(`[AUTONOMOUS] Agent request auto-approved: ${newApproval.id} (${details?.cmd || kind})`);
        broadcast('approval_decided', { id: newApproval.id, status: 'approved', auto: true, approvedBy: 'auto' });
        return res.json({ ok: true, approval: newApproval });
    }

    STATE.approvals.push(newApproval);
    saveApprovals();

    // Surface as a message in the chat thread
    const cmdLabel = details?.cmd || kind || 'action';
    const msgText = `⚠️ Approval Required [${targetProject}]: \`${cmdLabel}\``;
    const msg = {
        id: 'msg_appr_' + newApproval.id,
        createdAt: newApproval.createdAt,
        from: 'agent', to: 'user',
        channel: 'approval',
        text: msgText,
        status: 'new',
        targetId: targetProject,
        approvalId: newApproval.id
    };
    STATE.messages.push(msg);
    if (STATE.messages.length > 200) STATE.messages.shift();
    saveState();

    console.log(`[APPROVAL] Agent requested: ${newApproval.id} kind=${kind} cmd=${details?.cmd || '-'}`);
    broadcast('approval_requested', newApproval);
    broadcast('message_new', msg);

    res.json({ ok: true, approval: newApproval });
});

// GET /agent/approval-status/:id
// The agent polls this until status changes from 'pending' to 'approved' or 'denied'.
app.get('/agent/approval-status/:id', (req, res) => {
    const approval = STATE.approvals.find(a => a.id === req.params.id);
    if (!approval) return res.status(404).json({ error: 'not_found' });
    res.json({
        ok: true,
        id: approval.id,
        status: approval.status,
        decidedAt: approval.decidedAt,
        proceed: approval.status === 'approved'
    });
});

// --- Legacy v0.2 Routes ---
app.post('/approvals/request', checkAuth, (req, res) => {
    const { kind, details, risk, clientTag } = req.body;

    // Policy Check for commands
    if (kind === 'command') {
        const cmd = details?.cmd;
        const check = checkPolicy(cmd);
        if (!check.allowed) {
            console.warn(`[POLICY] Blocked command: "${cmd}" Reason: ${check.error}`);
            return res.status(403).json({ error: check.error });
        }
    }

    const newApproval = {
        id: `appr_${crypto.randomBytes(4).toString('hex')}`,
        createdAt: new Date().toISOString(),
        kind: kind || 'unknown',
        details: details || {},
        status: 'pending',
        decidedAt: null,
        meta: {
            risk: risk || 'unknown',
            clientTag: clientTag || null
        }
    };

    // ── Autonomous Mode: skip the queue entirely ──────────────────────────────
    // When AUTONOMOUS_MODE is on the agent must never block waiting for a human
    // decision. We mark the approval immediately, log it, broadcast the decision,
    // and return 'approved' — the agent continues without any round-trip wait.
    if (AUTONOMOUS_MODE) {
        newApproval.status = 'approved';
        newApproval.decidedAt = new Date().toISOString();
        newApproval.autoApproved = true;
        newApproval.approvedBy = 'auto';
        STATE.approvals.push(newApproval);
        saveApprovals();
        console.log(`[AUTONOMOUS] Auto-approved ${newApproval.id} (${kind}: ${details?.cmd || kind})`);
        broadcast('approval_decided', { id: newApproval.id, status: 'approved', auto: true, approvedBy: 'auto' });
        return res.json({ ok: true, approval: newApproval, auto: true });
    }
    // ─────────────────────────────────────────────────────────────────────────

    STATE.approvals.push(newApproval);
    saveApprovals();

    // Inject message for the chat thread
    const targetId = req.body.project || STATE.targetProject?.id || STATE.targetProject?.title || (typeof STATE.targetProject === 'string' ? STATE.targetProject : 'global');
    const msgText = kind === 'command' ? `Approval Required: ${details?.cmd}` : `Approval Required: ${kind}`;
    
    const msg = {
        id: 'msg_appr_' + newApproval.id,
        createdAt: newApproval.createdAt,
        from: 'agent',
        to: 'user',
        channel: 'approval',
        text: msgText,
        status: 'new',
        targetId: targetId,
        approvalId: newApproval.id
    };
    STATE.messages.push(msg);
    if (STATE.messages.length > 200) STATE.messages.shift();
    saveState();

    console.log(`[REQUEST] Approval requested: ${newApproval.id} (${kind})`);
    
    // Write approval request to MemFlow outbox for reliable mobile delivery
    memflowWriteResponse(msgText, {
        id: newApproval.id,
        project: targetId,
        channel: 'approval',
        from: 'agent',
        actorId: `agent_${targetId}`
    }).catch(err => console.error('[MEMFLOW] Failed to write approval to MemFlow:', err));

    broadcast('approval_requested', newApproval);
    broadcast('message_new', msg);
    res.json({ ok: true, approval: newApproval });
});

app.get('/approvals/:id', checkAuth, (req, res) => {
    const { id } = req.params;
    const approval = STATE.approvals.find(a => a.id === id);
    if (!approval) return res.status(404).json({ error: 'not_found' });
    res.json({ ok: true, approval });
});

app.get('/approvals/stream/summary', checkAuth, (req, res) => {
    const pending = STATE.approvals.filter(a => a.status === 'pending').length;
    const approved = STATE.approvals.filter(a => a.status === 'approved').length;
    const denied = STATE.approvals.filter(a => a.status === 'denied').length;
    res.json({
        ok: true,
        ts: new Date().toISOString(),
        pending,
        approved,
        denied,
        total: STATE.approvals.length
    });
});

// --- WebSocket Handling ---
server.on('upgrade', (request, socket, head) => {
    const url = new URL(request.url, `http://${request.headers.host}`);
    const token = url.searchParams.get('token');
    const pathname = url.pathname;

    if (pathname !== '/events') {
        socket.destroy();
        return;
    }

    // Allow test token in test environment
    if (process.env.NODE_ENV === 'test' && token === 'test-token') {
        // Allow
    } else if (!token || !TOKENS.has(token)) {
        socket.write('HTTP/1.1 401 Unauthorized\r\n\r\n');
        socket.destroy();
        return;
    }

    wss.handleUpgrade(request, socket, head, (ws) => {
        ws._agToken = token; // Tag socket for device-scoped disconnect on revoke
        wss.emit('connection', ws, request);
    });
});

wss.on('connection', (ws) => {
    ws.isAlive = true;
    ws.on('pong', () => { ws.isAlive = true; });

    ws.send(JSON.stringify({ event: 'hello', payload: { ts: new Date().toISOString() } }));

    // Replay pending approvals
    const pending = STATE.approvals.filter(a => a.status === 'pending');
    for (const approval of pending) {
        ws.send(JSON.stringify({
            event: 'approval_requested',
            payload: approval,
            ts: new Date().toISOString()
        }));
    }
});

// Heartbeat Interval (30s)
const interval = setInterval(() => {
    wss.clients.forEach((ws) => {
        if (ws.isAlive === false) return ws.terminate();

        ws.isAlive = false;
        ws.ping();
    });
}, 30000);

wss.on('close', () => {
    clearInterval(interval);
});

// --- MemFlow Outbox Polling ---
// Checks MemFlow for agent responses every 5 seconds and relays them to mobile.
// This completes the round-trip: Mobile → MemFlow inbox → Agent → MemFlow outbox → Mobile
let memflowPollTimer = null;
const MEMFLOW_POLL_INTERVAL = 5000; // 5 seconds

async function pollMemflowOutbox() {
    try {
        lastPollSuccess = Date.now(); // watchdog: mark that poll is running
        // Poll globally across all projects so we catch responses from background windows
        const project = null;
        
        const responses = await memflowPollResponses(project);
        
        if (responses.length > 0) {
            log('MEMFLOW', `Found ${responses.length} agent response(s) in outbox`);
            
            const memflowIdsToMark = [];
            
            // responses from memflow are newest-first (ORDER BY created_at DESC)
            // we must reverse them to process oldest-first to maintain chronological order when pushing
            for (const resp of responses.reverse()) {
                const baseId = resp.id.replace(/^(msg_)?(mf_)?/, '').replace(/^appr_/, '');

                // Convert to ag_bridge message format and store in STATE
                const msg = {
                    id: `msg_mf_${baseId}`,
                    createdAt: resp.createdAt,
                    from: resp.from || 'agent',
                    to: 'user',
                    channel: resp.channel || 'work',
                    text: resp.text,
                    status: 'new',
                    targetId: resp.project || project,
                    source: 'memflow'
                };
                
                // Avoid duplicates by checking exact ID or variations of the same base ID
                const isDuplicate = STATE.messages.find(m => {
                    if (m.id === msg.id) return true;
                    if (m.id === `msg_${baseId}`) return true;
                    if (m.id === `msg_appr_${baseId}`) return true;
                    if (m.id === `msg_mf_${baseId}`) return true;
                    if (m.approvalId === `appr_${baseId}`) return true;
                    return false;
                });

                if (!isDuplicate) {
                    STATE.messages.push(msg);
                    broadcast('message_new', msg);
                    log('MEMFLOW', `Relayed agent response to mobile: ${msg.id}`);

                    // If this is an approval request, auto-create an approval entry
                    if (resp.channel === 'approval' || (resp.text && resp.text.startsWith('Approval Required'))) {
                        const approvalId = resp.approvalId || `appr_mf_${baseId}`;
                        const existingApproval = STATE.approvals.find(a => a.id === approvalId);
                        if (!existingApproval) {
                            const newApproval = {
                                id: approvalId,
                                createdAt: resp.createdAt || new Date().toISOString(),
                                kind: resp.kind || 'command',
                                details: resp.details || { cmd: resp.text, risk: resp.risk || 'medium' },
                                status: 'pending',
                                decidedAt: null,
                                meta: { clientTag: resp.project || 'global', source: 'memflow' }
                            };
                            STATE.approvals.push(newApproval);
                            msg.approvalId = approvalId;
                            saveApprovals();
                            broadcast('approval_requested', newApproval);
                            log('MEMFLOW', `Created approval from MemFlow: ${approvalId}`);
                        }
                    }
                }
                
                if (resp.memflowId) {
                    memflowIdsToMark.push(resp.memflowId);
                }
            }
            
            // Keep array size manageable
            if (STATE.messages.length > 200) {
                STATE.messages = STATE.messages.slice(-200);
            }
            
            // Mark as read in MemFlow so they don't get polled again
            if (memflowIdsToMark.length > 0) {
                await memflowMarkAsRead(memflowIdsToMark);
            }
            
            saveState();
        }
    } catch (err) {
        // Silent fail — polling errors should not crash the server
        if (err.message && !err.message.includes('no such table')) {
        log('MEMFLOW', `Poll error: ${err.message}`);
        }
    }
}

// Start polling when server starts
memflowPollTimer = setInterval(pollMemflowOutbox, MEMFLOW_POLL_INTERVAL);

// ── Delivery Reconciliation Sweeper ──────────────────────────────────────────
const RECONCILE_INTERVAL = 10_000;
const STALE_NEW_THRESHOLD  = 15_000;    // 15s: message stuck in 'new'
const STALE_SENT_THRESHOLD = 30_000;    // 30s: agent hasn't picked up from MongoDB
const MAX_RESCUE_AGE       = 300_000;   // 5 min: stop trying to rescue very old messages
const MAX_WAKE_AGE         = 600_000;   // 10 min: stop CDP wake attempts

async function reconcileDelivery() {
    const now = Date.now();
    let dirty = false;

    try {
        // ── Phase 1: Rescue stuck 'new' messages (never made it to MongoDB) ──
        const stuckNew = STATE.messages.filter(m =>
            m.from === 'user' && m.status === 'new' &&
            (now - new Date(m.createdAt).getTime()) > STALE_NEW_THRESHOLD
        );
        if (stuckNew.length > 0) {
            // Expire messages older than MAX_RESCUE_AGE — they'll never deliver
            const rescuable = [];
            for (const m of stuckNew) {
                const age = now - new Date(m.createdAt).getTime();
                if (age > MAX_RESCUE_AGE) {
                    m.status = 'expired';
                    broadcast('message_ack', { id: m.id, status: 'expired', receipt: '✗ Expired' });
                    dirty = true;
                } else {
                    rescuable.push(m);
                }
            }
            if (rescuable.length > 0) {
                log('RECONCILE', `${rescuable.length} message(s) stuck in 'new' — forcing poke...`);
                await tryPoke(true);
                dirty = true;
            }
        }

        // ── Phase 2: Re-scan CDP for 'sent' messages the agent hasn't picked up ──
        const staleSent = STATE.messages.filter(m =>
            m.from === 'user' && m.status === 'sent' && m.memflowInboxId &&
            (now - new Date(m.createdAt).getTime()) > STALE_SENT_THRESHOLD &&
            (now - new Date(m.createdAt).getTime()) < MAX_WAKE_AGE
        );
        if (staleSent.length > 0) {
            // Agent hasn't read from MongoDB yet — try waking it via CDP
            const targets = await getTargetsForProductMode();
            const projectsToWake = [...new Set(staleSent.map(m => m.targetId).filter(Boolean))];
            const norm = (s) => (s || '').toLowerCase().replace(/[-_]/g, '-');

            for (const projName of projectsToWake) {
                const target = targets.find(t =>
                    t.port && (norm(t.projectName) === norm(projName) || (t.title && norm(t.title).includes(norm(projName))))
                );
                if (target) {
                    log('RECONCILE', `Waking agent for '${projName}' via CDP port ${target.port}...`);
                    const wakeMsg = `[System] You have unread mobile messages. Call mobile_read_inbox to process them.`;
                    const result = await pokeTarget(target, wakeMsg, { project: projName, from: 'system', channel: 'work' });
                    if (result.ok) {
                        log('RECONCILE', `CDP wake SUCCESS for '${projName}' — agent should now read inbox`);
                    } else {
                        log('RECONCILE', `CDP wake failed for '${projName}': ${result.error || result.reason || 'unknown'}`);
                    }
                } else {
                    // ── Cross-window broadcast wake (Approach A) ──
                    // No window matches this exact project, but another IDE window
                    // might be open. Send the wake there — MemFlow inbox is global,
                    // so any agent can call mobile_read_inbox and process the message.
                    const anyTarget = targets.find(t => t.port);
                    if (anyTarget) {
                        log('RECONCILE', `No window for '${projName}' — broadcasting wake to '${anyTarget.projectName || anyTarget.title}' (cross-window fallback)`);
                        const wakeMsg = `[System] You have unread mobile messages for project "${projName}". Call mobile_read_inbox to process them.`;
                        const result = await pokeTarget(anyTarget, wakeMsg, { project: projName, from: 'system', channel: 'work' });
                        if (result.ok) {
                            log('RECONCILE', `Cross-window wake SUCCESS via '${anyTarget.projectName || anyTarget.title}' for '${projName}'`);
                        } else {
                            log('RECONCILE', `Cross-window wake failed: ${result.error || result.reason || 'unknown'}`);
                        }
                    } else {
                        log('RECONCILE', `No CDP port found for '${projName}' and no other IDE windows available — agent must be prompted manually`);
                    }
                }
            }
        }

        // ── Phase 3: Check MongoDB for delivery receipts ──
        const sentWithId = STATE.messages.filter(m =>
            m.from === 'user' && m.status === 'sent' && m.memflowInboxId
        );
        if (sentWithId.length > 0) {
            const inboxIds = [...new Set(sentWithId.map(m => m.memflowInboxId))];
            const { read } = await memflowCheckReceipts(inboxIds);

            if (read.length > 0) {
                const readSet = new Set(read);
                let updated = 0;
                for (const m of sentWithId) {
                    if (readSet.has(m.memflowInboxId)) {
                        m.status = 'delivered';
                        broadcast('message_ack', { id: m.id, status: 'delivered', receipt: '✓✓ Delivered' });
                        updated++;
                    }
                }
                if (updated > 0) {
                    dirty = true;
                    log('RECEIPT', `${updated} message(s) confirmed delivered to agent`);
                }
            }
        }

        if (dirty) saveState();
    } catch (e) {
        // Silent — reconciliation should never crash the server
    }
}

setInterval(reconcileDelivery, RECONCILE_INTERVAL);


// ── SMS / iMessage Alert Fallback ────────────────────────────────────────────
// Uses osascript (macOS) to send iMessage when bridge delivery is broken.
// Set ALERT_PHONE env var or add "alertPhone": "+15551234567" to data/config.json

let ALERT_PHONE = process.env.ALERT_PHONE || null;

// Try to load phone from config.json on startup
(async () => {
    try {
        const cfgPath = join(DATA_DIR, 'config.json');
        if (existsSync(cfgPath)) {
            const cfg = JSON.parse(await readFile(cfgPath, 'utf8'));
            if (cfg.alertPhone) ALERT_PHONE = cfg.alertPhone;
            if (cfg.autonomousMode !== undefined) AUTONOMOUS_MODE = cfg.autonomousMode;
        }
    } catch (_) {}
})();

/**
 * Send an iMessage/SMS via osascript.
 * Works on macOS when Messages.app is configured with the target number.
 */
async function sendSMS(phone, message) {
    if (!phone) {
        log('SMS', 'No alert phone configured. Set ALERT_PHONE env or data/config.json alertPhone.');
        return { ok: false, reason: 'no_phone' };
    }
    const safeMsg = message.replace(/"/g, '\\"').replace(/\n/g, ' ');
    const script = `tell application "Messages"
    set targetService to 1st account whose service type = iMessage
    set targetBuddy to participant "${phone}" of targetService
    send "${safeMsg}" to targetBuddy
end tell`;
    return new Promise((resolve) => {
        const proc = spawn('osascript', ['-e', script]);
        let stderr = '';
        proc.stderr.on('data', d => { stderr += d.toString(); });
        proc.on('close', (code) => {
            if (code === 0) {
                log('SMS', `✅ iMessage sent to ${phone}`);
                resolve({ ok: true });
            } else {
                log('SMS', `❌ iMessage failed (${code}): ${stderr.trim()}`);
                resolve({ ok: false, reason: stderr.trim() });
            }
        });
    });
}

// POST /notify/sms — manually or agent-triggered SMS alert
app.post('/notify/sms', checkAuth, async (req, res) => {
    const { message, phone } = req.body;
    const target = phone || ALERT_PHONE;
    if (!target) return res.status(400).json({ error: 'no_phone_configured', hint: 'Set alertPhone in data/config.json or pass phone in body' });
    const result = await sendSMS(target, message || 'ag_bridge alert: bridge may be down');
    res.json(result);
});

// POST /notify/sms/test — quick test without auth, localhost-only
app.post('/notify/sms/test', (req, res) => {
    const ip = req.ip || '';
    const isLocal = ip === '::1' || ip === '127.0.0.1' || ip.startsWith('::ffff:127.');
    if (!isLocal) return res.status(403).json({ error: 'localhost_only' });
    const { phone, message } = req.body;
    const target = phone || ALERT_PHONE;
    if (!target) return res.status(400).json({ error: 'no_phone_configured' });
    sendSMS(target, message || '🟢 ag_bridge SMS test — if you got this, alerts work!').then(r => res.json(r));
});

// ── Delivery Watchdog ─────────────────────────────────────────────────────────
// Tracks consecutive memflow poll failures. After 3 failures, sends SMS alert.
let pollFailCount = 0;
const POLL_FAIL_THRESHOLD = 3;
const POLL_WATCHDOG_INTERVAL = 30_000; // check every 30s for persistent failure

let lastPollSuccess = Date.now();
let smsSentAt = 0; // throttle — don't send more than 1 SMS per 10 minutes

setInterval(async () => {
    const msSinceLastPoll = Date.now() - lastPollSuccess;
    const minutesSince = Math.floor(msSinceLastPoll / 60_000);

    if (msSinceLastPoll > POLL_FAIL_THRESHOLD * MEMFLOW_POLL_INTERVAL * 2) {
        // Poll has been silent for > 3 poll cycles
        const now = Date.now();
        if (ALERT_PHONE && now - smsSentAt > 10 * 60_000) {
            smsSentAt = now;
            log('WATCHDOG', `Poll silent for ${minutesSince}min — sending SMS alert`);
            await sendSMS(ALERT_PHONE,
                `⚠️ ag_bridge: Bridge delivery may be down. MongoDB poll silent for ${minutesSince} min. Check your Mac.`
            );
        } else if (!ALERT_PHONE) {
            log('WATCHDOG', `Poll silent for ${minutesSince}min. Set ALERT_PHONE to enable SMS alerts.`);
        }
    }
}, POLL_WATCHDOG_INTERVAL);

// Patch pollMemflowOutbox to update lastPollSuccess on success
const _origPollMemflowOutbox = pollMemflowOutbox;
// (lastPollSuccess is updated inline in the poll's try block — see below)

// --- Graceful Shutdown ---
function gracefulShutdown(signal) {
    console.log(`\n[SHUTDOWN] Received ${signal}. Closing server...`);
    if (memflowPollTimer) clearInterval(memflowPollTimer);
    server.close(() => {
        console.log('[SHUTDOWN] HTTP server closed.');
        process.exit(0);
    });
    // Force exit after 5s if graceful close hangs
    setTimeout(() => { process.exit(1); }, 5000);
}
process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));
process.on('SIGINT', () => gracefulShutdown('SIGINT'));
process.on('uncaughtException', (err) => {
    console.error(`[FATAL] Uncaught exception: ${err.message}`);
    console.error(err.stack);
    // Don't exit on non-fatal errors — let launchd restart if truly fatal
    if (err.code === 'EADDRINUSE') process.exit(1);
});

// --- Start ---
// Load state then start
if (process.argv[1] === fileURLToPath(import.meta.url)) {
    Promise.all([loadState(), loadPolicy()]).then(async () => {
        // Dynamic import for ESM compatibility if needed, or standard import
        let qrcode = null;
        try { qrcode = await import('qrcode-terminal'); } catch (e) { console.log('[WARN] qrcode-terminal not found'); }

        function startListening(retryCount = 0) {
            server.on('error', (err) => {
                if (err.code === 'EADDRINUSE' && retryCount < 3) {
                    console.log(`[STARTUP] Port ${PORT} in use (attempt ${retryCount + 1}/3). Killing stale process...`);
                    try {
                        const lsofOut = execSync(`/usr/sbin/lsof -ti :${PORT}`, { encoding: 'utf-8' }).trim();
                        if (lsofOut) {
                            const pids = lsofOut.split('\n').filter(Boolean);
                            const myPid = process.pid.toString();
                            for (const pid of pids) {
                                if (pid !== myPid) {
                                    console.log(`[STARTUP] Killing PID ${pid}`);
                                    execSync(`kill -9 ${pid}`, { stdio: 'ignore' });
                                }
                            }
                        }
                    } catch (killErr) {
                        console.log('[STARTUP] Could not kill stale process:', killErr.message);
                    }
                    // Wait for port to free, then retry bind (no exit/restart needed)
                    console.log(`[STARTUP] Waiting 2s for port to free, then retrying...`);
                    setTimeout(() => {
                        // Create a fresh server to retry
                        server.removeAllListeners('error');
                        startListening(retryCount + 1);
                    }, 2000);
                } else {
                    console.error(`[STARTUP] Fatal server error: ${err.message}`);
                    process.exit(1);
                }
            });

            server.listen(PORT, HOST, () => {
                const ips = getLocalIPs();
                const ts = getTailscaleInfo();

                console.log('='.repeat(50));
                console.log(` AG Bridge v${APP_VERSION} running on port ${PORT}`);
                console.log('='.repeat(50));
                console.log(` PAIRING CODE: [ ${PAIRING_CODE} ]`);
                console.log('-'.repeat(50));

                let qrUrl = null;

                console.log(' Local (same Wi-Fi):');
                if (ips.length > 0) {
                    ips.forEach(ip => {
                        const url = `http://${ip}:${PORT}`;
                        console.log(` ${url}`);
                        if (!qrUrl) qrUrl = url; // Fallback
                    });
                } else {
                    console.log(' (No local LAN IP found)');
                }

                if (ts) {
                    console.log('\n Remote (Tailscale Active):');
                    if (ts.name) {
                        const url = `http://${ts.name}:${PORT}`;
                        console.log(` ${url}`);
                        qrUrl = url; // Priority 1: Tailscale DNS

                        // Auto-provision HTTPS via Tailscale
                        const tsPaths = [
                            '/Applications/Tailscale.app/Contents/MacOS/Tailscale',
                            '/usr/local/bin/tailscale'
                        ];
                        let tsCmd = 'tailscale';
                        try {
                            for (const p of tsPaths) {
                                if (existsSync(p)) { tsCmd = p; break; }
                            }
                        } catch (e) { /* use default */ }
                        try {
                            // Check if serve is already configured before running
                            const serveStatus = execSync(`${tsCmd} serve status 2>&1`, { encoding: 'utf-8' }).trim();
                            const alreadyServing = serveStatus.includes(`:${PORT}`) || serveStatus.includes('proxy');
                            if (!alreadyServing) {
                                execSync(`${tsCmd} serve --bg ${PORT}`, { stdio: 'ignore' });
                                console.log(` https://${ts.name}  (Secure HTTPS — newly provisioned)`);
                            } else {
                                console.log(` https://${ts.name}  (Secure HTTPS Active)`);
                            }
                            qrUrl = `https://${ts.name}`; // Prefer secure for QR
                        } catch (e) {
                            console.log(` [WARN] Tailscale HTTPS not provisioned: ${e.message || 'not enabled on account'}`);
                        }
                    }
                    ts.ips.forEach(ip => {
                        const url = `http://${ip}:${PORT}`;
                        console.log(` ${url}`);
                        if (!qrUrl) qrUrl = url; // Priority 2: Tailscale IP
                    });
                } else {
                    console.log('\n Remote (Tailscale Inactive):');
                    console.log(' Install Tailscale for access anywhere: https://tailscale.com');
                }

                console.log('='.repeat(50));

                // Generate QR Code
                if (qrUrl && qrcode) {
                    const fullUrl = `${qrUrl}?code=${PAIRING_CODE}`;
                    console.log('\nScan to Connect:');
                    qrcode.default.generate(fullUrl, { small: true });
                    console.log(`(Encoded: ${fullUrl})`);
                    console.log('='.repeat(50));
                }
            });
        }

        startListening(0);
    });
}
