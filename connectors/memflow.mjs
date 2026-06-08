/**
 * connectors/memflow.mjs — MongoDB-backed MemFlow connector for ag_bridge
 *
 * Replaces the old SQLite-based implementation. The running memflow MCP server
 * (`memflow-cpu`) uses MongoDB (connector: "mongodb" in ~/.memflow/config.json),
 * so this connector must read and write to the same MongoDB database.
 *
 * Collection: "memories"  (memflow standard)
 * Database:   from config.mongo.database  (memflow_default)
 *
 * Documents follow the MemoryEntry schema used by MongoDBConnector in memflow-cpu:
 *   { id, key, content, coordinates: { namespace, project, scope }, tags, metadata, ... }
 *
 * Namespaces used by ag_bridge:
 *   ag_bridge/inbox   — messages FROM mobile TO agent  (we write, agent reads)
 *   ag_bridge/outbox  — messages FROM agent TO mobile  (agent writes, we read)
 */

import { readFile } from 'fs/promises';
import crypto from 'crypto';
import path from 'path';
import os from 'os';

// ── Config ────────────────────────────────────────────────────────────────────

const INBOX_NAMESPACE  = 'ag_bridge/inbox';
const OUTBOX_NAMESPACE = 'ag_bridge/outbox';
const COLLECTION = 'memories';

let _client = null;
let _db     = null;
let _col    = null;

async function loadConfig() {
    const cfgPath = path.join(os.homedir(), '.memflow', 'config.json');
    try {
        const raw = await readFile(cfgPath, 'utf8');
        return JSON.parse(raw);
    } catch (e) {
        console.warn('[MEMFLOW] Could not read ~/.memflow/config.json:', e.message);
        return null;
    }
}

async function getCollection() {
    if (_col) return _col;

    const cfg = await loadConfig();
    if (!cfg || !cfg.mongo?.uri) {
        throw new Error('[MEMFLOW] No MongoDB URI in ~/.memflow/config.json (cfg.mongo.uri)');
    }

    const { MongoClient } = await import('mongodb');
    _client = new MongoClient(cfg.mongo.uri, {
        maxPoolSize: 5,
        serverSelectionTimeoutMS: 10_000,
        socketTimeoutMS: 45_000,
    });
    await _client.connect();
    _db  = _client.db(cfg.mongo.database || 'memflow_default');
    _col = _db.collection(COLLECTION);
    console.log('[MEMFLOW] Connected to MongoDB');
    return _col;
}

// ── Write (inbox — mobile → agent) ───────────────────────────────────────────

/**
 * Write a message to the MemFlow inbox so the agent picks it up via
 * mobile_read_inbox MCP tool. Returns { ok, id }.
 */
export async function writeInboxMessage(text, metadata = {}) {
    const col = await getCollection();
    const msgId   = metadata.id || `inbox_${crypto.randomBytes(6).toString('hex')}`;
    const project = metadata.project || 'global';
    const now     = new Date().toISOString();

    const doc = {
        id:      msgId,
        key:     `ag_bridge/inbox/${msgId}`,
        content: text,
        coordinates: {
            namespace: INBOX_NAMESPACE,
            project,
            scope: 'workspace',
        },
        kind: 'knowledge',
        tags: ['ag_bridge', 'inbox', 'pending', project],
        metadata: {
            msgId,
            from:       metadata.from || 'user',
            to:         'agent',
            channel:    metadata.channel || 'work',
            status:     'pending',
            project,
            timestamp:  now,
        },
        provenance: { source: 'ag_bridge_mobile', actorId: 'mobile_user' },
        createdAt:  now,
        updatedAt:  now,
        version:    1,
    };

    await col.replaceOne(
        { 'coordinates.namespace': INBOX_NAMESPACE, key: doc.key },
        doc,
        { upsert: true }
    );

    console.log(`[MEMFLOW] Wrote inbox message: ${msgId} (project: ${project})`);
    return { ok: true, id: msgId };
}

// ── Read (outbox — agent → mobile) ────────────────────────────────────────────

/**
 * Poll for agent responses the agent wrote via mobile_respond MCP tool.
 * Returns array of message objects, newest-first, limited to 20.
 */
export async function pollResponses(project) {
    try {
        const col = await getCollection();

        const filter = {
            'coordinates.namespace': OUTBOX_NAMESPACE,
            tags: 'unread',
        };
        if (project) {
            filter.$or = [
                { 'coordinates.project': project },
                { tags: project },
            ];
        }

        const docs = await col
            .find(filter)
            .sort({ createdAt: -1 })
            .limit(20)
            .toArray();

        return docs.map(doc => {
            const meta = doc.metadata || {};
            return {
                id:         doc.id || doc._id?.toString(),
                memflowId:  doc.id || doc._id?.toString(),
                key:        doc.key,
                text:       doc.content,
                from:       meta.from    || 'agent',
                to:         meta.to      || 'user',
                channel:    meta.channel || 'work',
                project:    meta.project || doc.coordinates?.project || project,
                createdAt:  doc.createdAt,
                status:     meta.status  || 'unread',
                approvalId: meta.approvalId || null,
                kind:       meta.kind       || null,
                details:    meta.details    || null,
                risk:       meta.risk       || null,
            };
        });
    } catch (e) {
        console.error('[MEMFLOW] pollResponses error:', e.message);
        return [];
    }
}

/**
 * Mark outbox messages as read so they don't re-appear on next poll.
 */
export async function markMessagesRead(ids) {
    if (!ids || ids.length === 0) return;
    try {
        const col = await getCollection();
        await col.updateMany(
            { id: { $in: ids } },
            {
                $set:  { 'metadata.status': 'read', updatedAt: new Date().toISOString() },
                $pull: { tags: 'unread' },
            }
        );
    } catch (e) {
        console.warn('[MEMFLOW] markMessagesRead error:', e.message);
    }
}

// ── Write (outbox — for writeResponse used by approval surfacing) ─────────────

/**
 * Write an agent response/approval request to the MemFlow outbox.
 * The mobile_read_inbox-based polling flow picks these up.
 */
export async function writeResponse(text, metadata = {}) {
    const col = await getCollection();
    const msgId   = metadata.id || `resp_${crypto.randomBytes(6).toString('hex')}`;
    const project = metadata.project || 'global';
    const now     = new Date().toISOString();

    const doc = {
        id:      msgId,
        key:     `ag_bridge/outbox/${msgId}`,
        content: text,
        coordinates: {
            namespace: OUTBOX_NAMESPACE,
            project,
            scope: 'workspace',
        },
        kind: 'knowledge',
        tags: ['ag_bridge', 'outbox', 'unread', project, ...(metadata.channel === 'approval' ? ['approval'] : [])],
        metadata: {
            msgId,
            from:       metadata.from    || 'agent',
            to:         'user',
            channel:    metadata.channel || 'work',
            status:     'unread',
            project,
            timestamp:  now,
            inReplyTo:  metadata.inReplyTo || null,
            approvalId: metadata.approvalId || null,
            kind:       metadata.kind || null,
            details:    metadata.details || null,
        },
        provenance: { source: 'agent', actorId: metadata.actorId || 'ag_bridge_agent' },
        createdAt:  now,
        updatedAt:  now,
        version:    1,
    };

    await col.replaceOne(
        { 'coordinates.namespace': OUTBOX_NAMESPACE, key: doc.key },
        doc,
        { upsert: true }
    );

    return { ok: true, method: 'memflow_mongodb', id: msgId };
}

// ── Delivery Receipts (sweeper — confirm agent picked up inbox messages) ──────

/**
 * Check which inbox messages the agent has picked up (tag swapped from "pending" → "read").
 * Pass an array of msgIds we wrote to the inbox. Returns which ones the agent has read.
 */
export async function checkInboxReceipts(msgIds) {
    if (!msgIds || msgIds.length === 0) return { read: [], pending: [] };
    try {
        const col = await getCollection();
        // Find docs that WERE our inbox messages but no longer have "pending" tag
        // (the MCP mobile_read_inbox tool swaps "pending" → "read")
        const readDocs = await col.find({
            'coordinates.namespace': INBOX_NAMESPACE,
            id: { $in: msgIds },
            tags: { $nin: ['pending'] },  // agent removed the "pending" tag
        }).project({ id: 1 }).toArray();

        const readIds = new Set(readDocs.map(d => d.id));
        return {
            read: msgIds.filter(id => readIds.has(id)),
            pending: msgIds.filter(id => !readIds.has(id)),
        };
    } catch (e) {
        console.error('[MEMFLOW] checkInboxReceipts error:', e.message);
        return { read: [], pending: msgIds };
    }
}

// ── Graceful shutdown ─────────────────────────────────────────────────────────

export async function close() {
    if (_client) {
        await _client.close();
        _client = null;
        _db     = null;
        _col    = null;
    }
}

// ── Agent Registry (Strataflow / Phase 1) ────────────────────────────────────

/**
 * Retrieve agents that have sent a heartbeat in the last 5 minutes.
 * @param {string} [project] Optional project filter
 */
export async function getActiveAgents(project) {
    try {
        if (!_db) await getCollection(); // ensure connected
        const col = _db.collection('active_agents');
        
        const cutoff = new Date(Date.now() - 5 * 60 * 1000).toISOString();
        const query = { lastSeenAt: { $gte: cutoff } };
        if (project) {
            query.project = project;
        }

        const agents = await col.find(query).toArray();
        // Return full documents including conversationId and workspaceId if present
        return agents.map(a => ({
            agentId:        a.agentId,
            project:        a.project,
            state:          a.state,
            task:           a.task || '',
            lastSeenAt:     a.lastSeenAt,
            capabilities:   a.capabilities || [],
            conversationId: a.conversationId || null,   // IDE conversation UUID for precise CDP routing
            workspaceId:    a.workspaceId    || null,   // IDE workspace ID
        }));
    } catch (e) {
        console.error('[MEMFLOW] getActiveAgents error:', e.message);
        return [];
    }
}

// ── Universal Inbox Scanner (cross-project watchdog) ─────────────────────────

/**
 * Scan ALL projects' inboxes for messages stuck in 'pending' state.
 * Unlike checkInboxReceipts (which only checks specific known IDs), this
 * queries MongoDB directly so it catches messages written by mobile apps,
 * other agents, or any source that bypasses ag-bridge's STATE.messages.
 *
 * @param {number} olderThanMs — only return messages older than this (ms). Default 5 min.
 * @returns {Promise<Array<{id, project, content, createdAt, channel, metadata}>>}
 */
export async function pollAllPendingInbox(olderThanMs = 5 * 60 * 1000) {
    try {
        const col = await getCollection();
        const cutoff = new Date(Date.now() - olderThanMs).toISOString();
        const docs = await col.find({
            'coordinates.namespace': INBOX_NAMESPACE,
            tags: 'pending',
            createdAt: { $lt: cutoff },
        }).sort({ createdAt: 1 }).limit(50).toArray();

        return docs.map(doc => ({
            id:        doc.id,
            project:   doc.coordinates?.project || doc.metadata?.project || 'global',
            content:   doc.content,
            createdAt: doc.createdAt,
            channel:   doc.metadata?.channel || 'work',
            metadata:  doc.metadata || {},
        }));
    } catch (e) {
        console.error('[MEMFLOW] pollAllPendingInbox error:', e.message);
        return [];
    }
}

