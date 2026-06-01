import http from 'http';

export const CONNECTOR_ID = 'antigravity';
export const CONNECTOR_NAME = 'Antigravity IDE';
const STATIC_PORTS = [9000, 9001, 9002, 9003];

// Dynamically detect product type by scanning running processes (no CDP required)
async function getProductTypeForPid(pid) {
    if (!pid) return null;
    try {
        const { execSync } = await import('child_process');
        const cmd = execSync(`ps -p ${pid} -o args=`, { encoding: 'utf8', timeout: 1000 }).trim();
        if (cmd.includes('Antigravity IDE.app') || cmd.includes('antigravity-ide')) {
            return 'ide';
        } else if (cmd.includes('Antigravity.app') || cmd.includes('VibeCraft.app') || cmd.toLowerCase().includes('vibe')) {
            return 'vibe';
        }
    } catch (e) {}
    return null;
}

async function getPortOwner(port) {
    try {
        const { execSync } = await import('child_process');
        const out = execSync(`/usr/sbin/lsof -nP -iTCP:${port} -sTCP:LISTEN -Fp 2>/dev/null`, { encoding: 'utf8', timeout: 1000 });
        const pidLine = out.split('\n').find(line => line.startsWith('p'));
        const pid = pidLine ? parseInt(pidLine.slice(1), 10) : null;
        if (!pid) return { pid: null, productType: null };
        return { pid, productType: await getProductTypeForPid(pid) };
    } catch (e) {
        return { pid: null, productType: null };
    }
}

/**
 * Detect which Antigravity product is currently running by scanning process list.
 * Returns 'ide' | 'vibe' | null (null = nothing running).
 */
export async function getRunningProductType() {
    try {
        const { execSync } = await import('child_process');
        const out = execSync('ps -ax -o args= 2>/dev/null', { encoding: 'utf8', timeout: 2000 });
        if (out.includes('Antigravity IDE.app') || out.includes('antigravity-ide')) return 'ide';
        if (out.includes('VibeCraft.app') || out.includes('Antigravity.app/Contents')) return 'vibe';
    } catch (e) {}
    return null;
}

/**
 * Read open workspace folder names from the Antigravity IDE language server
 * processes. Each workspace gets a --workspace_id argument whose value is
 * either a hex hash (for cloud workspaces) or a path slug like
 *   file_Users_sean_Documents_projects_my_project
 * We decode path slugs back to the last folder segment (the project name).
 */

/** 
 * For cloud/remote workspaces (hex-hash workspace IDs), resolve the project
 * folder name by checking which /projects/* directory the PID has open via lsof.
 */
async function resolveProjectNameForPid(pid) {
    try {
        const { execSync } = await import('child_process');
        const out = execSync(
            `/usr/sbin/lsof -p ${pid} -Fn 2>/dev/null | /usr/bin/grep '^n/Users' | /usr/bin/grep '/projects/' | /usr/bin/head -5`,
            { encoding: 'utf8', timeout: 2000 }
        );
        for (const line of out.trim().split('\n').filter(Boolean)) {
            const path = line.replace(/^n/, '');
            const m = path.match(/\/projects\/([^/.][^/]*)/);
            if (m && m[1] && !m[1].startsWith('.')) return m[1];
        }
    } catch (_) {}
    return null;
}

async function getWorkspacesFromProcesses() {
    const workspaces = [];
    try {
        const { execSync } = await import('child_process');
        const out = execSync('ps ax -o pid=,args= 2>/dev/null', { encoding: 'utf8', timeout: 3000 });
        const lines = out.split('\n');
        const seen = new Set();
        const hexResolvePromises = [];

        for (const line of lines) {
            // Only look at language server processes that belong to Antigravity IDE
            if (!line.includes('language_server_macos') && !line.includes('language_server_linux') && !line.includes('language_server_win')) continue;
            if (!line.includes('Antigravity IDE')) continue;

            const workspaceMatch = line.match(/--workspace_id\s+(\S+)/);
            const portMatch = line.match(/--extension_server_port\s+(\d+)/);
            const pidMatch = line.match(/^\s*(\d+)/);
            const pid = pidMatch ? pidMatch[1] : null;

            // No workspace_id = global conversation window (no project context) — skip
            if (!workspaceMatch) continue;

            const workspaceId = workspaceMatch[1];
            if (seen.has(workspaceId)) continue;
            seen.add(workspaceId);

            // Decode path slugs (file_Users_..._Documents_projects_my_project -> my_project)
            // The slug encodes the full path with _ separators. We strip up to and including
            // 'projects_' (or the deepest 'Documents_' prefix) to get the real folder name.
            let projectName = null;
            if (workspaceId.startsWith('file_')) {
                const slug = workspaceId.replace(/^file_/, '');
                // Try to find '_projects_' prefix and take everything after it
                const projectsIdx = slug.lastIndexOf('_projects_');
                if (projectsIdx !== -1) {
                    projectName = slug.slice(projectsIdx + '_projects_'.length);
                } else {
                    // Fallback: strip common path prefixes then take what remains
                    const cleaned = slug
                        .replace(/^Users_[^_]+_/, '')          // strip Users/<username>/
                        .replace(/^(Documents|Desktop|Developer)_/, ''); // strip common dirs
                    projectName = cleaned || null;
                }
                // Replace any remaining leading/trailing underscores
                if (projectName) projectName = projectName.replace(/^_+|_+$/g, '') || null;
            }

            // Hex hashes = cloud/remote workspaces — resolve via lsof
            const isHexHash = !projectName && /^[0-9a-f]{32,}$/i.test(workspaceId);
            if (isHexHash && pid) {
                const extensionPort = portMatch ? parseInt(portMatch[1], 10) : null;
                hexResolvePromises.push(
                    resolveProjectNameForPid(pid).then(resolved => {
                        if (resolved) {
                            workspaces.push({
                                workspaceId,
                                projectName: resolved,
                                extensionPort,
                                productType: 'ide',
                                source: 'lsof_resolve',
                            });
                        }
                        // If lsof resolve fails, silently skip — don't add unnamed entries
                    })
                );
                continue; // Will be added asynchronously above
            }

            if (!projectName) continue; // Skip anything we can't name

            workspaces.push({
                workspaceId,
                projectName,
                extensionPort: portMatch ? parseInt(portMatch[1], 10) : null,
                productType: 'ide',
                source: 'process_scan',
            });
        }

        // Wait for all lsof resolutions
        if (hexResolvePromises.length > 0) {
            await Promise.allSettled(hexResolvePromises);
        }
    } catch (e) { /* process scan unavailable */ }
    return workspaces;
}


async function discoverPorts() {
    const results = [];
    // Always include static ports as default fallbacks. Resolve the owning PID
    // when possible because the same debug ports are valid for both products.
    for (const p of STATIC_PORTS) {
        const owner = await getPortOwner(p);
        results.push({ port: p, pid: owner.pid, productType: owner.productType });
    }

    try {
        const { execSync } = await import('child_process');
        const out = execSync("lsof -i -nP 2>/dev/null | grep -i 'Antigravi' | grep 'LISTEN'", { encoding: 'utf8', timeout: 3000 });
        const lines = out.split('\n');
        for (const line of lines) {
            const parts = line.trim().split(/\s+/);
            if (parts.length >= 9) {
                const pid = parseInt(parts[1], 10);
                const name = parts[8];
                const portMatch = name.match(/:(\d+)$/);
                if (portMatch) {
                    const port = parseInt(portMatch[1], 10);
                    // Avoid duplicating static ports
                    if (!STATIC_PORTS.includes(port)) {
                        const productType = await getProductTypeForPid(pid);
                        results.push({ port, pid, productType });
                    }
                }
            }
        }
    } catch (e) { /* lsof not available or no results */ }
    return results;
}

function getJson(url) {
    return new Promise((resolve, reject) => {
        const req = http.get(url, { timeout: 1500 }, (res) => {
            let data = '';
            res.on('data', chunk => data += chunk);
            res.on('end', () => {
                try { resolve(JSON.parse(data)); } catch (e) { reject(e); }
            });
        });
        req.on('timeout', () => {
            req.destroy();
            reject(new Error("Request timeout"));
        });
        req.on('error', reject);
    });
}

export function extractProjectName(title) {
    if (!title) return '';
    let p = title;
    if (p.includes(' — ')) {
        p = p.split(' — ')[0].trim();
    } else if (p.includes(' - ')) {
        const parts = p.split(' - ');
        p = parts.length > 1 ? parts[parts.length - 2].trim() : parts[0].trim();
    }
    return p.replace(' (Workspace)', '').trim();
}

export function extractConversationId(url) {
    if (!url) return null;
    const match = url.match(/\/c\/([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})/i);
    return match ? match[1] : null;
}

export async function getTargets() {
    const targets = [];
    const seenProjects = new Set();

    // ─── Layer 1: Language-server process scan (primary for Antigravity IDE) ───
    // Antigravity IDE no longer exposes --remote-debugging-port, but each open
    // workspace spawns a language_server process with --workspace_id that encodes
    // the project folder path. We use this as the authoritative project list.
    const workspaces = await getWorkspacesFromProcesses();
    for (const ws of workspaces) {
        if (seenProjects.has(ws.projectName)) continue;
        seenProjects.add(ws.projectName);
        targets.push({
            connectorId: CONNECTOR_ID,
            id: ws.workspaceId,
            title: ws.projectName,
            projectName: ws.projectName,
            port: null,                    // No CDP port — delivery via MemFlow only
            url: null,
            webSocketDebuggerUrl: null,
            conversationId: null,
            isConversation: false,
            pid: null,
            productType: ws.productType,
            source: 'process_scan',
        });
    }

    // ─── Layer 2: CDP / static port scan (legacy Antigravity / VibeCraft) ───
    const discovered = await discoverPorts();
    for (const d of discovered) {
        try {
            const list = await getJson(`http://127.0.0.1:${d.port}/json/list`);
            for (const t of list) {
                const url = t.url || '';
                const title = t.title || '';
                if (url.includes('chrome-extension://') ||
                    url.includes('chrome://') ||
                    url.includes('diff_worker') ||
                    url.includes('stripe.com') ||
                    url.includes('stripe.network') ||
                    url.includes('accounts.google.com') ||
                    (!title && !url)) continue;

                const isWorkbench = url.includes('workbench');
                const isConversation = url.includes('/c/');
                const isLocalPage = url.startsWith('file://') || url.startsWith('http://localhost') || url.startsWith('https://127.0.0.1');

                if (isWorkbench || isConversation || isLocalPage) {
                    const productType = d.productType || await getRunningProductType() || 'unknown';
                    const pName = extractProjectName(title);
                    // If process scan already found this project with port=null,
                    // BACKFILL the CDP port onto the existing target (don't skip!)
                    const existing = pName ? targets.find(tgt => tgt.projectName === pName && !tgt.port) : null;
                    if (existing) {
                        existing.port = d.port;
                        existing.url = url;
                        existing.webSocketDebuggerUrl = t.webSocketDebuggerUrl;
                        existing.id = t.id;
                        existing.source = 'process_scan+cdp';
                        existing.productType = existing.productType || productType;
                        continue;
                    }
                    if (pName && seenProjects.has(pName)) continue; // true duplicate
                    if (pName) seenProjects.add(pName);
                    targets.push({
                        connectorId: CONNECTOR_ID,
                        id: t.id,
                        title: title,
                        projectName: pName,
                        port: d.port,
                        url: url,
                        webSocketDebuggerUrl: t.webSocketDebuggerUrl,
                        conversationId: extractConversationId(url),
                        isConversation: url.includes('/c/'),
                        pid: d.pid,
                        productType,
                        source: 'cdp',
                    });
                }
            }
        } catch (e) { }
    }
    return targets;
}

const makePokeExpression = (messageContent) => `(async () => {
    const text = ${JSON.stringify(messageContent)};

    // 1. Check for blocking "Cancel" button (Agent is busy)
    const cancel = document.querySelector('[data-tooltip-id="input-send-button-cancel-tooltip"]');
    if (cancel && cancel.offsetParent !== null) {
        if (text.toUpperCase().includes('ABORT') || text.toUpperCase().includes('STOP')) {
            cancel.click();
            return { ok: true, method: "click_cancel" };
        }
        return { ok:false, reason:"busy_cancel_visible" };
    }

    // Helper: Find editor in a specific root (document or iframe)
    function findInRoot(root) {
        if (!root || !root.querySelectorAll) return null;
        const selector = '#cascade [data-lexical-editor="true"][contenteditable="true"][role="textbox"], ' +
                         'div[contenteditable="true"][role="combobox"], ' +
                         'div[contenteditable="true"][role="textbox"], ' +
                         '.monaco-editor textarea, ' +
                         'textarea[aria-label*="Ask"], textarea[aria-label*="Chat"], ' +
                         'div.interactive-input-part textarea, ' +
                         'textarea[placeholder*="Ask"], textarea[placeholder*="Message"]';
        const candidates = [...root.querySelectorAll(selector)];
        const visible = candidates.filter(el => el.offsetParent !== null || el.getBoundingClientRect().width > 0);
        return visible.length > 0 ? visible.at(-1) : candidates.at(-1);
    }

    async function findEditorAsync() {
        for (let i = 0; i < 5; i++) {
            let found = findInRoot(document);
            if (found) return found;
            const iframes = document.querySelectorAll('iframe, webview');
            for (const frame of iframes) {
                try {
                    const doc = frame.contentDocument;
                    if (doc) {
                        found = findInRoot(doc);
                        if (found) return found;
                    }
                } catch (e) { }
            }
            if (i < 4) await new Promise(r => setTimeout(r, 300));
        }
        return null;
    }
    // Try finding editor immediately (v2.0 has it always visible)
    let editor = await findEditorAsync();
    
    // Fallback: Try opening chat panel via Cmd+L (legacy Antigravity)
    if (!editor) {
        document.dispatchEvent(new KeyboardEvent('keydown', { metaKey: true, key: 'l', code: 'KeyL', keyCode: 76, bubbles: true }));
        await new Promise(r => setTimeout(r, 800));
        editor = await findEditorAsync();
    }
    if (!editor) return { ok:false, error:"editor_not_found" };

    // text definition moved up

    const lowerText = text.toLowerCase().trim();
    const root = editor.getRootNode();
    const buttons = Array.from((root.querySelectorAll || document.querySelectorAll).call(root, 'button'));
    
    // Check for Approval buttons (Accept/Reject)
    const approveBtn = buttons.find(b => b.textContent && (b.textContent.toLowerCase().includes('approve') || b.textContent.toLowerCase().includes('accept')));
    const rejectBtn = buttons.find(b => b.textContent && (b.textContent.toLowerCase().includes('reject') || b.textContent.toLowerCase().includes('deny')));

    if (approveBtn && (lowerText === 'yes' || lowerText === 'y' || lowerText === 'approve' || lowerText === 'accept' || lowerText.includes('accept all'))) {
        approveBtn.click();
        return { ok: true, method: "click_approve" };
    }
    
    if (rejectBtn && (lowerText === 'no' || lowerText === 'n' || lowerText === 'reject' || lowerText === 'deny')) {
        rejectBtn.click();
        return { ok: true, method: "click_reject" };
    }

    editor.focus();
    document.execCommand?.("selectAll", false, null);
    document.execCommand?.("delete", false, null);

    let inserted = false;
    try { inserted = !!document.execCommand?.("insertText", false, text); } catch {}
    if (!inserted) {
        if (editor.tagName === 'TEXTAREA' || editor.tagName === 'INPUT') {
            const nativeInputValueSetter = Object.getOwnPropertyDescriptor(window.HTMLTextAreaElement.prototype, "value")?.set || 
                                           Object.getOwnPropertyDescriptor(window.HTMLInputElement.prototype, "value")?.set;
            if (nativeInputValueSetter) {
                nativeInputValueSetter.call(editor, text);
            } else {
                editor.value = text;
            }
            editor.dispatchEvent(new Event('input', { bubbles: true }));
        } else {
            // For Lexical and contenteditables, textContent breaks the internal state.
            // Dispatching a paste event is universally supported by rich text editors.
            const dataTransfer = new DataTransfer();
            dataTransfer.setData('text/plain', text);
            const pasteEvent = new ClipboardEvent('paste', {
                clipboardData: dataTransfer,
                bubbles: true,
                cancelable: true
            });
            editor.dispatchEvent(pasteEvent);
            
            // If it still didn't update (extremely rare), fallback to textContent
            if (!editor.textContent.includes(text.substring(0, 5))) {
                editor.textContent = text;
                editor.dispatchEvent(new InputEvent("input", { bubbles:true, inputType:"insertText", data:text }));
            }
        }
    }

    // Wait for React/Lexical state to flush the new text
    await new Promise(r => setTimeout(r, 150));

    const submit = (root.querySelector || document.querySelector).call(root, "svg.lucide-arrow-right")?.closest("button") || 
                   (root.querySelector || document.querySelector).call(root, '[aria-label="Send Message"]') ||
                   (root.querySelector || document.querySelector).call(root, '.codicon-send') ||
                   buttons.find(b => b.textContent?.toLowerCase() === 'send');

    if (submit && !submit.disabled) {
        submit.click();
        return { ok:true, method:"click_submit" };
    }

    editor.dispatchEvent(new KeyboardEvent("keydown", { bubbles:true, key:"Enter", code:"Enter", keyCode: 13 }));
    editor.dispatchEvent(new KeyboardEvent("keyup", { bubbles:true, key:"Enter", code:"Enter", keyCode: 13 }));

    return { ok:true, method:"enter_fallback", submitFound: !!submit, submitDisabled: submit?.disabled ?? null };
})()`;

async function internalPoke(target, messageContent) {
    if (!target || !target.webSocketDebuggerUrl) {
        // No CDP WebSocket available — do NOT fall back to AppleScript keystrokes.
        // AppleScript keystroke injection is fundamentally unsafe: it types into
        // whichever window happens to be focused, which can be a code editor.
        // MemFlow is the primary delivery mechanism; CDP is just a notification layer.
        return { ok: false, error: "cdp_not_available", details: "No WebSocket debugger URL. MemFlow handles delivery." };
    }

    let WebSocketClass = global.WebSocket;
    if (!WebSocketClass) {
        try {
            const wsModule = await import('ws');
            WebSocketClass = wsModule.default;
        } catch (e) {
            return { ok: false, error: "ws_module_missing", details: e.message };
        }
    }
    const ws = new WebSocketClass(target.webSocketDebuggerUrl);

    await new Promise((resolve, reject) => {
        ws.onopen = resolve;
        ws.onerror = reject;
    });

    let idCounter = 1;
    const call = (method, params) => new Promise((resolve, reject) => {
        const id = idCounter++;
        const handler = (msg) => {
            const data = JSON.parse(msg.data);
            if (data.id === id) {
                ws.removeEventListener('message', handler);
                if (data.error) reject(data.error);
                else resolve(data.result);
            }
        };
        ws.addEventListener('message', handler);
        setTimeout(() => {
            ws.removeEventListener('message', handler);
            reject(new Error("RPC Timeout"));
        }, 3000);
        ws.send(JSON.stringify({ id, method, params }));
    });

    try {
        await call("Runtime.enable", {});
        await call("Page.enable", {}).catch(() => {}); // may not be available on all targets

        // Send Cmd+L (Meta+L) to open the chat panel
        try {
            await call("Input.dispatchKeyEvent", { type: "rawKeyDown", modifiers: 8, windowsVirtualKeyCode: 76, key: "l" });
            await call("Input.dispatchKeyEvent", { type: "keyUp", modifiers: 8, windowsVirtualKeyCode: 76, key: "l" });
            await new Promise(r => setTimeout(r, 200)); // wait for UI
        } catch (e) { /* ignore if Input domain unsupported */ }

        const expression = makePokeExpression(messageContent);

        // Strategy 1: Evaluate in the default (main) execution context directly.
        // This works when the page is already loaded — no need to wait for context events.
        try {
            const evalResult = await call("Runtime.evaluate", {
                expression,
                returnByValue: true,
                awaitPromise: true,
            });
            const val = evalResult?.result?.value;
            if (val?.ok) return val;
            if (val?.reason === "busy_cancel_visible") return { ok: false, reason: "busy" };
            // If editor not found in main context, fall through to frame search
        } catch (e) { /* try frames next */ }

        // Strategy 2: Enumerate all frames and evaluate in each execution context.
        // Covers Electron webviews and sandboxed iframes.
        try {
            const { frameTree } = await call("Page.getFrameTree", {});
            const frameIds = [];
            const collectFrames = (node) => {
                if (node?.frame?.id) frameIds.push(node.frame.id);
                for (const child of node?.childFrames || []) collectFrames(child);
            };
            collectFrames(frameTree);

            // Get all execution contexts for those frames
            const { contexts: allContexts } = await call("Runtime.getHeapUsage", {}).catch(() => ({ contexts: [] }));

            // Fallback: use frameId-based context execution
            for (const frameId of frameIds) {
                try {
                    const evalResult = await call("Runtime.evaluate", {
                        expression,
                        returnByValue: true,
                        awaitPromise: true,
                    });
                    const val = evalResult?.result?.value;
                    if (val?.ok) return val;
                } catch (e) { }
            }
        } catch (e) { /* Page.getFrameTree not supported */ }

        return { ok: false, error: "editor_not_found_in_any_context" };
    } catch (err) {
        return { ok: false, error: "runtime_error", details: err.message };
    } finally {
        ws.close();
    }
}

export async function poke(target, messageContent) {
    // Try the initial poke first
    let result = await internalPoke(target, messageContent);
    if (result.ok) return result;

    // Connection or RPC failure — enter self-healing recovery flow
    console.warn(`[POKE RECOVERY] Delivery failed: ${result.error || 'unknown'}. Re-scanning targets to self-heal...`);
    
    try {
        const freshTargets = await getTargets();
        if (freshTargets.length > 0) {
            // Match the target by project folder name, window title, or url
            const norm = (s) => (s || '').toLowerCase().replace(/[-_]/g, '-');
            const match = freshTargets.find(t => 
                (target.projectName && norm(t.projectName) === norm(target.projectName)) || 
                (target.title && norm(t.title) === norm(target.title)) || 
                (target.url && t.url === target.url)
            ) || null; // NO fallback to [0] — wrong project is worse than no delivery

            if (match && match.webSocketDebuggerUrl !== target.webSocketDebuggerUrl) {
                console.log(`[POKE RECOVERY] Recovered active target on port ${match.port}. Retrying connection...`);
                result = await internalPoke(match, messageContent);
                if (result.ok) {
                    console.log(`[POKE RECOVERY] Auto-recovery SUCCESS: Connection restored and delivered via port ${match.port}!`);
                    return result;
                }
            }
        }
    } catch (err) {
        console.error(`[POKE RECOVERY] Error during self-healing:`, err.message);
    }

    // No AppleScript fallback — it's fundamentally unsafe (keystroke injection
    // into the wrong window corrupts source files). MemFlow is the sole delivery
    // mechanism; CDP is an optional wake-up signal only.

    return result;
}
