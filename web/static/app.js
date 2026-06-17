const csrfHeader = { 'X-Deployer-CSRF': '1' };

function apiFetch(url, options = {}) {
    return fetch(url, {
        ...options,
        headers: {
            ...csrfHeader,
            ...(options.headers || {}),
        },
    });
}

function isPlainObject(value) {
    return value !== null && typeof value === 'object' && !Array.isArray(value);
}

function parseJSONObjectField(rawValue, label, { requireStringValues = false, example = '' } = {}) {
    const text = (rawValue || '').trim();
    if (!text) return {};

    let value;
    try {
        value = JSON.parse(text);
    } catch (err) {
        throw new Error(`${label} must be valid JSON. ${err.message}`);
    }

    if (!isPlainObject(value)) {
        throw new Error(`${label} must be a JSON object${example ? `, for example ${example}` : ''}.`);
    }
    if (requireStringValues) {
        for (const [key, entry] of Object.entries(value)) {
            if (typeof entry !== 'string') {
                throw new Error(`${label}.${key} must be a string value. Use quotes around values${example ? `, for example ${example}` : ''}.`);
            }
        }
    }
    return value;
}

function setButtonBusy(button, busyText) {
    if (!button) return () => {};
    const previousText = button.textContent;
    const wasDisabled = button.disabled;
    button.disabled = true;
    button.setAttribute('aria-busy', 'true');
    if (busyText) button.textContent = button.classList.contains('btn-deploy-sm') ? '...' : busyText;
    return () => {
        button.disabled = wasDisabled;
        button.removeAttribute('aria-busy');
        button.textContent = previousText;
    };
}

function toastRegion() {
    let region = document.getElementById('toast-region');
    if (region) return region;

    region = document.createElement('div');
    region.id = 'toast-region';
    region.className = 'toast-region';
    region.setAttribute('aria-live', 'polite');
    document.body.appendChild(region);
    return region;
}

function showToast(message, type = 'error') {
    const toast = document.createElement('div');
    toast.className = `toast toast-${type}`;
    toast.setAttribute('role', type === 'error' ? 'alert' : 'status');

    const text = document.createElement('span');
    text.textContent = message;
    toast.appendChild(text);

    const close = document.createElement('button');
    close.type = 'button';
    close.className = 'toast-close';
    close.setAttribute('aria-label', 'Dismiss message');
    close.textContent = 'x';
    close.addEventListener('click', () => toast.remove());
    toast.appendChild(close);

    toastRegion().appendChild(toast);
    window.setTimeout(() => toast.remove(), type === 'error' ? 8000 : 3500);
}

function confirmAction({ title, message, confirmText = 'Confirm', danger = false }) {
    return new Promise(resolve => {
        const backdrop = document.createElement('div');
        backdrop.className = 'dialog-backdrop';

        const dialog = document.createElement('div');
        dialog.className = 'dialog';
        dialog.setAttribute('role', 'dialog');
        dialog.setAttribute('aria-modal', 'true');

        const heading = document.createElement('h2');
        heading.textContent = title;
        dialog.appendChild(heading);

        const body = document.createElement('p');
        body.textContent = message;
        dialog.appendChild(body);

        const actions = document.createElement('div');
        actions.className = 'dialog-actions';

        const cancel = document.createElement('button');
        cancel.type = 'button';
        cancel.className = 'btn';
        cancel.textContent = 'Cancel';

        const confirm = document.createElement('button');
        confirm.type = 'button';
        confirm.className = danger ? 'btn btn-danger' : 'btn btn-primary';
        confirm.textContent = confirmText;

        actions.appendChild(cancel);
        actions.appendChild(confirm);
        dialog.appendChild(actions);
        backdrop.appendChild(dialog);
        document.body.appendChild(backdrop);

        const close = result => {
            document.removeEventListener('keydown', onKeydown);
            backdrop.remove();
            resolve(result);
        };
        const onKeydown = event => {
            if (event.key === 'Escape') close(false);
        };
        cancel.addEventListener('click', () => close(false));
        confirm.addEventListener('click', () => close(true));
        backdrop.addEventListener('click', event => {
            if (event.target === backdrop) close(false);
        });
        document.addEventListener('keydown', onKeydown);
        confirm.focus();
    });
}

// Deploy a project
async function deploy(projectId, projectName, button) {
    const confirmed = await confirmAction({
        title: 'Deploy project',
        message: `Start a new deploy for ${projectName}?`,
        confirmText: 'Deploy',
    });
    if (!confirmed) return;
    const restoreButton = setButtonBusy(button, 'Deploying...');
    try {
        const res = await apiFetch(`/api/projects/${projectId}/deploy`, { method: 'POST' });
        const data = await res.json();
        if (!res.ok) { showToast(`Deploy failed: ${data.error}`); return; }
        window.location.href = `/builds/${data.build_id}`;
    } catch (err) { showToast(`Deploy failed: ${err.message}`); }
    finally { restoreButton(); }
}

// Fetch current project files from the remote agent
async function snapshotProject(projectId, projectName, button) {
    const confirmed = await confirmAction({
        title: 'Fetch remote state',
        message: `Ask the runner to package and upload the current remote state for ${projectName}?`,
        confirmText: 'Fetch',
    });
    if (!confirmed) return;
    const restoreButton = setButtonBusy(button, 'Fetching...');
    try {
        const res = await apiFetch(`/api/projects/${projectId}/snapshot`, { method: 'POST' });
        const data = await res.json();
        if (!res.ok) { showToast(`Snapshot failed: ${data.error}`); return; }
        window.location.href = `/builds/${data.build_id}`;
    } catch (err) { showToast(`Snapshot failed: ${err.message}`); }
    finally { restoreButton(); }
}

// Cancel a build
async function cancelBuild(buildId, button) {
    const confirmed = await confirmAction({
        title: 'Cancel build',
        message: 'Cancel this running build?',
        confirmText: 'Cancel build',
        danger: true,
    });
    if (!confirmed) return;
    const restoreButton = setButtonBusy(button, 'Cancelling...');
    try {
        const res = await apiFetch(`/api/builds/${buildId}/cancel`, { method: 'POST' });
        if (!res.ok) { const data = await res.json(); showToast(`Cancel failed: ${data.error}`); }
    } catch (err) { showToast(`Cancel failed: ${err.message}`); }
    finally { restoreButton(); }
}

// Delete a project
async function deleteProject(projectId, projectName, button) {
    const confirmed = await confirmAction({
        title: 'Delete project',
        message: `Delete "${projectName}" and all of its build history?`,
        confirmText: 'Delete',
        danger: true,
    });
    if (!confirmed) return;
    const restoreButton = setButtonBusy(button, 'Deleting...');
    try {
        const res = await apiFetch(`/api/projects/${projectId}`, { method: 'DELETE' });
        if (res.ok) { window.location.href = '/'; }
        else { const data = await res.json(); showToast(`Delete failed: ${data.error}`); }
    } catch (err) { showToast(`Delete failed: ${err.message}`); }
    finally { restoreButton(); }
}

// Save/create project
async function saveProject(event) {
    event.preventDefault();
    const form = event.target;
    const formData = new FormData(form);
    const deployMode = formData.get('deploy_mode') || 'docker';
    const project = {
        name: formData.get('name'),
        repo_path: formData.get('repo_path'),
        dockerfile_path: formData.get('dockerfile_path') || 'Dockerfile',
        compose_file: formData.get('compose_file') || 'docker-compose.yml',
        image_name: formData.get('image_name') || '',
        ssh_host: formData.get('ssh_host') || '',
        deploy_dir: formData.get('deploy_dir'),
        health_url: formData.get('health_url') || '',
        health_container: formData.get('health_container') || '',
        compose_services: formData.get('compose_services') || 'app worker',
        git_pull_before_build: form.querySelector('[name="git_pull_before_build"]').checked,
        runner_id: parseInt(formData.get('runner_id') || '0'),
        deploy_mode: deployMode,
        post_deploy: formData.get('post_deploy') || '',
        permissions: formData.get('permissions') || '',
        preserve: formData.get('preserve') || '',
    };
    try {
        project.build_args = parseJSONObjectField(formData.get('build_args') || '{}', 'Build Args', {
            requireStringValues: true,
            example: '{"APP_ENV":"production"}',
        });
        if (project.permissions) {
            parseJSONObjectField(project.permissions, 'Permissions', {
                example: '{"owner":"www-data:www-data"}',
            });
        }
    } catch (err) { showToast(err.message); return; }

    const idField = form.querySelector('[name="id"]');
    const isNew = !idField;
    const url = isNew ? '/api/projects' : `/api/projects/${idField.value}`;
    const submitButton = form.querySelector('button[type="submit"]');
    const restoreButton = setButtonBusy(submitButton, isNew ? 'Creating...' : 'Saving...');
    try {
        const res = await apiFetch(url, {
            method: isNew ? 'POST' : 'PUT',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(project),
        });
        const data = await res.json();
        if (!res.ok) { showToast(`Save failed: ${data.error}`); return; }
        window.location.href = `/projects/${data.id}`;
    } catch (err) { showToast(`Save failed: ${err.message}`); }
    finally { restoreButton(); }
}

// ========== Helpers ==========

function fmtDuration(seconds) {
    const s = Math.floor(seconds);
    if (s < 60) return s + 's';
    return Math.floor(s / 60) + 'm ' + (s % 60) + 's';
}

// Parse ##[endgroup:123] -> 123, or ##[endgroup] -> null
function parseEndGroup(line) {
    const m = line.match(/^##\[endgroup(?::(\d+))?\]$/);
    if (!m) return null;
    return m[1] !== undefined ? parseInt(m[1]) : null;
}

function getContainer() { return document.getElementById('steps-container'); }
function getSidebar() { return document.getElementById('sidebar-steps'); }

function span(className, text) {
    const el = document.createElement('span');
    el.className = className;
    el.textContent = text;
    return el;
}

function setChevron(header, open) {
    const chevron = header.querySelector('.step-chevron');
    if (chevron) chevron.textContent = open ? '\u25bc' : '\u25b6';
}

function setStatusIcon(icon, status) {
    if (!icon) return;
    if (status === 'failed') {
        icon.className = 'step-status-icon icon-x';
        icon.textContent = '\u2715';
    } else if (status === 'running') {
        icon.className = 'step-status-icon icon-running';
        icon.textContent = '\u21bb';
    } else {
        icon.className = 'step-status-icon icon-check';
        icon.textContent = '\u2713';
    }
}

function setSidebarIcon(icon, status) {
    if (!icon) return;
    if (status === 'failed') {
        icon.className = 'step-icon icon-x';
        icon.textContent = '\u2715';
    } else if (status === 'running') {
        icon.className = 'step-icon icon-running';
        icon.textContent = '\u21bb';
    } else {
        icon.className = 'step-icon icon-check';
        icon.textContent = '\u2713';
    }
}

function buildStepHeader(name, open, status) {
    const header = document.createElement('div');
    header.className = 'step-header';
    header.appendChild(span('step-chevron', open ? '\u25bc' : '\u25b6'));
    const icon = span('step-status-icon', '');
    setStatusIcon(icon, status);
    header.appendChild(icon);
    header.appendChild(span('step-name', name));
    header.appendChild(span('step-timer', status === 'running' ? '0s' : ''));
    return header;
}

function buildSidebarItem(name, status) {
    const sideItem = document.createElement('div');
    sideItem.className = status === 'running' ? 'step-item step-running' : 'step-item step-success';
    const icon = span('step-icon', '');
    setSidebarIcon(icon, status);
    sideItem.appendChild(icon);
    sideItem.appendChild(span('', name));
    sideItem.appendChild(span('step-item-dur', ''));
    return sideItem;
}

// ========== Batch render for completed builds ==========

function renderLogBatch(logText, finalStatus) {
    const container = getContainer();
    const sidebar = getSidebar();
    if (!container) return;

    const lines = logText.split('\n');
    let currentPre = null;
    let currentSection = null;
    let stepCount = 0;
    const fragment = document.createDocumentFragment();
    const sidebarFragment = document.createDocumentFragment();

    function startStep(name) {
        const section = document.createElement('div');
        section.className = 'step-section';

        const header = buildStepHeader(name, false, 'success');
        header.addEventListener('click', () => {
            section.classList.toggle('open');
            setChevron(header, section.classList.contains('open'));
        });

        const pre = document.createElement('pre');
        pre.className = 'step-log';

        section.appendChild(header);
        section.appendChild(pre);
        fragment.appendChild(section);

        currentSection = { el: section, header, pre, name };
        currentPre = pre;
        stepCount++;

        // Sidebar entry
        const sideItem = buildSidebarItem(name, 'success');
        sideItem.style.cursor = 'pointer';
        sideItem.dataset.stepIndex = stepCount - 1;
        sideItem.addEventListener('click', () => {
            section.scrollIntoView({ behavior: 'smooth', block: 'start' });
        });
        sidebarFragment.appendChild(sideItem);
    }

    function endStep(duration) {
        if (currentSection && duration !== null) {
            const timer = currentSection.header.querySelector('.step-timer');
            if (timer) timer.textContent = fmtDuration(duration);
            // Also set sidebar duration
            const idx = stepCount - 1;
            const sideItems = sidebarFragment.querySelectorAll('.step-item');
            if (sideItems[idx]) {
                const durEl = sideItems[idx].querySelector('.step-item-dur');
                if (durEl) durEl.textContent = fmtDuration(duration);
            }
        }
        currentSection = null;
        currentPre = null;
    }

    for (let i = 0; i < lines.length; i++) {
        const line = lines[i];

        if (line.startsWith('##[group]')) {
            if (currentSection) endStep(null);
            startStep(line.substring(9));
            continue;
        }

        const egDur = parseEndGroup(line);
        if (egDur !== null || line === '##[endgroup]') {
            endStep(egDur);
            continue;
        }

        if (currentPre) {
            currentPre.textContent += line + '\n';
        } else {
            if (!fragment.querySelector('.step-prelude')) {
                const pre = document.createElement('pre');
                pre.className = 'step-log step-prelude';
                fragment.prepend(pre);
                currentPre = pre;
            } else {
                currentPre = fragment.querySelector('.step-prelude');
            }
            if (line.trim()) currentPre.textContent += line + '\n';
            currentPre = currentSection ? currentSection.pre : null;
        }
    }

    // Mark last step as failed if build failed
    if (finalStatus === 'failed') {
        const allSections = fragment.querySelectorAll('.step-section');
        if (allSections.length > 0) {
            const lastSection = allSections[allSections.length - 1];
            const icon = lastSection.querySelector('.step-status-icon');
            setStatusIcon(icon, 'failed');
            lastSection.classList.add('open');
            setChevron(lastSection, true);
        }
        const sideItems = sidebarFragment.querySelectorAll('.step-item');
        if (sideItems.length > 0) {
            const last = sideItems[sideItems.length - 1];
            last.className = 'step-item step-failed';
            const si = last.querySelector('.step-icon');
            setSidebarIcon(si, 'failed');
        }
    }

    // If success, open last step by default
    if (finalStatus === 'success') {
        const allSections = fragment.querySelectorAll('.step-section');
        if (allSections.length > 0) {
            const lastSection = allSections[allSections.length - 1];
            lastSection.classList.add('open');
            setChevron(lastSection, true);
        }
    }

    container.appendChild(fragment);
    paginateLogBlocks(container);
    if (sidebar) sidebar.appendChild(sidebarFragment);
}

function paginateLogBlocks(root) {
    const pageSize = 300;
    root.querySelectorAll('pre.step-log').forEach(pre => {
        const lines = pre.textContent.split('\n');
        if (lines.length <= pageSize) return;

        let page = 1;
        const totalPages = Math.ceil(lines.length / pageSize);
        const pager = document.createElement('div');
        pager.className = 'step-log-pager';

        const prev = document.createElement('button');
        prev.type = 'button';
        prev.className = 'btn';
        prev.textContent = 'Previous';

        const status = document.createElement('span');
        status.className = 'pagination-status';

        const next = document.createElement('button');
        next.type = 'button';
        next.className = 'btn';
        next.textContent = 'Next';

        const render = () => {
            const start = (page - 1) * pageSize;
            const end = Math.min(start + pageSize, lines.length);
            pre.textContent = lines.slice(start, end).join('\n');
            status.textContent = `Log page ${page} of ${totalPages}`;
            prev.disabled = page <= 1;
            next.disabled = page >= totalPages;
        };

        prev.addEventListener('click', event => {
            event.stopPropagation();
            page--;
            render();
        });
        next.addEventListener('click', event => {
            event.stopPropagation();
            page++;
            render();
        });

        pager.append(prev, status, next);
        pre.insertAdjacentElement('afterend', pager);
        render();
    });
}

// ========== Live streaming for running builds ==========

let liveSteps = [];
let liveCurrentStep = null;
let livePrelude = null;

function liveAppendLine(line) {
    const container = getContainer();
    if (!container) return;

    if (line.startsWith('##[group]')) {
        if (liveCurrentStep) liveCloseStep(null);
        liveOpenStep(line.substring(9));
        return;
    }

    const egDur = parseEndGroup(line);
    if (egDur !== null || line === '##[endgroup]') {
        liveCloseStep(egDur);
        return;
    }

    if (liveCurrentStep) {
        liveCurrentStep.pre.textContent += line + '\n';
        liveCurrentStep.pre.scrollTop = liveCurrentStep.pre.scrollHeight;
    } else {
        if (!livePrelude) {
            livePrelude = document.createElement('pre');
            livePrelude.className = 'step-log step-prelude';
            container.prepend(livePrelude);
        }
        if (line.trim()) livePrelude.textContent += line + '\n';
    }
}

function liveOpenStep(name) {
    const container = getContainer();
    const sidebar = getSidebar();

    // Close previous steps (collapse them)
    if (liveCurrentStep) {
        liveCurrentStep.el.classList.remove('open');
        setChevron(liveCurrentStep.header, false);
    }

    const section = document.createElement('div');
    section.className = 'step-section open';

    const header = buildStepHeader(name, true, 'running');
    header.addEventListener('click', () => {
        section.classList.toggle('open');
        setChevron(header, section.classList.contains('open'));
    });

    const pre = document.createElement('pre');
    pre.className = 'step-log';

    section.appendChild(header);
    section.appendChild(pre);
    container.appendChild(section);

    const step = { name, el: section, header, pre, startTime: Date.now() };

    // Live step timer
    step.timerInterval = setInterval(() => {
        const elapsed = Math.floor((Date.now() - step.startTime) / 1000);
        const timer = step.header.querySelector('.step-timer');
        if (timer) timer.textContent = fmtDuration(elapsed);
    }, 1000);

    liveSteps.push(step);
    liveCurrentStep = step;

    if (sidebar) {
        const sideItem = buildSidebarItem(name, 'running');
        sideItem.dataset.stepIndex = liveSteps.length - 1;
        sideItem.style.cursor = 'pointer';
        sideItem.addEventListener('click', () => section.scrollIntoView({ behavior: 'smooth' }));
        sidebar.appendChild(sideItem);
    }
}

function liveCloseStep(serverDuration) {
    if (!liveCurrentStep) return;

    // Stop timer
    if (liveCurrentStep.timerInterval) clearInterval(liveCurrentStep.timerInterval);

    // Use server duration if available, otherwise calculate from JS time
    const dur = serverDuration !== null ? serverDuration : Math.floor((Date.now() - liveCurrentStep.startTime) / 1000);
    const timer = liveCurrentStep.header.querySelector('.step-timer');
    if (timer) timer.textContent = fmtDuration(dur);

    const icon = liveCurrentStep.header.querySelector('.step-status-icon');
    setStatusIcon(icon, 'success');

    // Collapse
    liveCurrentStep.el.classList.remove('open');
    setChevron(liveCurrentStep.header, false);

    const sidebar = getSidebar();
    if (sidebar) {
        const idx = liveSteps.length - 1;
        const si = sidebar.querySelector(`[data-step-index="${idx}"]`);
        if (si) {
            si.className = 'step-item step-success';
            const sIcon = si.querySelector('.step-icon');
            setSidebarIcon(sIcon, 'success');
            const durEl = si.querySelector('.step-item-dur');
            if (durEl) durEl.textContent = fmtDuration(dur);
        }
    }
    liveCurrentStep = null;
}

function liveMarkFinished(status) {
    if (liveCurrentStep) {
        if (liveCurrentStep.timerInterval) clearInterval(liveCurrentStep.timerInterval);

        const icon = liveCurrentStep.header.querySelector('.step-status-icon');
        setStatusIcon(icon, status === 'failed' ? 'failed' : 'success');

        const sidebar = getSidebar();
        if (sidebar) {
            const idx = liveSteps.length - 1;
            const si = sidebar.querySelector(`[data-step-index="${idx}"]`);
            if (si) {
                si.className = status === 'failed' ? 'step-item step-failed' : 'step-item step-success';
                const sIcon = si.querySelector('.step-icon');
                setSidebarIcon(sIcon, status === 'failed' ? 'failed' : 'success');
            }
        }
        liveCurrentStep = null;
    }
}

// ========== Live Build Timer ==========

let buildTimerInterval = null;

function startBuildTimer(startedAt) {
    const timerEl = document.getElementById('build-timer');
    if (!timerEl) return;

    buildTimerInterval = setInterval(() => {
        const elapsed = Math.floor((Date.now() - startedAt.getTime()) / 1000);
        timerEl.textContent = fmtDuration(elapsed);
    }, 1000);
}

function stopBuildTimer() {
    if (buildTimerInterval) {
        clearInterval(buildTimerInterval);
        buildTimerInterval = null;
    }
}

// ========== Init ==========

function initBuildLog(buildId, buildStatus, logB64, startedAt, duration) {
    const statusEl = document.getElementById('build-status');

    // For completed builds -- render log in one pass, show final duration
    if (buildStatus !== 'running') {
        if (logB64) {
            try {
                const logText = atob(logB64);
                renderLogBatch(logText, buildStatus);
            } catch (e) {
                console.error('Failed to decode log:', e);
            }
        }
        return;
    }

    // For running builds -- start live timer
    startBuildTimer(startedAt);

    // Use SSE for live streaming
    const source = new EventSource(`/api/builds/${buildId}/stream`);

    source.onmessage = function(event) {
        liveAppendLine(event.data);
    };

    source.addEventListener('status', function(event) {
        const status = event.data;
        if (statusEl) {
            statusEl.className = 'build-badge ' + status;
            statusEl.textContent = status;
        }
        liveMarkFinished(status);
        stopBuildTimer();
        source.close();
        if (status !== 'running') {
            setTimeout(() => window.location.reload(), 1500);
        }
    });

    source.onerror = function() {
        source.close();
        fetch(`/api/builds/${buildId}`)
            .then(r => r.json())
            .then(build => { if (build.status !== 'running') window.location.reload(); })
            .catch(() => {});
    };
}

function toggleSSHField() {
    const select = document.getElementById('runner-select');
    const sshGroup = document.getElementById('ssh-host-group');
    if (!select || !sshGroup) return;
    if (parseInt(select.value) > 0) {
        sshGroup.style.opacity = '0.4';
        sshGroup.querySelector('input').removeAttribute('required');
    } else {
        sshGroup.style.opacity = '1';
    }
}

function toggleDeployMode() {
    const select = document.getElementById('deploy-mode-select');
    if (!select) return;
    const isFiles = select.value === 'files';
    document.querySelectorAll('.docker-field').forEach(el => {
        el.style.display = isFiles ? 'none' : '';
    });
    document.querySelectorAll('.files-field').forEach(el => {
        el.style.display = isFiles ? '' : 'none';
    });

    const imgInput = document.querySelector('[name="image_name"]');
    if (imgInput) {
        if (isFiles) imgInput.removeAttribute('required');
        else imgInput.setAttribute('required', '');
    }
}

function showAddRunner() {
    const form = document.getElementById('add-runner-form');
    if (!form) return;
    form.style.display = 'block';
    const name = document.getElementById('runner-name');
    if (name) name.focus();
}

function hideAddRunner() {
    const form = document.getElementById('add-runner-form');
    if (form) form.style.display = 'none';
}

function hideSetup() {
    const instructions = document.getElementById('setup-instructions');
    if (instructions) instructions.style.display = 'none';
    window.location.reload();
}

function listPagination(list) {
    let controls = list.nextElementSibling;
    if (!controls || !controls.classList.contains('pagination')) {
        controls = document.createElement('div');
        controls.className = 'pagination';
        list.insertAdjacentElement('afterend', controls);
    }
    return controls;
}

function updatePagedList(list) {
    const pageSize = parseInt(list.dataset.pageSize || '0');
    const rows = Array.from(list.querySelectorAll('.workflow-run'));
    const matching = rows.filter(row => row.dataset.filterMatch !== 'false');
    const empty = list.dataset.pageEmpty ? document.querySelector(list.dataset.pageEmpty) : null;

    if (!pageSize || rows.length <= pageSize) {
        rows.forEach(row => { row.hidden = row.dataset.filterMatch === 'false'; });
        const controls = list.nextElementSibling;
        if (controls && controls.classList.contains('pagination')) controls.remove();
        if (empty) empty.hidden = matching.length !== 0;
        return;
    }

    const totalPages = Math.max(1, Math.ceil(matching.length / pageSize));
    let page = parseInt(list.dataset.page || '1');
    if (page > totalPages) page = totalPages;
    if (page < 1) page = 1;
    list.dataset.page = String(page);

    let visibleIndex = 0;
    rows.forEach(row => {
        if (row.dataset.filterMatch === 'false') {
            row.hidden = true;
            return;
        }
        const visible = visibleIndex >= (page - 1) * pageSize && visibleIndex < page * pageSize;
        row.hidden = !visible;
        visibleIndex++;
    });

    const controls = listPagination(list);
    controls.replaceChildren();
    if (matching.length > pageSize) {
        const prev = document.createElement('button');
        prev.type = 'button';
        prev.className = 'btn';
        prev.textContent = 'Previous';
        prev.disabled = page <= 1;
        prev.addEventListener('click', () => {
            list.dataset.page = String(page - 1);
            updatePagedList(list);
        });

        const next = document.createElement('button');
        next.type = 'button';
        next.className = 'btn';
        next.textContent = 'Next';
        next.disabled = page >= totalPages;
        next.addEventListener('click', () => {
            list.dataset.page = String(page + 1);
            updatePagedList(list);
        });

        const status = document.createElement('span');
        status.className = 'pagination-status';
        status.textContent = `Page ${page} of ${totalPages}`;

        controls.append(prev, status, next);
    } else {
        controls.remove();
    }
    if (empty) empty.hidden = matching.length !== 0;
}

function initPagination() {
    document.querySelectorAll('[data-page-size]').forEach(list => {
        list.dataset.page = list.dataset.page || '1';
        list.querySelectorAll('.workflow-run').forEach(row => {
            row.dataset.filterMatch = row.dataset.filterMatch || 'true';
        });
        updatePagedList(list);
    });
}

function initFilters() {
    document.querySelectorAll('[data-filter-target]').forEach(input => {
        const target = document.querySelector(input.dataset.filterTarget);
        if (!target) return;
        const rows = Array.from(target.querySelectorAll('.workflow-run'));
        const applyFilter = () => {
            const query = input.value.trim().toLowerCase();
            rows.forEach(row => {
                const text = (row.dataset.filterText || row.textContent || '').toLowerCase();
                const match = query === '' || text.includes(query);
                row.dataset.filterMatch = match ? 'true' : 'false';
            });
            target.dataset.page = '1';
            updatePagedList(target);
        };
        input.addEventListener('input', applyFilter);
        applyFilter();
    });
}

async function createRunner(button) {
    const name = document.getElementById('runner-name').value.trim();
    const labels = document.getElementById('runner-labels').value.trim();
    if (!name) { showToast('Runner name is required.'); return; }

    const restoreButton = setButtonBusy(button, 'Creating...');
    try {
        const res = await apiFetch('/api/runners', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ name, labels }),
        });
        const data = await res.json();
        if (!res.ok) {
            showToast(`Create runner failed: ${data.error}`);
            return;
        }

        hideAddRunner();
        showRunnerSetup(data, 'create');
    } catch (err) {
        showToast(`Create runner failed: ${err.message}`);
    } finally { restoreButton(); }
}

function runnerServerURL() {
    const runnerConfig = document.getElementById('runner-config');
    const configuredURL = runnerConfig ? runnerConfig.dataset.publicUrl : '';
    return configuredURL || window.location.origin;
}

function showRunnerSetup(data, mode) {
    const serverURL = runnerServerURL();
    const title = mode === 'rotate' ? `Runner Token Rotated: ${data.name}` : `Runner Created: ${data.name}`;
    const commands = mode === 'rotate' ? `# Paste the runner token from the Deployer UI when prompted
read -r -s -p "Runner token: " DEPLOYER_TOKEN; echo

# Update this runner's token
sudo install -d -m 0755 /etc/deployer
sudo tee /etc/deployer/deployer-agent.env > /dev/null << EOF
DEPLOYER_SERVER=${serverURL}
DEPLOYER_TOKEN=\${DEPLOYER_TOKEN}
DEPLOYER_AGENT_AUTO_UPDATE=false
EOF
sudo chmod 600 /etc/deployer/deployer-agent.env
sudo systemctl restart deployer-agent` : `# Paste the runner token from the Deployer UI when prompted
read -r -s -p "Runner token: " DEPLOYER_TOKEN; echo

# Download the deployer binary
curl -H "Authorization: Bearer \${DEPLOYER_TOKEN}" -o deployer ${serverURL}/download/deployer && chmod +x deployer

# Test the connection
./deployer agent --server ${serverURL} --token "\${DEPLOYER_TOKEN}"

# (Optional) Install as a systemd service for auto-start
sudo mv deployer /usr/local/bin/deployer
sudo install -d -m 0755 /etc/deployer
sudo tee /etc/deployer/deployer-agent.env > /dev/null << EOF
DEPLOYER_SERVER=${serverURL}
DEPLOYER_TOKEN=\${DEPLOYER_TOKEN}
DEPLOYER_AGENT_AUTO_UPDATE=false
EOF
sudo chmod 600 /etc/deployer/deployer-agent.env

sudo tee /etc/systemd/system/deployer-agent.service > /dev/null << 'EOF'
[Unit]
Description=Deployer Agent (${data.name})
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
EnvironmentFile=/etc/deployer/deployer-agent.env
ExecStart=/usr/local/bin/deployer agent
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl daemon-reload
sudo systemctl enable --now deployer-agent`;

    document.getElementById('setup-title').textContent = title;
    document.getElementById('runner-token').textContent = data.token;
    document.getElementById('setup-commands').textContent = commands;
    document.getElementById('setup-instructions').style.display = 'block';
    window.scrollTo({ top: 0, behavior: 'smooth' });
}

function copyRunnerToken() {
    const text = document.getElementById('runner-token').textContent;
    navigator.clipboard.writeText(text).then(() => {
        showToast('Token copied to clipboard.', 'success');
    }).catch(err => showToast(`Copy failed: ${err.message}`));
}

function copySetupCommands() {
    const text = document.getElementById('setup-commands').textContent;
    navigator.clipboard.writeText(text).then(() => {
        showToast('Commands copied to clipboard.', 'success');
    }).catch(err => showToast(`Copy failed: ${err.message}`));
}

async function deleteRunner(id, name, button) {
    const confirmed = await confirmAction({
        title: 'Delete runner',
        message: `Delete runner "${name}"? Projects assigned to it will need another deployment path.`,
        confirmText: 'Delete',
        danger: true,
    });
    if (!confirmed) return;
    const restoreButton = setButtonBusy(button, 'Deleting...');
    try {
        const res = await apiFetch(`/api/runners/${id}`, { method: 'DELETE' });
        if (res.ok) {
            window.location.reload();
        } else {
            const data = await res.json();
            showToast(`Delete runner failed: ${data.error}`);
        }
    } catch (err) {
        showToast(`Delete runner failed: ${err.message}`);
    } finally { restoreButton(); }
}

async function rotateRunner(id, name, button) {
    const confirmed = await confirmAction({
        title: 'Rotate runner token',
        message: `Rotate the token for runner "${name}"? Existing agents using the old token will stop working.`,
        confirmText: 'Rotate token',
        danger: true,
    });
    if (!confirmed) return;
    const restoreButton = setButtonBusy(button, 'Rotating...');
    try {
        const res = await apiFetch(`/api/runners/${id}/rotate`, { method: 'POST' });
        const data = await res.json();
        if (!res.ok) {
            showToast(`Rotate token failed: ${data.error}`);
            return;
        }
        showRunnerSetup(data, 'rotate');
    } catch (err) {
        showToast(`Rotate token failed: ${err.message}`);
    } finally { restoreButton(); }
}

function initPage() {
    document.querySelectorAll('[data-deploy-project-id]').forEach(button => {
        button.addEventListener('click', () => deploy(parseInt(button.dataset.deployProjectId), button.dataset.projectName, button));
    });
    document.querySelectorAll('[data-snapshot-project-id]').forEach(button => {
        button.addEventListener('click', () => snapshotProject(parseInt(button.dataset.snapshotProjectId), button.dataset.projectName, button));
    });
    document.querySelectorAll('[data-delete-project-id]').forEach(button => {
        button.addEventListener('click', () => deleteProject(parseInt(button.dataset.deleteProjectId), button.dataset.projectName, button));
    });
    document.querySelectorAll('[data-cancel-build-id]').forEach(button => {
        button.addEventListener('click', () => cancelBuild(parseInt(button.dataset.cancelBuildId), button));
    });
    document.querySelectorAll('[data-nav-url]').forEach(button => {
        button.addEventListener('click', () => { window.location = button.dataset.navUrl; });
    });
    document.querySelectorAll('[data-build-url]').forEach(row => {
        row.addEventListener('click', () => { window.location = row.dataset.buildUrl; });
    });
    document.querySelectorAll('[data-delete-runner-id]').forEach(button => {
        button.addEventListener('click', () => deleteRunner(parseInt(button.dataset.deleteRunnerId), button.dataset.runnerName, button));
    });
    document.querySelectorAll('[data-rotate-runner-id]').forEach(button => {
        button.addEventListener('click', () => rotateRunner(parseInt(button.dataset.rotateRunnerId), button.dataset.runnerName, button));
    });
    document.querySelectorAll('[data-action="show-add-runner"]').forEach(button => {
        button.addEventListener('click', showAddRunner);
    });
    document.querySelectorAll('[data-action="hide-add-runner"]').forEach(button => {
        button.addEventListener('click', hideAddRunner);
    });
    document.querySelectorAll('[data-action="hide-setup"]').forEach(button => {
        button.addEventListener('click', hideSetup);
    });
    document.querySelectorAll('[data-action="create-runner"]').forEach(button => {
        button.addEventListener('click', () => createRunner(button));
    });
    document.querySelectorAll('[data-action="copy-setup-commands"]').forEach(button => {
        button.addEventListener('click', copySetupCommands);
    });
    document.querySelectorAll('[data-action="copy-runner-token"]').forEach(button => {
        button.addEventListener('click', copyRunnerToken);
    });

    const projectForm = document.getElementById('project-form');
    if (projectForm) projectForm.addEventListener('submit', saveProject);

    initPagination();
    initFilters();

    const runnerSelect = document.getElementById('runner-select');
    if (runnerSelect) {
        runnerSelect.addEventListener('change', toggleSSHField);
        toggleSSHField();
    }
    const modeSelect = document.getElementById('deploy-mode-select');
    if (modeSelect) {
        modeSelect.addEventListener('change', toggleDeployMode);
        toggleDeployMode();
    }

    const buildData = document.getElementById('build-data');
    if (buildData) {
        initBuildLog(
            parseInt(buildData.dataset.buildId),
            buildData.dataset.buildStatus,
            buildData.dataset.buildLogB64,
            new Date(buildData.dataset.buildStartedAt),
            parseInt(buildData.dataset.buildDuration || '0'),
        );
    }
}

document.addEventListener('DOMContentLoaded', initPage);
