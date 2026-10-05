(() => {
    const form = document.getElementById('notification-channel-form');
    if (!form) return;
    const editor = document.getElementById('notification-editor');
    const container = document.getElementById('notification-channels');
    const status = document.getElementById('notification-settings-status');
    const saveStatus = document.getElementById('notification-save-status');
    const endpointField = document.getElementById('channel-endpoint-field');
    const browserHelp = document.getElementById('browser-channel-help');
    const names = { browser: 'Browser', discord: 'Discord', slack: 'Slack', webhook: 'Webhook' };

    async function request(url, options = {}) {
        const response = await apiFetch(url, options);
        const data = await response.json();
        if (!response.ok) throw new Error(data.error || 'Request failed');
        return data;
    }

    function updateKind() {
        const browser = form.elements.kind.value === 'browser';
        endpointField.hidden = browser;
        browserHelp.hidden = !browser;
        form.elements.endpoint.required = !browser && !form.elements.id.value;
    }

    function openEditor(channel = null) {
        form.reset();
        form.elements.id.value = channel ? channel.id : '';
		form.elements.kind.disabled = !!channel;
        document.getElementById('channel-editor-title').textContent = channel ? 'Edit notification channel' : 'Add notification channel';
        if (channel) {
            form.elements.name.value = channel.name;
            form.elements.kind.value = channel.kind;
            form.elements.enabled.checked = channel.enabled;
            form.dataset.browserId = channel.browser_id;
            form.querySelectorAll('[name="events"]').forEach(input => { input.checked = channel.events.includes(input.value); });
            form.querySelectorAll('[name="project_ids"]').forEach(input => { input.checked = channel.project_ids.includes(Number(input.value)); });
        } else {
            form.dataset.browserId = browserNotificationDevice();
        }
        document.getElementById('channel-endpoint-help').textContent = channel && channel.endpoint_configured
            ? 'Leave blank to keep the saved webhook URL. Enter a new URL to replace it.'
            : 'The URL is stored encrypted and never shown again.';
        saveStatus.textContent = '';
        editor.hidden = false;
        updateKind();
        form.elements.name.focus();
    }

    function button(label, handler, danger = false) {
        const element = document.createElement('button');
        element.type = 'button';
        element.className = danger ? 'btn btn-danger' : 'btn';
        element.textContent = label;
        element.addEventListener('click', () => handler(element));
        return element;
    }

    async function loadChannels() {
        try {
            const data = await request('/api/notifications/channels');
            container.replaceChildren();
            status.textContent = data.channels.length ? '' : 'No channels yet. Add your first channel to receive deployment alerts.';
            for (const channel of data.channels) {
                const card = document.createElement('section');
                card.className = 'notification-card';
                const heading = document.createElement('h2');
                heading.textContent = channel.name;
                const description = document.createElement('p');
                const projects = channel.project_ids.length ? `${channel.project_ids.length} selected projects` : 'All projects';
                description.textContent = `${names[channel.kind]} · ${channel.enabled ? 'Enabled' : 'Paused'} · ${projects} · ${channel.events.join(', ')}`;
                if (channel.kind === 'browser' && channel.browser_id !== browserNotificationDevice()) description.textContent += ' · Another browser';
                const actions = document.createElement('div');
                actions.className = 'header-actions';
                const history = document.createElement('div');
                history.className = 'notification-history';
                history.setAttribute('aria-live', 'polite');
                actions.append(button('Edit', () => openEditor(channel)));
                const test = button('Send test', async (btn) => {
                    const restore = setButtonBusy(btn, 'Queuing…');
                    try {
                        await request(`/api/notifications/channels/${channel.id}/test`, { method: 'POST' });
                        showToast('Test queued. Check delivery history for the result.', 'success');
                        if (channel.kind === 'browser') pollBrowserNotifications();
                    } catch (error) { showToast(error.message); } finally { restore(); }
                });
                test.disabled = !channel.enabled;
                actions.append(test);
                actions.append(button('Delivery history', async (btn) => {
                    const restore = setButtonBusy(btn, 'Loading…');
                    try {
                        const deliveries = await request(`/api/notifications/channels/${channel.id}/deliveries`);
                        history.replaceChildren();
                        if (!deliveries.length) history.textContent = 'No deliveries yet.';
                        for (const delivery of deliveries) {
                            const row = document.createElement('p');
                            const build = delivery.build_id ? `Build #${delivery.build_id}` : 'Test';
                            row.textContent = `${build} · ${delivery.status.replaceAll('_', ' ')} · ${delivery.attempts} attempts · ${new Date(delivery.created_at).toLocaleString()}${delivery.last_error ? ` · ${delivery.last_error}` : ''}`;
                            history.append(row);
                        }
                    } catch (error) { history.textContent = error.message; } finally { restore(); }
                }));
                actions.append(button('Delete', async (btn) => {
                    if (!await confirmAction({ title: 'Delete notification channel', message: `Delete “${channel.name}” and its delivery history?`, confirmText: 'Delete', danger: true })) return;
                    const restore = setButtonBusy(btn, 'Deleting…');
                    try {
                        await request(`/api/notifications/channels/${channel.id}`, { method: 'DELETE' });
                        await loadChannels();
                    } catch (error) { showToast(error.message); } finally { restore(); }
                }, true));
                card.append(heading, description, actions, history);
                container.append(card);
            }
        } catch (error) { status.textContent = `Could not load your channels: ${error.message}`; }
    }

    form.addEventListener('submit', async (event) => {
        event.preventDefault();
        const submit = form.querySelector('[type="submit"]');
        const restore = setButtonBusy(submit, 'Saving…');
        saveStatus.textContent = '';
        try {
            const kind = form.elements.kind.value;
            if (kind === 'browser' && form.elements.enabled.checked) {
                if (!('Notification' in window)) throw new Error('This browser does not support notifications. Use a webhook channel instead.');
                const permission = await Notification.requestPermission();
                if (permission !== 'granted') throw new Error('Allow notifications in your browser settings before enabling this channel.');
            }
            const id = form.elements.id.value;
            const payload = {
                name: form.elements.name.value,
                kind,
                endpoint: form.elements.endpoint.value.trim(),
                browser_id: kind === 'browser' ? (form.dataset.browserId || browserNotificationDevice()) : '',
                project_ids: [...form.querySelectorAll('[name="project_ids"]:checked')].map(input => Number(input.value)),
                events: [...form.querySelectorAll('[name="events"]:checked')].map(input => input.value),
                enabled: form.elements.enabled.checked,
            };
            await request(`/api/notifications/channels${id ? `/${id}` : ''}`, { method: id ? 'PUT' : 'POST', body: JSON.stringify(payload), headers: { 'Content-Type': 'application/json' } });
            form.elements.endpoint.value = '';
            editor.hidden = true;
            await loadChannels();
			document.getElementById('add-notification-channel').focus();
            showToast('Notification channel saved.', 'success');
        } catch (error) { saveStatus.textContent = error.message; } finally { restore(); }
    });
    document.getElementById('add-notification-channel').addEventListener('click', () => openEditor());
    document.getElementById('cancel-channel-editor').addEventListener('click', () => { form.elements.endpoint.value = ''; editor.hidden = true; document.getElementById('add-notification-channel').focus(); });
    form.elements.kind.addEventListener('change', updateKind);
    loadChannels();
})();
