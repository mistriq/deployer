#!/usr/bin/env python3
"""Local executable smoke check. Generates ephemeral credentials; submits no jobs."""
import json
import os
import pathlib
import secrets
import socket
import subprocess
import tempfile
import time
import urllib.error
import urllib.request

binary = pathlib.Path(__file__).resolve().parents[1] / 'bin/runtime-deployer'
with tempfile.TemporaryDirectory(prefix='runtime-cli-smoke-') as temp:
    root = pathlib.Path(temp)
    token = secrets.token_hex(24)

    def private(name, value):
        p = root / name
        p.write_text(value)
        p.chmod(0o600)
        return str(p)

    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        port = sock.getsockname()[1]
    env = os.environ.copy()
    env.update({
        'DEPLOYER_LISTEN': f'127.0.0.1:{port}',
        'DEPLOYER_STATE_PATH': str(root / 'state.enc'),
        'DEPLOYER_STATE_KEY_FILE': private('key', secrets.token_hex(32)),
        'PORTAL_TOKENS_FILE': private('portal', json.dumps({'smoke': token})),
        'RUNTIME_TOKEN_FILE': private('runtime', secrets.token_hex(24)),
        'RUNTIME_BASE_URL': 'https://runtime.invalid',
        'RUNTIME_SANDBOX': 'true',
        'REGISTRY_IMAGE_PREFIX': 'registry.invalid/apps',
        'REGISTRY_PUSH_USERNAME': '',
        'REGISTRY_PUSH_PASSWORD_FILE': '',
    })
    result = subprocess.run([str(binary), '--check-config'], env=env,
                            capture_output=True, text=True, timeout=10)
    assert result.returncode == 0, result.stderr
    proc = subprocess.Popen([str(binary)], env=env, stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE, text=True)
    try:
        target = f'http://127.0.0.1:{port}/internal/v1/deployments/missing'
        for _ in range(80):
            try:
                urllib.request.urlopen(target, timeout=.2)
            except urllib.error.HTTPError as error:
                assert error.code == 401
                break
            except urllib.error.URLError:
                time.sleep(.05)
        else:
            raise AssertionError('server did not start')
        request = urllib.request.Request(target, headers={'Authorization': 'Bearer ' + token})
        try:
            urllib.request.urlopen(request, timeout=3)
        except urllib.error.HTTPError as error:
            assert error.code == 404
        else:
            raise AssertionError('missing deployment returned success')
    finally:
        proc.terminate()
        out, err = proc.communicate(timeout=10)
    assert proc.returncode == 0, err
    assert token not in out + err
    print('PASS: configuration, authentication, authorized read, graceful shutdown; no deployment submitted.')
