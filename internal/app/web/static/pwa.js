// Installation uses the browser's native app menu. Never cache admin data.
if ('serviceWorker' in navigator && window.isSecureContext) {
    window.addEventListener('load', () => {
        navigator.serviceWorker.register('/service-worker.js', {scope: '/', updateViaCache: 'none'})
            .catch(error => console.warn('Deployer offline support could not start:', error));
    });
}
