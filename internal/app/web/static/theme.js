// Applies the saved appearance before first paint so pages never flash.
(function () {
    try {
        var theme = localStorage.getItem('deployer-theme');
        if (theme === 'light' || theme === 'dark') document.documentElement.dataset.theme = theme;
    } catch (error) { /* Storage can be unavailable in private modes. */ }
}());
