/**
 * Enhanced Language Switcher for TubeWhale Admin
 * Provides smooth language switching with auto-reload
 */

(function() {
    'use strict';
    
    // Initialize language switcher
    document.addEventListener('DOMContentLoaded', function() {
        initLanguageSwitcher();
    });
    
    function initLanguageSwitcher() {
        const langLinks = document.querySelectorAll('.lang-btn');
        
        langLinks.forEach(function(link) {
            link.addEventListener('click', function(e) {
                e.preventDefault();
                
                const url = this.getAttribute('href');
                const targetLang = extractLangFromUrl(url);
                
                // Add loading state
                this.style.opacity = '0.6';
                this.style.pointerEvents = 'none';
                
                // Show loading indicator
                showLoadingIndicator();
                
                // Navigate to new URL
                window.location.href = url;
            });
            
            // Add keyboard support
            link.addEventListener('keydown', function(e) {
                if (e.key === 'Enter' || e.key === ' ') {
                    e.preventDefault();
                    this.click();
                }
            });
        });
    }
    
    function extractLangFromUrl(url) {
        const match = url.match(/admin_lang=([^&]+)/);
        return match ? match[1] : null;
    }
    
    function showLoadingIndicator() {
        const switcher = document.querySelector('.language-switcher');
        if (switcher) {
            const loading = document.createElement('div');
            loading.className = 'lang-loading';
            loading.innerHTML = '⟳';
            loading.style.cssText = `
                position: absolute;
                top: 50%;
                left: 50%;
                transform: translate(-50%, -50%);
                font-size: 16px;
                animation: spin 1s linear infinite;
            `;
            
            // Add spinner animation
            if (!document.querySelector('#lang-spinner-style')) {
                const style = document.createElement('style');
                style.id = 'lang-spinner-style';
                style.textContent = `
                    @keyframes spin {
                        0% { transform: translate(-50%, -50%) rotate(0deg); }
                        100% { transform: translate(-50%, -50%) rotate(360deg); }
                    }
                `;
                document.head.appendChild(style);
            }
            
            switcher.style.position = 'relative';
            switcher.appendChild(loading);
            
            // Remove after timeout
            setTimeout(() => {
                if (loading.parentNode) {
                    loading.parentNode.removeChild(loading);
                }
            }, 3000);
        }
    }
    
    // Auto-detect language preference changes
    function detectLanguageChange() {
        const currentLang = document.documentElement.getAttribute('lang') || 'en';
        const storedLang = localStorage.getItem('tubewhale_admin_lang');
        
        if (storedLang && storedLang !== currentLang) {
            // Language changed, update storage
            localStorage.setItem('tubewhale_admin_lang', currentLang);
        }
    }
    
    // Initialize on page load
    detectLanguageChange();
    
})();
