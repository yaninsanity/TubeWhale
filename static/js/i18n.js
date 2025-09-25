/**
 * TubeWhale JavaScript Internationalization System
 * English-First Multilingual Support for Frontend
 */

class TubeWhaleI18n {
    constructor() {
        this.currentLanguage = document.documentElement.lang || 'en';
        this.fallbackLanguage = 'en';
        this.translations = {};
        this.rtlLanguages = ['ar', 'he', 'fa', 'ur'];
        
        // Initialize from Django context
        this.initializeFromDjango();
        
        // Load translations
        this.loadTranslations();
        
        // Setup language detection
        this.setupLanguageDetection();
        
        console.log(`🌍 TubeWhale i18n initialized for language: ${this.currentLanguage}`);
    }
    
    /**
     * Initialize from Django template context
     */
    initializeFromDjango() {
        // Get language info from Django context
        if (window.LANGUAGE_CONTEXT) {
            this.currentLanguage = window.LANGUAGE_CONTEXT.CURRENT_LANGUAGE || 'en';
            this.availableLanguages = window.LANGUAGE_CONTEXT.LANGUAGE_CODES || ['en'];
            this.isRTL = window.LANGUAGE_CONTEXT.IS_RTL || false;
            this.languageInfo = window.LANGUAGE_CONTEXT.CURRENT_LANGUAGE_INFO || {};
        }
    }
    
    /**
     * Load translation strings
     */
    loadTranslations() {
        // English base translations (always available)
        this.translations.en = {
            // Core interface
            'loading': 'Loading...',
            'error': 'Error',
            'success': 'Success',
            'warning': 'Warning',
            'info': 'Information',
            'close': 'Close',
            'cancel': 'Cancel',
            'save': 'Save',
            'delete': 'Delete',
            'edit': 'Edit',
            'submit': 'Submit',
            'search': 'Search',
            'filter': 'Filter',
            'refresh': 'Refresh',
            'back': 'Back',
            'next': 'Next',
            'previous': 'Previous',
            'continue': 'Continue',
            'finish': 'Finish',
            
            // Wizard interface
            'wizard_title': 'TubeWhale YouTube Analysis Wizard',
            'wizard_subtitle': 'Professional YouTube video analysis with deep thinking questions',
            'select_tool': 'Select Your Analysis Tool',
            'choose_role': 'Select Your Professional Role',
            'configure_analysis': 'Configure Analysis',
            'start_analysis': 'Start Analysis',
            'single_video': 'Single Video Analysis',
            'playlist_analysis': 'Playlist Analysis',
            'brainstorm_tool': 'Brainstorm Tool',
            'content_creator': 'Content Creator',
            'marketing_expert': 'Marketing Expert',
            'data_analyst': 'Data Analyst',
            
            // YouTube templates
            'comprehensive_analysis': 'Comprehensive Content Analysis',
            'engagement_optimization': 'Engagement Optimization Review',
            'audience_growth': 'Audience Growth Strategy',
            'trending_analyzer': 'Trending Content Analyzer',
            'brand_analysis': 'Brand Perception Analysis',
            'campaign_optimizer': 'Marketing Campaign Optimizer',
            'competitive_intelligence': 'Competitive Intelligence Report',
            'performance_metrics': 'Performance Metrics Dashboard',
            'trend_forecasting': 'Trend Forecasting Model',
            'roi_analysis': 'ROI Analysis Framework',
            
            // Form elements
            'youtube_url': 'YouTube Video URL or ID',
            'enter_url': 'Enter the YouTube video URL or video ID',
            'analysis_depth': 'Analysis Depth',
            'depth_basic': 'Basic',
            'depth_comprehensive': 'Comprehensive',
            'depth_expert': 'Expert',
            'custom_questions': 'Custom Thinking Questions',
            'enable_custom': 'Enable custom questions',
            'question_1': 'Question 1',
            'question_2': 'Question 2',
            'question_3': 'Question 3',
            'enter_question': 'Enter your thinking question',
            
            // Status messages
            'analysis_starting': 'Starting analysis...',
            'analysis_complete': 'Analysis complete!',
            'analysis_failed': 'Analysis failed. Please try again.',
            'invalid_url': 'Please enter a valid YouTube URL',
            'processing': 'Processing your request...',
            'please_wait': 'Please wait...',
            
            // Language interface
            'select_language': 'Select Language',
            'current_language': 'Current Language',
            'switch_language': 'Switch Language',
            'language_changed': 'Language changed successfully',
            
            // Time and dates
            'just_now': 'just now',
            'minute_ago': 'a minute ago',
            'minutes_ago': '{count} minutes ago',
            'hour_ago': 'an hour ago',
            'hours_ago': '{count} hours ago',
            'day_ago': 'a day ago',
            'days_ago': '{count} days ago',
            'week_ago': 'a week ago',
            'weeks_ago': '{count} weeks ago',
            'month_ago': 'a month ago',
            'months_ago': '{count} months ago',
            'year_ago': 'a year ago',
            'years_ago': '{count} years ago',
            
            // Validation
            'field_required': 'This field is required',
            'invalid_format': 'Invalid format',
            'url_invalid': 'Please enter a valid URL',
            'video_not_found': 'Video not found',
            'access_denied': 'Access denied',
            
            // Numbers and plurals
            'video_count': '{count} video|{count} videos',
            'analysis_count': '{count} analysis|{count} analyses',
            'result_count': '{count} result|{count} results',
        };
        
        // Chinese Simplified translations
        this.translations['zh-hans'] = {
            'loading': '加载中...',
            'error': '错误',
            'success': '成功',
            'warning': '警告',
            'info': '信息',
            'close': '关闭',
            'cancel': '取消',
            'save': '保存',
            'delete': '删除',
            'edit': '编辑',
            'submit': '提交',
            'search': '搜索',
            'filter': '筛选',
            'refresh': '刷新',
            'back': '返回',
            'next': '下一步',
            'previous': '上一步',
            'continue': '继续',
            'finish': '完成',
            
            'wizard_title': 'TubeWhale YouTube 智能分析向导',
            'wizard_subtitle': '专业的YouTube视频分析，包含深度思考问题',
            'select_tool': '选择您的分析工具',
            'choose_role': '选择您的专业角色',
            'configure_analysis': '配置分析设置',
            'start_analysis': '开始分析',
            'single_video': '单个视频分析',
            'playlist_analysis': '播放列表分析',
            'brainstorm_tool': '头脑风暴工具',
            'content_creator': '内容创作者',
            'marketing_expert': '营销专家',
            'data_analyst': '数据分析师',
            
            'youtube_url': 'YouTube视频URL或ID',
            'enter_url': '请输入YouTube视频URL或视频ID',
            'analysis_depth': '分析深度',
            'depth_basic': '基础',
            'depth_comprehensive': '全面',
            'depth_expert': '专家',
            'custom_questions': '自定义思考问题',
            'enable_custom': '启用自定义问题',
            'question_1': '问题1',
            'question_2': '问题2',
            'question_3': '问题3',
            'enter_question': '输入您的思考问题',
            
            'analysis_starting': '正在开始分析...',
            'analysis_complete': '分析完成！',
            'analysis_failed': '分析失败，请重试。',
            'invalid_url': '请输入有效的YouTube URL',
            'processing': '正在处理您的请求...',
            'please_wait': '请稍候...',
            
            'select_language': '选择语言',
            'current_language': '当前语言',
            'switch_language': '切换语言',
            'language_changed': '语言切换成功',
            
            'field_required': '此字段为必填项',
            'invalid_format': '格式无效',
            'url_invalid': '请输入有效的URL',
            'video_not_found': '未找到视频',
            'access_denied': '访问被拒绝',
        };
        
        // Japanese translations
        this.translations.ja = {
            'loading': '読み込み中...',
            'error': 'エラー',
            'success': '成功',
            'warning': '警告',
            'info': '情報',
            'wizard_title': 'TubeWhale YouTube分析ウィザード',
            'select_tool': '分析ツールを選択',
            'choose_role': 'プロフェッショナルロールを選択',
            'configure_analysis': '分析を設定',
            'start_analysis': '分析を開始',
            'content_creator': 'コンテンツクリエーター',
            'marketing_expert': 'マーケティングエキスパート',
            'data_analyst': 'データアナリスト',
        };
        
        // Korean translations
        this.translations.ko = {
            'loading': '로딩 중...',
            'error': '오류',
            'success': '성공',
            'warning': '경고',
            'info': '정보',
            'wizard_title': 'TubeWhale YouTube 분석 마법사',
            'select_tool': '분석 도구 선택',
            'choose_role': '전문 역할 선택',
            'configure_analysis': '분석 구성',
            'start_analysis': '분석 시작',
            'content_creator': '콘텐츠 제작자',
            'marketing_expert': '마케팅 전문가',
            'data_analyst': '데이터 분석가',
        };
        
        // Load additional translations from server if available
        this.loadServerTranslations();
    }
    
    /**
     * Get translated string
     */
    t(key, params = {}) {
        // Try current language first
        let translation = this.getTranslation(key, this.currentLanguage);
        
        // Fallback to English if not found
        if (!translation && this.currentLanguage !== this.fallbackLanguage) {
            translation = this.getTranslation(key, this.fallbackLanguage);
        }
        
        // Use key as fallback
        if (!translation) {
            translation = key.replace(/_/g, ' ').replace(/\b\w/g, l => l.toUpperCase());
        }
        
        // Replace parameters
        return this.replaceParams(translation, params);
    }
    
    /**
     * Get translation from language pack
     */
    getTranslation(key, language) {
        const translations = this.translations[language];
        return translations ? translations[key] : null;
    }
    
    /**
     * Replace parameters in translation string
     */
    replaceParams(str, params) {
        return str.replace(/\{(\w+)\}/g, (match, key) => {
            return params.hasOwnProperty(key) ? params[key] : match;
        });
    }
    
    /**
     * Handle pluralization
     */
    plural(key, count, params = {}) {
        const translation = this.t(key, { ...params, count });
        
        // Handle English plurals (singular|plural)
        if (this.currentLanguage === 'en' && translation.includes('|')) {
            const [singular, plural] = translation.split('|');
            return count === 1 ? singular : plural;
        }
        
        // For other languages, use appropriate plural rules
        return this.applyPluralRules(translation, count, this.currentLanguage);
    }
    
    /**
     * Apply language-specific plural rules
     */
    applyPluralRules(translation, count, language) {
        // Simple implementation - can be extended for complex plural rules
        if (translation.includes('|')) {
            const forms = translation.split('|');
            
            switch (language) {
                case 'zh-hans':
                case 'zh-hant':
                case 'ja':
                case 'ko':
                    // These languages don't have plural forms
                    return forms[0];
                    
                case 'ru':
                    // Russian has complex plural rules
                    if (count % 10 === 1 && count % 100 !== 11) {
                        return forms[0]; // 1, 21, 31, etc.
                    } else if ([2, 3, 4].includes(count % 10) && ![12, 13, 14].includes(count % 100)) {
                        return forms[1] || forms[0]; // 2-4, 22-24, etc.
                    } else {
                        return forms[2] || forms[1] || forms[0]; // 0, 5-20, 25-30, etc.
                    }
                    
                default:
                    // Default English-like behavior
                    return count === 1 ? forms[0] : (forms[1] || forms[0]);
            }
        }
        
        return translation;
    }
    
    /**
     * Change current language
     */
    setLanguage(languageCode) {
        if (this.availableLanguages && !this.availableLanguages.includes(languageCode)) {
            console.warn(`Language ${languageCode} not available`);
            return false;
        }
        
        this.currentLanguage = languageCode;
        this.isRTL = this.rtlLanguages.includes(languageCode);
        
        // Update document language
        document.documentElement.lang = languageCode;
        document.documentElement.dir = this.isRTL ? 'rtl' : 'ltr';
        
        // Update body classes
        document.body.classList.toggle('rtl', this.isRTL);
        document.body.classList.toggle('ltr', !this.isRTL);
        
        // Trigger language change event
        this.triggerLanguageChange(languageCode);
        
        // Update dynamic content
        this.updateDynamicContent();
        
        return true;
    }
    
    /**
     * Setup language detection from URL parameters
     */
    setupLanguageDetection() {
        const urlParams = new URLSearchParams(window.location.search);
        const langParam = urlParams.get('lang');
        
        if (langParam && langParam !== this.currentLanguage) {
            this.setLanguage(langParam);
        }
    }
    
    /**
     * Load translations from server
     */
    async loadServerTranslations() {
        try {
            const response = await fetch(`/api/i18n/translations/${this.currentLanguage}/`);
            if (response.ok) {
                const serverTranslations = await response.json();
                this.translations[this.currentLanguage] = {
                    ...this.translations[this.currentLanguage],
                    ...serverTranslations
                };
            }
        } catch (error) {
            console.warn('Could not load server translations:', error);
        }
    }
    
    /**
     * Update dynamic content with new translations
     */
    updateDynamicContent() {
        // Update elements with data-i18n attribute
        document.querySelectorAll('[data-i18n]').forEach(element => {
            const key = element.getAttribute('data-i18n');
            element.textContent = this.t(key);
        });
        
        // Update placeholders
        document.querySelectorAll('[data-i18n-placeholder]').forEach(element => {
            const key = element.getAttribute('data-i18n-placeholder');
            element.placeholder = this.t(key);
        });
        
        // Update titles
        document.querySelectorAll('[data-i18n-title]').forEach(element => {
            const key = element.getAttribute('data-i18n-title');
            element.title = this.t(key);
        });
    }
    
    /**
     * Trigger language change event
     */
    triggerLanguageChange(language) {
        const event = new CustomEvent('languageChanged', {
            detail: { 
                language, 
                isRTL: this.isRTL,
                translations: this.translations[language] 
            }
        });
        document.dispatchEvent(event);
    }
    
    /**
     * Format date according to current language
     */
    formatDate(date, options = {}) {
        const locale = this.getDateLocale();
        return new Intl.DateTimeFormat(locale, options).format(date);
    }
    
    /**
     * Format number according to current language
     */
    formatNumber(number, options = {}) {
        const locale = this.getDateLocale();
        return new Intl.NumberFormat(locale, options).format(number);
    }
    
    /**
     * Get locale for date/number formatting
     */
    getDateLocale() {
        const localeMap = {
            'en': 'en-US',
            'zh-hans': 'zh-CN',
            'zh-hant': 'zh-TW',
            'ja': 'ja-JP',
            'ko': 'ko-KR',
            'es': 'es-ES',
            'fr': 'fr-FR',
            'de': 'de-DE',
            'pt': 'pt-PT',
            'ru': 'ru-RU',
            'ar': 'ar-SA',
            'hi': 'hi-IN',
        };
        
        return localeMap[this.currentLanguage] || 'en-US';
    }
    
    /**
     * Get relative time string
     */
    relativeTime(date) {
        const now = new Date();
        const diff = now - date;
        const seconds = Math.floor(diff / 1000);
        const minutes = Math.floor(seconds / 60);
        const hours = Math.floor(minutes / 60);
        const days = Math.floor(hours / 24);
        const weeks = Math.floor(days / 7);
        const months = Math.floor(days / 30);
        const years = Math.floor(days / 365);
        
        if (seconds < 60) return this.t('just_now');
        if (minutes < 2) return this.t('minute_ago');
        if (minutes < 60) return this.t('minutes_ago', { count: minutes });
        if (hours < 2) return this.t('hour_ago');
        if (hours < 24) return this.t('hours_ago', { count: hours });
        if (days < 2) return this.t('day_ago');
        if (days < 7) return this.t('days_ago', { count: days });
        if (weeks < 2) return this.t('week_ago');
        if (weeks < 4) return this.t('weeks_ago', { count: weeks });
        if (months < 2) return this.t('month_ago');
        if (months < 12) return this.t('months_ago', { count: months });
        if (years < 2) return this.t('year_ago');
        return this.t('years_ago', { count: years });
    }
}

// Initialize global i18n instance
window.i18n = new TubeWhaleI18n();

// Global translation function
window.t = (key, params) => window.i18n.t(key, params);
window.plural = (key, count, params) => window.i18n.plural(key, count, params);

// Language switching function
window.switchLanguage = function(languageCode) {
    if (window.i18n.setLanguage(languageCode)) {
        // Update URL with new language parameter
        const url = new URL(window.location);
        url.searchParams.set('lang', languageCode);
        
        // Use Django's language switching endpoint
        const form = document.createElement('form');
        form.method = 'POST';
        form.action = '/i18n/setlang/';
        
        const csrfToken = document.querySelector('[name=csrfmiddlewaretoken]');
        if (csrfToken) {
            const csrfInput = document.createElement('input');
            csrfInput.type = 'hidden';
            csrfInput.name = 'csrfmiddlewaretoken';
            csrfInput.value = csrfToken.value;
            form.appendChild(csrfInput);
        }
        
        const langInput = document.createElement('input');
        langInput.type = 'hidden';
        langInput.name = 'language';
        langInput.value = languageCode;
        form.appendChild(langInput);
        
        const nextInput = document.createElement('input');
        nextInput.type = 'hidden';
        nextInput.name = 'next';
        nextInput.value = url.toString();
        form.appendChild(nextInput);
        
        document.body.appendChild(form);
        form.submit();
    }
};

console.log('🌍 TubeWhale JavaScript i18n system loaded');