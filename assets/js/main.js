// PySpark & Data Engineering Learning Hub - Main JavaScript

// Theme Management
const ThemeManager = {
    init() {
        const savedTheme = localStorage.getItem('theme') || 'light';
        this.setTheme(savedTheme);
        this.bindEvents();
    },

    setTheme(theme) {
        document.documentElement.setAttribute('data-theme', theme);
        localStorage.setItem('theme', theme);
        this.updateToggleIcon(theme);
    },

    toggle() {
        const currentTheme = document.documentElement.getAttribute('data-theme');
        const newTheme = currentTheme === 'dark' ? 'light' : 'dark';
        this.setTheme(newTheme);
    },

    updateToggleIcon(theme) {
        const toggleBtn = document.querySelector('.theme-toggle');
        if (toggleBtn) {
            toggleBtn.innerHTML = theme === 'dark' ? '☀️' : '🌙';
        }
    },

    bindEvents() {
        const toggleBtn = document.querySelector('.theme-toggle');
        if (toggleBtn) {
            toggleBtn.addEventListener('click', () => this.toggle());
        }
    }
};

// Search Functionality
const SearchManager = {
    init() {
        this.searchInput = document.querySelector('.search-input');
        this.cards = document.querySelectorAll('.card');
        if (this.searchInput) {
            this.bindEvents();
        }
    },

    bindEvents() {
        this.searchInput.addEventListener('input', (e) => this.handleSearch(e.target.value));
    },

    handleSearch(query) {
        const normalizedQuery = query.toLowerCase().trim();
        
        this.cards.forEach(card => {
            const title = card.querySelector('.card-title')?.textContent.toLowerCase() || '';
            const description = card.querySelector('.card-description')?.textContent.toLowerCase() || '';
            const isMatch = title.includes(normalizedQuery) || description.includes(normalizedQuery);
            
            card.style.display = isMatch ? 'block' : 'none';
            
            if (isMatch && normalizedQuery) {
                card.classList.add('animate-fade-in');
            } else {
                card.classList.remove('animate-fade-in');
            }
        });

        // Show/hide category sections based on visible cards
        document.querySelectorAll('.category-section').forEach(section => {
            const visibleCards = section.querySelectorAll('.card[style="display: block"], .card:not([style*="display"])');
            const hasVisibleCards = Array.from(visibleCards).some(card => card.style.display !== 'none');
            section.style.display = hasVisibleCards ? 'block' : 'none';
        });
    }
};

// Tab Management
const TabManager = {
    init() {
        this.bindEvents();
    },

    bindEvents() {
        document.querySelectorAll('.tab').forEach(tab => {
            tab.addEventListener('click', (e) => this.handleTabClick(e));
        });
    },

    handleTabClick(e) {
        const tab = e.target;
        const tabGroup = tab.closest('.tabs');
        const contentContainer = tabGroup.nextElementSibling;
        const targetId = tab.dataset.tab;

        // Update active tab
        tabGroup.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
        tab.classList.add('active');

        // Update active content
        if (contentContainer) {
            contentContainer.querySelectorAll('.tab-content').forEach(content => {
                content.classList.remove('active');
                if (content.id === targetId) {
                    content.classList.add('active');
                }
            });
        }
    }
};

// Code Copy Functionality
const CodeCopyManager = {
    init() {
        this.bindEvents();
    },

    bindEvents() {
        document.querySelectorAll('.copy-btn').forEach(btn => {
            btn.addEventListener('click', (e) => this.handleCopy(e));
        });
    },

    async handleCopy(e) {
        const btn = e.target;
        const codeContainer = btn.closest('.code-container');
        const codeBlock = codeContainer.querySelector('.code-block pre, .code-block code');
        
        if (codeBlock) {
            try {
                await navigator.clipboard.writeText(codeBlock.textContent);
                const originalText = btn.textContent;
                btn.textContent = 'Copied!';
                btn.style.background = 'var(--accent-success)';
                
                setTimeout(() => {
                    btn.textContent = originalText;
                    btn.style.background = '';
                }, 2000);
            } catch (err) {
                console.error('Failed to copy:', err);
            }
        }
    }
};

// Smooth Scroll
const SmoothScrollManager = {
    init() {
        document.querySelectorAll('a[href^="#"]').forEach(anchor => {
            anchor.addEventListener('click', (e) => {
                e.preventDefault();
                const target = document.querySelector(anchor.getAttribute('href'));
                if (target) {
                    target.scrollIntoView({ behavior: 'smooth', block: 'start' });
                }
            });
        });
    }
};

// Intersection Observer for Animations
const AnimationObserver = {
    init() {
        const observer = new IntersectionObserver((entries) => {
            entries.forEach(entry => {
                if (entry.isIntersecting) {
                    entry.target.classList.add('animate-fade-in');
                }
            });
        }, { threshold: 0.1 });

        document.querySelectorAll('.card, .category-section').forEach(el => {
            observer.observe(el);
        });
    }
};

// Navigation Active State
const NavigationManager = {
    init() {
        this.updateActiveNav();
        window.addEventListener('scroll', () => this.updateActiveNav());
    },

    updateActiveNav() {
        const sections = document.querySelectorAll('.category-section');
        const navLinks = document.querySelectorAll('.nav-link');
        
        let currentSection = '';
        
        sections.forEach(section => {
            const sectionTop = section.offsetTop - 100;
            if (window.scrollY >= sectionTop) {
                currentSection = section.id;
            }
        });

        navLinks.forEach(link => {
            link.classList.remove('active');
            if (link.getAttribute('href') === `#${currentSection}`) {
                link.classList.add('active');
            }
        });
    }
};

// Initialize all managers on DOM load
document.addEventListener('DOMContentLoaded', () => {
    ThemeManager.init();
    SearchManager.init();
    TabManager.init();
    CodeCopyManager.init();
    SmoothScrollManager.init();
    AnimationObserver.init();
    NavigationManager.init();
});

// Export for use in other modules
window.LearningHub = {
    ThemeManager,
    SearchManager,
    TabManager,
    CodeCopyManager
};
