// ========== АВТОРИЗАЦИЯ ЧЕРЕЗ TELEGRAM ==========
// Общий скрипт для всех страниц

(function() {
    'use strict';

    // Создаём CSS для экрана авторизации
    const authStyles = document.createElement('style');
    authStyles.textContent = `
        .tg-auth-overlay {
            position: fixed;
            top: 0; left: 0; width: 100%; height: 100%;
            background: #000;
            display: flex;
            flex-direction: column;
            justify-content: center;
            align-items: center;
            z-index: 99999;
            text-align: center;
            padding: 30px;
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            color: #fff;
        }
        .tg-auth-overlay * { box-sizing: border-box; margin: 0; padding: 0; }
        .tg-auth-wrap { max-width: 400px; width: 100%; }
        .tg-auth-icon { font-size: 80px; margin-bottom: 30px; }
        .tg-auth-title {
            font-size: 28px; font-weight: 700; margin-bottom: 15px;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            -webkit-background-clip: text; -webkit-text-fill-color: transparent;
            background-clip: text;
        }
        .tg-auth-text {
            font-size: 16px; color: rgba(255,255,255,0.7);
            margin-bottom: 20px; line-height: 1.5;
        }
        .tg-auth-steps { text-align: left; margin-bottom: 25px; }
        .tg-auth-step {
            display: flex; align-items: flex-start; gap: 10px;
            margin-bottom: 12px; font-size: 14px; color: rgba(255,255,255,0.7);
        }
        .tg-auth-step b { color: #fff; }
        .tg-auth-step-num {
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white; width: 24px; height: 24px; border-radius: 50%;
            display: flex; align-items: center; justify-content: center;
            font-size: 12px; font-weight: 700; flex-shrink: 0;
        }
        .tg-auth-code {
            background: rgba(255,255,255,0.1);
            border: 2px solid #667eea; border-radius: 12px;
            padding: 15px 20px; font-family: 'Courier New', monospace;
            font-size: 18px; font-weight: 700; letter-spacing: 3px;
            color: #64ffda; word-break: break-all; margin-bottom: 15px;
            user-select: all; cursor: pointer; transition: all 0.3s ease;
        }
        .tg-auth-code:hover {
            background: rgba(255,255,255,0.15);
            border-color: #64ffda;
        }
        .tg-auth-btn {
            display: inline-flex; align-items: center; gap: 12px;
            background: linear-gradient(135deg, #0088cc 0%, #0066aa 100%);
            color: white; border: none; border-radius: 15px;
            padding: 16px 32px; font-size: 18px; font-weight: 700;
            cursor: pointer; transition: all 0.3s ease;
            box-shadow: 0 8px 25px rgba(0,136,204,0.4);
            text-decoration: none;
        }
        .tg-auth-btn:hover {
            transform: translateY(-3px);
            box-shadow: 0 12px 35px rgba(0,136,204,0.5);
        }
        .tg-auth-btn svg { width: 24px; height: 24px; fill: white; }
        .tg-auth-status {
            font-size: 14px; color: rgba(255,255,255,0.5);
            margin-top: 15px; display: flex;
            align-items: center; justify-content: center; gap: 8px;
        }
        .tg-auth-spinner {
            width: 16px; height: 16px;
            border: 2px solid rgba(255,255,255,0.2);
            border-top-color: #64ffda; border-radius: 50%;
            animation: tgAuthSpin 1s linear infinite;
        }
        @keyframes tgAuthSpin { to { transform: rotate(360deg); } }
    `;
    document.head.appendChild(authStyles);

    // Проверяем, является ли это Telegram WebApp
    function isTelegramWebApp() {
        return typeof Telegram !== 'undefined' && Telegram.WebApp && 
               Telegram.WebApp.initDataUnsafe && Telegram.WebApp.initDataUnsafe.user;
    }

    // Проверяем сохранённую сессию
    function getSavedSession() {
        try {
            const saved = localStorage.getItem('rasswet_user');
            if (saved) {
                const user = JSON.parse(saved);
                if (user && user.id) return user;
            }
        } catch (e) {}
        return null;
    }

    // Создаём оверлей авторизации
    function createAuthOverlay() {
        const overlay = document.createElement('div');
        overlay.id = 'tg-auth-overlay';
        overlay.className = 'tg-auth-overlay';
        overlay.innerHTML = `
            <div class="tg-auth-wrap">
                <div class="tg-auth-icon">🔐</div>
                <div class="tg-auth-title">Войдите через Telegram</div>
                <div class="tg-auth-text">Откройте бота и введите команду:</div>
                <div class="tg-auth-steps">
                    <div class="tg-auth-step">
                        <div class="tg-auth-step-num">1</div>
                        <span>Откройте бота <b>@rasswetgifts_bot</b></span>
                    </div>
                    <div class="tg-auth-step">
                        <div class="tg-auth-step-num">2</div>
                        <span>Введите команду:</span>
                    </div>
                </div>
                <div class="tg-auth-code" id="tg-auth-code" title="Нажмите чтобы скопировать">
                    /auth ЗАГРУЗКА...
                </div>
                <a href="https://t.me/rasswetgifts_bot" target="_blank" class="tg-auth-btn">
                    <svg viewBox="0 0 24 24"><path d="M11.944 0A12 12 0 0 0 0 12a12 12 0 0 0 12 12 12 12 0 0 0 12-12A12 12 0 0 0 12 0a12 12 0 0 0-.056 0zm4.962 7.224c.1-.002.321.023.465.14a.506.506 0 0 1 .171.325c.016.093.036.306.02.472-.18 1.898-.962 6.502-1.36 8.627-.168.9-.499 1.201-.82 1.23-.696.065-1.225-.46-1.9-.902-1.056-.693-1.653-1.124-2.678-1.8-1.185-.78-.417-1.21.258-1.91.177-.184 3.247-2.977 3.307-3.23.007-.032.014-.15-.056-.212s-.174-.041-.249-.024c-.106.024-1.793 1.14-5.061 3.345-.48.33-.913.49-1.302.48-.428-.008-1.252-.241-1.865-.44-.752-.245-1.349-.374-1.297-.789.027-.216.325-.437.893-.663 3.498-1.524 5.83-2.529 6.998-3.014 3.332-1.386 4.025-1.627 4.476-1.635z"/></svg>
                    Открыть бота
                </a>
                <div class="tg-auth-status" id="tg-auth-status">
                    <div class="tg-auth-spinner"></div>
                    Ожидание авторизации...
                </div>
            </div>
        `;
        document.body.appendChild(overlay);

        // Клик на код — копирование
        document.getElementById('tg-auth-code').addEventListener('click', function() {
            const text = this.textContent;
            navigator.clipboard.writeText(text).then(() => {
                const el = this;
                el.textContent = '✅ Скопировано!';
                setTimeout(() => el.textContent = text, 1500);
            }).catch(() => {});
        });

        return overlay;
    }

    // Генерируем код и начинаем polling
    async function startBrowserAuth(onSuccess) {
        const overlay = createAuthOverlay();

        try {
            const resp = await fetch('/api/generate-auth-code', { method: 'POST' });
            const data = await resp.json();
            if (data.success) {
                document.getElementById('tg-auth-code').textContent = '/auth ' + data.code;
                // Polling
                const interval = setInterval(async () => {
                    try {
                        const checkResp = await fetch('/api/check-auth-code', {
                            method: 'POST',
                            headers: {'Content-Type': 'application/json'},
                            body: JSON.stringify({ code: data.code })
                        });
                        const checkData = await checkResp.json();
                        if (checkData.success && checkData.confirmed && checkData.user_data) {
                            clearInterval(interval);
                            localStorage.setItem('rasswet_user', JSON.stringify(checkData.user_data));
                            overlay.remove();
                            onSuccess(checkData.user_data);
                        }
                        if (!checkData.success && (checkData.error === 'Код не найден' || checkData.error === 'Код истёк')) {
                            clearInterval(interval);
                            overlay.remove();
                            startBrowserAuth(onSuccess); // перегенерируем
                        }
                    } catch (e) {}
                }, 2000);
            }
        } catch (e) {
            document.getElementById('tg-auth-code').textContent = 'Ошибка. Перезагрузите страницу.';
        }
    }

    // Главная функция — вызывается из каждой страницы
    // Возвращает Promise с user_data или null
    window.telegramAuth = function() {
        return new Promise((resolve) => {
            // 1. Telegram MiniApp
            if (isTelegramWebApp()) {
                const tg = Telegram.WebApp;
                tg.ready();
                tg.expand();
                const tgUser = tg.initDataUnsafe.user;
                if (tgUser) {
                    const userData = {
                        id: tgUser.id,
                        first_name: tgUser.first_name,
                        last_name: tgUser.last_name || '',
                        username: tgUser.username || '',
                        photo_url: tgUser.photo_url || '',
                        balance_stars: 0,
                        balance_tickets: 0
                    };
                    resolve(userData);
                    return;
                }
            }

            // 2. Сохранённая сессия
            const saved = getSavedSession();
            if (saved) {
                resolve(saved);
                return;
            }

            // 3. Авторизация через бота
            startBrowserAuth(function(userData) {
                resolve(userData);
            });
        });
    };
})();
