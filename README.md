# Розподіл заявок Bitrix24 (Streamlit)

Це Streamlit-скрипт для рівномірного (round-robin) розподілу заявок між менеджерами у вказаному статусі конкретної воронки Bitrix24.

## Що робить

- Вхід в локальний інтерфейс (логін/пароль з `st.secrets`).
- Логін у CRM не потрібен: зв'язок з Bitrix24 працює через ваш webhook.
- Вибір напрямку (воронка + статус).
- Показ кількості доступних угод у статусі.
- Вибір менеджерів для розподілу.
- Вибір кількості угод для розподілу.
- Масове призначення відповідального (`ASSIGNED_BY_ID`) через Bitrix REST API.

## Налаштування

1. Встановіть залежності:

```bash
pip install -r requirements.txt
```

2. Створіть файл `.streamlit/secrets.toml` на базі `.streamlit/secrets.toml.example`.

3. Заповніть:
   - `bitrix.webhook_url` — webhook для API Bitrix24,
   - `[[auth.users]]` — користувачі для входу в інтерфейс (login/password/manager_id),
   - `[[directions]]` — напрямки (name + funnel_id + status_id),
   - `[[managers]]` — менеджери для розподілу (name + id).

## Запуск

```bash
streamlit run app.py
```

## Важливо

- Для `status_id` використовуйте технічне значення стадії (наприклад `NEW`, `C2:NEW`).
- Скрипт працює тільки з тими воронками/стадіями, які ви вкажете в `secrets.toml`.
