from collections import defaultdict
from datetime import date
import sqlite3
from typing import Dict, List, Optional

import requests
import streamlit as st


st.set_page_config(page_title="Розподіл заявок", page_icon="📥", layout="wide")

DB_PATH = "distribution_history.db"

LANDING_SOURCE_NAMES = {
    "лендинг 1 грам",
    "лендинг -2=1",
    "лендинг 2 за 1 оффер",
    "лендинг каблучки 100$",
    "лендинг каблучки 1 грам",
    "лендинг - стара ціна 2025",
    "лендинг раннє бронювання",
}


def _secret_required(path: str):
    cursor = st.secrets
    for key in path.split("."):
        if key not in cursor:
            raise KeyError(f"Відсутній секрет: {path}")
        cursor = cursor[key]
    return cursor


def get_auth_user(login: str, password: str) -> Optional[Dict]:
    users = _secret_required("auth.users")
    for user in users:
        if str(user["login"]) == login and str(user["password"]) == password:
            return {
                "login": str(user["login"]),
                "name": str(user.get("name") or user["login"]),
                "manager_id": int(user["manager_id"]),
            }
    return None


def bitrix_request(method: str, payload: Dict) -> Dict:
    base_url = _secret_required("bitrix.webhook_url").rstrip("/")
    response = requests.post(f"{base_url}/{method}.json", json=payload, timeout=30)
    response.raise_for_status()
    data = response.json()
    if "error" in data:
        raise RuntimeError(f"Bitrix API error: {data.get('error_description') or data['error']}")
    return data


def fetch_deals(category_id: int, stage_id: str, limit: int | None = None) -> List[Dict]:
    deals: List[Dict] = []
    start = 0

    while True:
        payload = {
            "filter": {
                "CATEGORY_ID": category_id,
                "STAGE_ID": stage_id,
            },
            "order": {"ID": "ASC"},
            "select": ["ID", "TITLE", "ASSIGNED_BY_ID", "SOURCE_ID"],
            "start": start,
        }

        data = bitrix_request("crm.deal.list", payload)
        chunk = data.get("result", [])
        deals.extend(chunk)

        if limit is not None and len(deals) >= limit:
            return deals[:limit]

        next_start = data.get("next")
        if next_start is None or not chunk:
            break
        start = int(next_start)

    return deals


def fetch_source_map() -> Dict[str, str]:
    payload = {"filter": {"ENTITY_ID": "SOURCE"}}
    data = bitrix_request("crm.status.list", payload)
    return {str(item.get("STATUS_ID", "")): str(item.get("NAME", "")) for item in data.get("result", [])}


def classify_deal_type(deal: Dict, source_map: Dict[str, str]) -> str:
    source_id = str(deal.get("SOURCE_ID") or "")
    source_name = source_map.get(source_id, source_id).strip().lower()

    if source_name in LANDING_SOURCE_NAMES:
        return "Лендинг"

    if source_name == "лендинг":
        return "Сайт"

    return "Лендинг"


def update_deal_assignment_and_stage(deal_id: int, manager_id: int, next_stage_id: str) -> None:
    payload = {
        "id": int(deal_id),
        "fields": {
            "ASSIGNED_BY_ID": int(manager_id),
            "STAGE_ID": str(next_stage_id),
        },
    }
    bitrix_request("crm.deal.update", payload)


def get_direction_config() -> Dict[str, Dict]:
    directions = _secret_required("directions")
    return {item["name"]: item for item in directions}


def get_managers_config() -> Dict[str, int]:
    managers = _secret_required("managers")
    return {str(item["name"]): int(item["id"]) for item in managers}


def init_db() -> None:
    conn = sqlite3.connect(DB_PATH)
    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS distribution_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                distribution_date TEXT NOT NULL,
                direction_name TEXT NOT NULL,
                manager_name TEXT NOT NULL,
                deal_type TEXT NOT NULL,
                deal_id INTEGER NOT NULL
            )
            """
        )
        conn.commit()
    finally:
        conn.close()


def store_distribution_rows(direction_name: str, rows: List[Dict]) -> None:
    if not rows:
        return

    distribution_date = date.today().isoformat()
    conn = sqlite3.connect(DB_PATH)
    try:
        conn.executemany(
            """
            INSERT INTO distribution_history (
                distribution_date,
                direction_name,
                manager_name,
                deal_type,
                deal_id
            ) VALUES (?, ?, ?, ?, ?)
            """,
            [
                (
                    distribution_date,
                    direction_name,
                    row["manager"],
                    row["deal_type"],
                    int(row["deal_id"]),
                )
                for row in rows
            ],
        )
        conn.commit()
    finally:
        conn.close()


def get_daily_summary(direction_name: str) -> Dict[str, Dict[str, int]]:
    distribution_date = date.today().isoformat()
    conn = sqlite3.connect(DB_PATH)
    try:
        cursor = conn.execute(
            """
            SELECT manager_name, deal_type, COUNT(*)
            FROM distribution_history
            WHERE distribution_date = ? AND direction_name = ?
            GROUP BY manager_name, deal_type
            """,
            (distribution_date, direction_name),
        )
        summary: Dict[str, Dict[str, int]] = defaultdict(lambda: {"Сайт": 0, "Лендинг": 0})
        for manager_name, deal_type, count in cursor.fetchall():
            summary[str(manager_name)][str(deal_type)] = int(count)
        return summary
    finally:
        conn.close()


def clear_daily_distribution(direction_name: str) -> int:
    distribution_date = date.today().isoformat()
    conn = sqlite3.connect(DB_PATH)
    try:
        cursor = conn.execute(
            """
            DELETE FROM distribution_history
            WHERE distribution_date = ? AND direction_name = ?
            """,
            (distribution_date, direction_name),
        )
        conn.commit()
        return int(cursor.rowcount or 0)
    finally:
        conn.close()


def build_summary_table(direction_name: str, selected_managers: List[str]) -> List[Dict]:
    summary = get_daily_summary(direction_name)
    table: List[Dict] = []

    managers_to_show = selected_managers or sorted(summary.keys())
    for manager in managers_to_show:
        site_count = summary.get(manager, {}).get("Сайт", 0)
        landing_count = summary.get(manager, {}).get("Лендинг", 0)
        table.append({"Менеджер": manager, "Сайт": site_count, "Лендинг": landing_count})

    return table


def login_screen() -> None:
    st.title("Вхід в систему розподілу заявок")
    with st.form("login"):
        username = st.text_input("Логін")
        password = st.text_input("Пароль", type="password")
        submitted = st.form_submit_button("Увійти")

    if submitted:
        user = get_auth_user(username.strip(), password)
        if user:
            st.session_state["authenticated"] = True
            st.session_state["user"] = user
            st.rerun()
        else:
            st.error("Невірний логін або пароль.")


def distribution_screen() -> None:
    user = st.session_state.get("user", {})

    st.title("Розподіл заявок між менеджерами")
    st.caption(
        f"Користувач: {user.get('name', '-')} | ID менеджера акаунта: {user.get('manager_id', '-')}")

    if st.button("Вийти"):
        st.session_state.clear()
        st.rerun()

    direction_options = get_direction_config()
    manager_options = get_managers_config()

    col1, col2 = st.columns(2)
    with col1:
        direction_name = st.selectbox("Напрямок", list(direction_options.keys()))
    with col2:
        selected_managers = st.multiselect(
            "Менеджери для розподілу",
            options=list(manager_options.keys()),
            help="ID менеджерів не показуються в інтерфейсі.",
        )

    direction = direction_options[direction_name]
    category_id = int(direction["funnel_id"])
    stage_id = str(direction["status_id"])
    next_stage_id = str(direction.get("next_status_id") or "").strip()

    if not next_stage_id:
        st.warning("Для цього напрямку не задано `next_status_id` у secrets.toml. Розподіл заблоковано.")

    with st.spinner("Отримуємо кількість заявок..."):
        deals_all = fetch_deals(category_id, stage_id, limit=None)
        source_map = fetch_source_map()

    available_count = len(deals_all)
    st.info(f"Знайдено заявок у статусі: **{available_count}**")

    max_to_assign = max(available_count, 1)
    amount = st.number_input(
        "Скільки заявок розподілити",
        min_value=1,
        max_value=max_to_assign,
        value=min(available_count, 10) if available_count else 1,
        step=1,
    )

    disabled = available_count == 0 or not next_stage_id
    if st.button("Розподілити заявки", type="primary", disabled=disabled):
        if not selected_managers:
            st.warning("Оберіть хоча б одного менеджера.")
            return

        distribution_size = min(int(amount), available_count)
        target_deals = deals_all[:distribution_size]

        manager_ids = [manager_options[name] for name in selected_managers]
        manager_idx_by_type = {"Сайт": 0, "Лендинг": 0}
        results = []

        with st.spinner("Розподіляємо заявки..."):
            for deal in target_deals:
                deal_type = classify_deal_type(deal, source_map)
                manager_index = manager_idx_by_type[deal_type] % len(manager_ids)
                manager_id = manager_ids[manager_index]
                manager_name = selected_managers[manager_index]
                manager_idx_by_type[deal_type] += 1

                update_deal_assignment_and_stage(int(deal["ID"]), manager_id, next_stage_id)
                results.append(
                    {
                        "deal_id": int(deal["ID"]),
                        "deal_title": deal.get("TITLE", ""),
                        "deal_type": deal_type,
                        "manager": manager_name,
                        "next_stage": next_stage_id,
                    }
                )

        store_distribution_rows(direction_name, results)

        st.success(f"Успішно розподілено {len(results)} заявок та переведено у наступний статус.")
        st.dataframe(results, use_container_width=True)

    st.subheader("Таблиця розподілу за сьогодні")
    st.dataframe(build_summary_table(direction_name, selected_managers), use_container_width=True)

    if st.button("Очистити значення", type="secondary"):
        deleted_rows = clear_daily_distribution(direction_name)
        if deleted_rows:
            st.success(f"Очищено записів: {deleted_rows}. Історію розподілу за сьогодні скинуто.")
        else:
            st.info("Немає значень для очищення за сьогодні у цьому напрямку.")
        st.rerun()


init_db()

try:
    if st.session_state.get("authenticated"):
        distribution_screen()
    else:
        login_screen()
except Exception as exc:
    st.error(f"Критична помилка: {exc}")
    st.stop()
