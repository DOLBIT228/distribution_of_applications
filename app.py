from collections import defaultdict
from datetime import date, datetime
import sqlite3
import time
from typing import Dict, List, Optional

import requests
import streamlit as st


st.set_page_config(page_title="Розподіл заявок", page_icon="📥", layout="wide")

DB_PATH = "distribution_history.db"
DASHBOARD_URL = "https://panel-for-manager-call.streamlit.app/"
DEFAULT_BATCH_SIZE = 3

LANDING_SOURCE_NAMES = {
    "лендинг 1 грам",
    "лендинг -2=1",
    "лендинг 2 за 1 оффер",
    "лендинг каблучки 100$",
    "лендинг каблучки 1 грам",
    "лендинг - стара ціна 2025",
    "лендинг раннє бронювання",
}

SITE_DEAL_TYPES = ["Сайт", "Лендинг"]
INSTAGRAM_DEAL_TYPES = ["Інстаграм"]


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


def fetch_deal_count_for_manager(category_id: int, stage_id: str, manager_id: int) -> int:
    payload = {
        "filter": {
            "CATEGORY_ID": category_id,
            "STAGE_ID": stage_id,
            "ASSIGNED_BY_ID": int(manager_id),
        },
    }
    data = bitrix_request("crm.deal.list", payload)
    total = data.get("total")
    if total is not None:
        return int(total)
    return len(data.get("result", []))


def fetch_source_map() -> Dict[str, str]:
    payload = {"filter": {"ENTITY_ID": "SOURCE"}}
    data = bitrix_request("crm.status.list", payload)
    return {str(item.get("STATUS_ID", "")): str(item.get("NAME", "")) for item in data.get("result", [])}


def get_direction_logic(direction_name: str, direction: Dict) -> str:
    explicit_logic = str(direction.get("distribution_logic") or "").strip().lower()
    if explicit_logic in {"site", "instagram"}:
        return explicit_logic

    if "інст" in direction_name.lower():
        return "instagram"

    return "site"


def classify_deal_type(deal: Dict, source_map: Dict[str, str], logic: str) -> str:
    if logic == "instagram":
        return "Інстаграм"

    source_id = str(deal.get("SOURCE_ID") or "")
    source_name = source_map.get(source_id, source_id).strip().lower()

    if source_name in LANDING_SOURCE_NAMES:
        return "Лендинг"

    if source_name == "лендинг":
        return "Сайт"

    return "Лендинг"


def get_deal_types_for_logic(logic: str) -> List[str]:
    if logic == "instagram":
        return INSTAGRAM_DEAL_TYPES
    return SITE_DEAL_TYPES


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
        summary: Dict[str, Dict[str, int]] = defaultdict(dict)
        for manager_name, deal_type, count in cursor.fetchall():
            summary[str(manager_name)][str(deal_type)] = int(count)
        return summary
    finally:
        conn.close()


def get_daily_manager_state(
    direction_name: str,
    selected_managers: List[str],
    deal_types: List[str],
) -> Dict[str, Dict[str, Optional[str] | int]]:
    distribution_date = date.today().isoformat()
    conn = sqlite3.connect(DB_PATH)
    try:
        state: Dict[str, Dict[str, Optional[str] | int]] = {
            manager_name: {deal_type: 0 for deal_type in deal_types} for manager_name in selected_managers
        }

        for manager_name in selected_managers:
            state[manager_name].update({"total": 0, "last_type": None})

        cursor = conn.execute(
            """
            SELECT manager_name,
                   deal_type,
                   COUNT(*) AS cnt,
                   MAX(id) AS last_row_id
            FROM distribution_history
            WHERE distribution_date = ? AND direction_name = ?
            GROUP BY manager_name, deal_type
            """,
            (distribution_date, direction_name),
        )
        rows = cursor.fetchall()

        last_row_by_manager: Dict[str, int] = {}
        for manager_name, deal_type, count, last_row_id in rows:
            manager_name = str(manager_name)
            deal_type = str(deal_type)
            if manager_name not in state:
                continue

            if deal_type not in state[manager_name]:
                state[manager_name][deal_type] = 0
            state[manager_name][deal_type] = int(count)
            state[manager_name]["total"] = int(state[manager_name]["total"]) + int(count)

            if last_row_id is not None:
                prev_last = last_row_by_manager.get(manager_name)
                if prev_last is None or int(last_row_id) > prev_last:
                    last_row_by_manager[manager_name] = int(last_row_id)

        for manager_name, last_row_id in last_row_by_manager.items():
            deal_type_cursor = conn.execute(
                "SELECT deal_type FROM distribution_history WHERE id = ?",
                (int(last_row_id),),
            )
            deal_type_row = deal_type_cursor.fetchone()
            if deal_type_row:
                state[manager_name]["last_type"] = str(deal_type_row[0])

        return state
    finally:
        conn.close()


def select_manager_for_deal(
    deal_type: str,
    selected_managers: List[str],
    manager_state: Dict[str, Dict[str, Optional[str] | int]],
    logic: str,
    batch_load: Dict[str, int],
    batch_size: int,
) -> str:
    under_limit = [manager for manager in selected_managers if int(batch_load[manager]) < batch_size]
    if not under_limit:
        raise RuntimeError("Немає доступних менеджерів у межах поточної пачки.")

    minimum_batch_total = min(int(batch_load[manager]) for manager in under_limit)
    candidates = [manager for manager in under_limit if int(batch_load[manager]) == minimum_batch_total]

    if logic == "instagram":
        return candidates[0]

    preferred_candidates = [
        manager for manager in candidates if manager_state[manager].get("last_type") != deal_type
    ]
    tie_pool = preferred_candidates or candidates

    minimum_type_count = min(int(manager_state[manager][deal_type]) for manager in tie_pool)
    final_candidates = [manager for manager in tie_pool if int(manager_state[manager][deal_type]) == minimum_type_count]
    return final_candidates[0]


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


def build_summary_table(direction_name: str, selected_managers: List[str], deal_types: List[str]) -> List[Dict]:
    summary = get_daily_summary(direction_name)
    table: List[Dict] = []

    managers_to_show = selected_managers or sorted(summary.keys())
    for manager in managers_to_show:
        row = {"Менеджер": manager}
        for deal_type in deal_types:
            row[deal_type] = summary.get(manager, {}).get(deal_type, 0)
        table.append(row)

    return table


def run_distribution_once(
    *,
    category_id: int,
    direction_name: str,
    target_stage_id: str,
    in_progress_stage_id: str,
    distribution_logic: str,
    deal_types: List[str],
    batch_size: int,
    selected_managers: List[str],
    manager_options: Dict[str, int],
    deals_all: List[Dict],
    source_map: Dict[str, str],
) -> Dict:
    if not selected_managers:
        return {"status": "warning", "message": "Оберіть хоча б одного менеджера."}

    if not deals_all:
        return {"status": "info", "message": "Немає заявок для розподілу у вибраному статусі."}

    manager_ids = {name: manager_options[name] for name in selected_managers}
    in_progress_counts = {
        manager_name: fetch_deal_count_for_manager(category_id, in_progress_stage_id, manager_ids[manager_name])
        for manager_name in selected_managers
    }

    available_managers = [
        manager_name for manager_name in selected_managers if in_progress_counts[manager_name] == 0
    ]

    if not available_managers:
        return {
            "status": "warning",
            "message": "Немає вільних менеджерів: у всіх є активні угоди в статусі 'Угода в роботі'.",
            "in_progress_counts": in_progress_counts,
            "results": [],
        }

    max_for_batch = len(available_managers) * batch_size
    distribution_size = min(len(deals_all), max_for_batch)
    target_deals = deals_all[:distribution_size]

    manager_state = get_daily_manager_state(direction_name, available_managers, deal_types)
    batch_load = {manager_name: 0 for manager_name in available_managers}
    results = []

    for deal in target_deals:
        deal_type = classify_deal_type(deal, source_map, distribution_logic)
        manager_name = select_manager_for_deal(
            deal_type,
            available_managers,
            manager_state,
            distribution_logic,
            batch_load,
            batch_size,
        )
        manager_id = manager_ids[manager_name]

        if deal_type not in manager_state[manager_name]:
            manager_state[manager_name][deal_type] = 0
        manager_state[manager_name][deal_type] = int(manager_state[manager_name][deal_type]) + 1
        manager_state[manager_name]["total"] = int(manager_state[manager_name]["total"]) + 1
        manager_state[manager_name]["last_type"] = deal_type
        batch_load[manager_name] = int(batch_load[manager_name]) + 1

        update_deal_assignment_and_stage(int(deal["ID"]), manager_id, target_stage_id)
        results.append(
            {
                "deal_id": int(deal["ID"]),
                "deal_title": deal.get("TITLE", ""),
                "deal_type": deal_type,
                "manager": manager_name,
                "next_stage": target_stage_id,
            }
        )

    store_distribution_rows(direction_name, results)

    return {
        "status": "success",
        "message": (
            f"Успішно розподілено {len(results)} заявок. "
            f"Вільних менеджерів: {len(available_managers)}. Розмір пачки: {batch_size}."
        ),
        "in_progress_counts": in_progress_counts,
        "results": results,
    }


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

    if "auto_distribution_enabled" not in st.session_state:
        st.session_state["auto_distribution_enabled"] = False
    if "auto_distribution_last_run" not in st.session_state:
        st.session_state["auto_distribution_last_run"] = None

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
    in_progress_stage_id = str(direction.get("in_progress_status_id") or next_stage_id).strip()
    target_stage_id = in_progress_stage_id or next_stage_id
    distribution_logic = get_direction_logic(direction_name, direction)
    deal_types = get_deal_types_for_logic(distribution_logic)
    batch_size = int(direction.get("batch_size") or DEFAULT_BATCH_SIZE)
    auto_interval_seconds = int(direction.get("auto_interval_seconds") or 30)

    if not target_stage_id:
        st.warning(
            "Для цього напрямку не задано `in_progress_status_id` (або запасний `next_status_id`) у secrets.toml. "
            "Розподіл заблоковано."
        )

    with st.spinner("Отримуємо заявки та джерела..."):
        deals_all = fetch_deals(category_id, stage_id, limit=None)
        source_map = fetch_source_map()

    if st.button("Оновити статус"):
        st.rerun()

    available_count = len(deals_all)
    st.info(f"Знайдено заявок у статусі: **{available_count}**")

    if distribution_logic == "instagram":
        st.caption("Логіка напрямку: Instagram (рівномірно по загальній кількості заявок)")
    else:
        st.caption("Логіка напрямку: Сайт/Лендинг (розподіл по джерелах)")

    action_col1, action_col2, action_col3 = st.columns(3)
    with action_col1:
        if st.button(
            "Розподілити заявки 1 раз",
            type="primary",
            disabled=available_count == 0 or not target_stage_id,
        ):
            with st.spinner("Розподіляємо заявки..."):
                run_result = run_distribution_once(
                    category_id=category_id,
                    direction_name=direction_name,
                    target_stage_id=target_stage_id,
                    in_progress_stage_id=in_progress_stage_id,
                    distribution_logic=distribution_logic,
                    deal_types=deal_types,
                    batch_size=batch_size,
                    selected_managers=selected_managers,
                    manager_options=manager_options,
                    deals_all=deals_all,
                    source_map=source_map,
                )
            st.session_state["auto_distribution_last_run"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
            status = run_result["status"]
            if status == "success":
                st.success(run_result["message"])
            elif status == "warning":
                st.warning(run_result["message"])
            else:
                st.info(run_result["message"])

            if run_result.get("in_progress_counts"):
                st.dataframe(
                    [
                        {"Менеджер": name, "Активних в роботі": run_result["in_progress_counts"][name]}
                        for name in selected_managers
                    ],
                    use_container_width=True,
                )
            if run_result.get("results"):
                st.dataframe(run_result["results"], use_container_width=True)

    with action_col2:
        if st.button(
            "Почати авто-розподіл",
            disabled=st.session_state["auto_distribution_enabled"] or not target_stage_id or not selected_managers,
        ):
            st.session_state["auto_distribution_enabled"] = True
            st.rerun()

    with action_col3:
        if st.button("Зупинити авто-розподіл", disabled=not st.session_state["auto_distribution_enabled"]):
            st.session_state["auto_distribution_enabled"] = False
            st.rerun()

    if st.session_state["auto_distribution_enabled"]:
        if not selected_managers:
            st.warning("Авто-режим зупинено: оберіть хоча б одного менеджера для розподілу.")
            st.session_state["auto_distribution_enabled"] = False
            st.rerun()

        st.success(
            f"Авто-режим увімкнено. Перевірка та розподіл виконуються кожні {auto_interval_seconds} сек."
        )
        with st.spinner("Авто-режим: запускаємо розподіл..."):
            run_result = run_distribution_once(
                category_id=category_id,
                direction_name=direction_name,
                target_stage_id=target_stage_id,
                in_progress_stage_id=in_progress_stage_id,
                distribution_logic=distribution_logic,
                deal_types=deal_types,
                batch_size=batch_size,
                selected_managers=selected_managers,
                manager_options=manager_options,
                deals_all=deals_all,
                source_map=source_map,
            )
        st.session_state["auto_distribution_last_run"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")

        status = run_result["status"]
        if status == "success":
            st.success(run_result["message"])
        elif status == "warning":
            st.warning(run_result["message"])
        else:
            st.info(run_result["message"])

        if run_result.get("in_progress_counts"):
            st.dataframe(
                [
                    {"Менеджер": name, "Активних в роботі": run_result["in_progress_counts"][name]}
                    for name in selected_managers
                ],
                use_container_width=True,
            )
        if run_result.get("results"):
            st.dataframe(run_result["results"], use_container_width=True)

        st.caption(
            f"Останній авто-запуск: {st.session_state.get('auto_distribution_last_run', '-')}. "
            "Сторінка перезапуститься автоматично."
        )
        time.sleep(auto_interval_seconds)
        st.rerun()

    st.subheader("Таблиця розподілу за сьогодні")
    st.dataframe(build_summary_table(direction_name, selected_managers, deal_types), use_container_width=True)

    if st.button("Очистити значення", type="secondary"):
        deleted_rows = clear_daily_distribution(direction_name)
        if deleted_rows:
            st.success(f"Очищено записів: {deleted_rows}. Історію розподілу за сьогодні скинуто.")
        else:
            st.info("Немає значень для очищення за сьогодні у цьому напрямку.")
        st.rerun()


init_db()
st.link_button("⬅ Назад до панелі менеджера", DASHBOARD_URL)
st.divider()

try:
    if st.session_state.get("authenticated"):
        distribution_screen()
    else:
        login_screen()
except Exception as exc:
    st.error(f"Критична помилка: {exc}")
    st.stop()
