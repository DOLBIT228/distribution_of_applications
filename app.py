from typing import Dict, List, Optional

import requests
import streamlit as st


st.set_page_config(page_title="Розподіл заявок", page_icon="📥", layout="wide")


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
            "select": ["ID", "TITLE", "ASSIGNED_BY_ID"],
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


def assign_deal(deal_id: int, manager_id: int) -> None:
    payload = {
        "id": int(deal_id),
        "fields": {
            "ASSIGNED_BY_ID": int(manager_id),
        },
    }
    bitrix_request("crm.deal.update", payload)


def get_direction_config() -> Dict[str, Dict]:
    directions = _secret_required("directions")
    return {item["name"]: item for item in directions}


def get_managers_config() -> Dict[str, int]:
    managers = _secret_required("managers")
    return {str(item["name"]): int(item["id"]) for item in managers}


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

    with st.spinner("Отримуємо кількість заявок..."):
        deals_all = fetch_deals(category_id, stage_id, limit=None)
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

    if st.button("Розподілити заявки", type="primary", disabled=available_count == 0):
        if not selected_managers:
            st.warning("Оберіть хоча б одного менеджера.")
            return

        distribution_size = min(int(amount), available_count)
        target_deals = deals_all[:distribution_size]

        manager_ids = [manager_options[name] for name in selected_managers]
        results = []

        with st.spinner("Розподіляємо заявки..."):
            for index, deal in enumerate(target_deals):
                manager_index = index % len(manager_ids)
                manager_id = manager_ids[manager_index]
                manager_name = selected_managers[manager_index]
                assign_deal(int(deal["ID"]), manager_id)
                results.append(
                    {
                        "deal_id": int(deal["ID"]),
                        "deal_title": deal.get("TITLE", ""),
                        "manager": manager_name,
                    }
                )

        st.success(f"Успішно розподілено {len(results)} заявок.")
        st.dataframe(results, use_container_width=True)


try:
    if st.session_state.get("authenticated"):
        distribution_screen()
    else:
        login_screen()
except Exception as exc:
    st.error(f"Критична помилка: {exc}")
    st.stop()
