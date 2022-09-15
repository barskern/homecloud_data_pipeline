from typing import List
import logging
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.models import Variable

# from homecloud_data_pipeline.common.workday import AfterWorkdayTimetable
# from airflow.example_dags.plugins.workday import AfterWorkdayTimetable

logger = logging.getLogger(__name__)


@dag(
    start_date=datetime(2020, 1, 1),
    schedule_interval="@daily",
    # timetable=AfterWorkdayTimetable(),
)
def sbanken_etl():
    @task(multiple_outputs=True)
    def extract_metadata():
        """
        Fetch data which is not time dependent, but we store them in a
        'time-dependent' way to be able to see changes etc.
        """
        from homecloud_data_pipeline.common.sbanken_api import (
            store_raw_data,
            get_oauth_session,
            fetch_accounts,
            fetch_cards,
            fetch_customer,
        )
        raw_datafolder = "data/0-raw/sbanken_etl"

        client_id = Variable.get("SBANKEN_CLIENT_ID")
        client_secret = Variable.get("SBANKEN_CLIENT_SECRET")
        oauth = get_oauth_session(client_id, client_secret)

        context = get_current_context()
        logical_date = context["logical_date"]

        accounts = fetch_accounts(oauth, logical_date)
        accounts_path = store_raw_data(
            accounts, f"accounts-{logical_date.to_date_string()}", raw_datafolder
        )
        cards = fetch_cards(oauth, logical_date)
        cards_path = store_raw_data(
            cards, f"cards-{logical_date.to_date_string()}", raw_datafolder
        )
        customer = fetch_customer(oauth, logical_date)
        customer_path = store_raw_data(
            customer, f"customer-{logical_date.to_date_string()}", raw_datafolder
        )
        return {
            "account_ids": [account["accountId"] for account in accounts["items"]],
            "accounts_path": accounts_path,
            "cards_path": cards_path,
            "customer_path": customer_path,
        }

    @task()
    def extract_transactions(account_ids: List[str]):
        """Fetch archived transactions for the given accounts"""
        from homecloud_data_pipeline.common.sbanken_api import (
            store_raw_data,
            get_oauth_session,
            fetch_transactions_for_account,
        )
        raw_datafolder = "data/0-raw/sbanken_etl"

        context = get_current_context()
        start_date = context["data_interval_start"]
        end_date = context["data_interval_end"]
        if start_date + timedelta(days=1) != end_date:
            raise RuntimeError("DAG only configured for daily workloads!")

        query_date = start_date

        client_id = Variable.get("SBANKEN_CLIENT_ID")
        client_secret = Variable.get("SBANKEN_CLIENT_SECRET")
        oauth = get_oauth_session(client_id, client_secret)

        for account_id in account_ids:
            transactions = fetch_transactions_for_account(
                oauth,
                account_id,
                query_date,
                query_date,
            )
            store_raw_data(
                transactions,
                account_id,
                raw_datafolder,
                subfolders=[
                    "transactions",
                    f"{query_date.year:04d}",
                    f"{query_date.month:02d}",
                    f"{query_date.day:02d}",
                ],
            )

    @task()
    def transform():
        pass

    @task()
    def load():
        pass

    metadata = extract_metadata()
    extract_transactions(metadata['account_ids'])


sbanken_dag = sbanken_etl()
