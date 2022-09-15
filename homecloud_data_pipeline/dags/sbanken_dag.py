from typing import List
from datetime import datetime, timedelta
import os
from authlib.integrations.requests_client.oauth2_session import OAuth2Session
import pendulum

from prefect import flow, task, get_run_logger
from prefect.context import get_run_context

from homecloud_data_pipeline.common.sbanken_api import (
    get_oauth_session,
    store_raw_data,
    fetch_accounts,
    fetch_cards,
    fetch_customer,
    fetch_transactions_for_account,
)


@task(name="Extract metadata from SBanken APIs")
def extract_metadata(
    oauth: OAuth2Session,
):
    """
    Fetch data which is not time dependent, but we store them in a
    'time-dependent' way to be able to see changes etc.
    """
    context = get_run_context()
    logger = get_run_logger(context)

    raw_datafolder = "data/0-raw/sbanken_etl"

    execution_datetime = context.task_run.start_time
    execution_date = execution_datetime.date()

    accounts = fetch_accounts(oauth, execution_date)
    accounts_path = store_raw_data(
        accounts, f"accounts-{execution_date.to_date_string()}", raw_datafolder
    )
    cards = fetch_cards(oauth, execution_date)
    cards_path = store_raw_data(
        cards, f"cards-{execution_date.to_date_string()}", raw_datafolder
    )
    customer = fetch_customer(oauth, execution_date)
    customer_path = store_raw_data(
        customer, f"customer-{execution_date.to_date_string()}", raw_datafolder
    )
    return {
        "account_ids": [account["accountId"] for account in accounts["items"]],
        "accounts_path": accounts_path,
        "cards_path": cards_path,
        "customer_path": customer_path,
    }


@task()
def extract_transactions_for_day(
    oauth: OAuth2Session, account_ids: List[str], query_date: pendulum.Date
):
    """Fetch archived transactions for the given accounts"""
    context = get_run_context()
    logger = get_run_logger(context)

    raw_datafolder = "data/0-raw/sbanken_etl"

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


@flow(name="Run SBanken ETL Pipeline")
def sbanken_etl():
    context = get_run_context()

    client_id = os.getenv("CLIENT_ID", "")
    client_secret = os.getenv("CLIENT_SECRET", "")
    oauth = get_oauth_session(client_id, client_secret)

    generating_date = context.flow_run.expected_start_time
    query_date = generating_date.subtract(days=1).date()

    metadata = extract_metadata(oauth)
    extract_transactions_for_day(oauth, metadata["account_ids"], query_date)


if __name__ == "__main__":
    sbanken_etl()
