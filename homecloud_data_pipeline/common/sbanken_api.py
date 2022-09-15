from typing import Any, Dict, List, Optional
from datetime import datetime, date
import json
import logging
import os
from urllib.parse import quote

from authlib.integrations.requests_client.oauth2_session import OAuth2Session

logging.basicConfig(level="DEBUG")
logger = logging.getLogger(__name__)

API_URL = "https://publicapi.sbanken.no/apibeta"
CACHE_FOLDER = "cache"


def get_oauth_session(client_id: str, client_secret: str):
    token_filename = f"{CACHE_FOLDER}/token.json"
    token = None
    try:
        with open(token_filename) as file:
            token = json.load(file)
            logger.info(f"Found token in file '{token_filename}'")

            since_expiration = datetime.now().timestamp() - token["expires_at"]
            if since_expiration > 0:
                logger.info(f"Token expired {since_expiration:.0f} seconds ago")
                token = None
            else:
                logger.info(
                    f"Token will expire in {-since_expiration:.0f} seconds, so continue using it"
                )
    except FileNotFoundError:
        pass

    oauth = OAuth2Session(
        client_id=quote(client_id),
        client_secret=quote(client_secret),
        token=token,
    )

    if token is None:
        logger.info("Token was expired or missing, generating a new token...")

        token = oauth.fetch_token(
            url="https://auth.sbanken.no/identityserver/connect/token",
        )

        with open(token_filename, "w") as file:
            json.dump(token, file)
        logger.info(f"Dumped new token to '{token_filename}'")

    return oauth


class QueryError(Exception):
    """Unable to query SBanken API"""
    pass


def query_or_cache(
    oauth: OAuth2Session, name: str, url: str, params: Optional[Dict[str, str]] = None
):
    filepath = f"{CACHE_FOLDER}/{name}.json"
    try:
        with open(filepath) as file:
            data = json.load(file)
        logger.debug(f"Fetched data for '{name}' from cache at '{filepath}'")
    except FileNotFoundError:
        result = oauth.get(url, params=params)

        if result.status_code == 200:
            data = result.json()

            os.makedirs(CACHE_FOLDER, exist_ok=True)
            with open(filepath, "w") as file:
                json.dump(data, file)
            logger.debug(
                f"Fetched data for '{name}' from API and cached it to '{filepath}'"
            )
        else:
            raise QueryError(f"{result.status_code}: {result.text}")

    return data


def fetch_transactions_for_account(
    oauth: OAuth2Session, account_id: str, start_date: date, end_date: date
):
    if start_date == end_date:
        name = f"{start_date}-{account_id}-transactions"
    else:
        name = f"{start_date}-{end_date}-{account_id}-transactions"

    transactions = query_or_cache(
        oauth,
        name,
        f"{API_URL}/api/v2/Transactions/archive/{account_id}",
        {"startDate": start_date.isoformat(), "endDate": end_date.isoformat()},
    )
    logger.info(
        f"Found {transactions['availableItems']} transactions for {account_id} from {start_date} to {end_date}"
    )
    return transactions


def fetch_accounts(oauth: OAuth2Session, execution_date: date):
    name = f"{execution_date.isoformat()}-accounts"

    accounts = query_or_cache(
        oauth,
        name,
        f"{API_URL}/api/v2/Accounts",
    )
    logger.info(f"Found {accounts['availableItems']} accounts the {execution_date}")
    return accounts

def fetch_cards(oauth: OAuth2Session, execution_date: date):
    name = f"{execution_date.isoformat()}-cards"

    cards = query_or_cache(
        oauth,
        name,
        f"{API_URL}/api/v2/Cards",
    )
    logger.info(f"Found {cards['availableItems']} cards the {execution_date}")
    return cards

def fetch_customer(oauth: OAuth2Session, execution_date: date):
    name = f"{execution_date.isoformat()}-customer"

    customer = query_or_cache(
        oauth,
        name,
        f"{API_URL}/api/v2/Customers",
    )
    logger.info(f"Found a customer with id {customer['customerId']} the {execution_date}")
    return customer


def store_raw_data(
    data: Any, name: str, data_folder: str, subfolders: List[str] = None
) -> str:
    """Store the data to the given folder with the naming given"""
    subfolders_ = "/".join(subfolders) + "/" if isinstance(subfolders, list) else ""

    filepath = f"{data_folder}/{subfolders_}{name}.json"
    os.makedirs(f"{data_folder}/{subfolders_}", exist_ok=True)

    with open(filepath, "w") as file:
        json.dump(data, file)
    logger.info(f"Stored '{name}' data to '{filepath}'")

    return filepath
