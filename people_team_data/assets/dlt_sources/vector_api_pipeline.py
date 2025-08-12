import time
from dataclasses import dataclass
from typing import Dict, Iterator, Optional

import dlt
import requests
from dagster import EnvVar

VECTOR_CLIENT_ID = EnvVar("VECTOR_CLIENT_ID").get_value()
VECTOR_CLIENT_SECRET = EnvVar("VECTOR_CLIENT_SECRET").get_value()
VECTOR_DOMAIN = EnvVar("VECTOR_DOMAIN").get_value()
VECTOR_GRAPHQL_URL = f"{VECTOR_DOMAIN}/graphql"


@dataclass
class AccessToken:
    token_type: str
    access_token: str
    expires_in: int
    created_at: int

    def is_expired(self, buffer_seconds=60) -> bool:
        expiry_time = self.created_at + self.expires_in
        now = int(time.time())
        return now >= (expiry_time - buffer_seconds)

    @property
    def valid_access_token(self) -> str:
        if self.is_expired():
            updated = get_access_token()
            self.token_type = updated.token_type
            self.access_token = updated.access_token
            self.expires_in = updated.expires_in
            self.created_at = updated.created_at
        return self.access_token

    @property
    def auth_string(self) -> str:
        return f"{self.token_type} {self.valid_access_token}"


def get_access_token() -> AccessToken:
    """Return a valid access token for Vector API."""
    url = f"{VECTOR_DOMAIN}/oauth/token"
    params = {
        "grant_type": "client_credentials",
        "client_id": VECTOR_CLIENT_ID,
        "client_secret": VECTOR_CLIENT_SECRET,
    }
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
    }
    response = requests.post(url, params=params, headers=headers)
    response.raise_for_status()
    access_token = response.json()
    if not access_token:
        raise RuntimeError(
            "Failed to retrieve access token from Vector API response."
        )
    return AccessToken(**access_token)


access_token = get_access_token()


def query_vector_graphql(query: str, variables: Optional[Dict] = None) -> Dict:
    headers = {
        "Authorization": access_token.auth_string,
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    response = requests.post(
        VECTOR_GRAPHQL_URL,
        json={"query": query, "variables": variables or {}},
        headers=headers,
    )
    response.raise_for_status()
    return response.json()


@dlt.source(name="vector_api_source")
def vector_source() -> list:
    @dlt.resource(name="raw_vector_people")
    def vector_people() -> Iterator[Dict]:
        get_people_query = """
            query GetPeople($first: Int, $after: ID) {
                People(first: $first, after: $after) {
                    nodes {
                        personId
                        first
                        last
                        email
                    }
                    pageInfo {
                        hasNextPage
                        endCursor
                    }
                }
            }
        """
        variables = {"first": 100, "after": None}
        has_next_page = True
        while has_next_page:
            result = query_vector_graphql(get_people_query, variables=variables)
            people_data = result["data"]["People"]
            if not people_data:
                break
            nodes = people_data["nodes"]
            for node in nodes:
                yield node
            page_info = result["data"]["People"]["pageInfo"]
            has_next_page = page_info["hasNextPage"]
            variables["after"] = page_info["endCursor"]

    return [vector_people]
