import json
import time
from dataclasses import dataclass
from datetime import datetime, timezone
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


ACCESS_TOKEN = get_access_token()


def query_vector_graphql(query: str, variables: Optional[Dict] = None) -> Dict:
    headers = {
        "Authorization": ACCESS_TOKEN.auth_string,
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


@dlt.source(name="vector_api_pipeline")
def vector_source() -> list:
    def _convert_to_utc_string(dt):
        """
        Convert a string or datetime to UTC ISO 8601 string.
        Accepts:
        - ISO 8601 string (returns as-is if endswith 'Z')
        - naive datetime (assumes UTC)
        - aware datetime (converts to UTC)
        Always returns a string in '%Y-%m-%dT%H:%M:%S.%f+00:00' format.
        """
        if isinstance(dt, str):
            parsed = datetime.fromisoformat(dt)
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            else:
                parsed = parsed.astimezone(timezone.utc)
            return parsed.strftime("%Y-%m-%dT%H:%M:%S.%f+00:00")
        elif isinstance(dt, datetime):
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            else:
                dt = dt.astimezone(timezone.utc)
            return dt.strftime("%Y-%m-%dT%H:%M:%S.%f+00:00")
        else:
            raise TypeError(f"Unsupported type for date conversion: {type(dt)}")

    @dlt.resource(name="raw_vector_compliance")
    def compliance(
        begin_date: str | datetime,
        end_date: str | datetime,
        refreshed_since: Optional[str | datetime] = None,
    ) -> Iterator[Dict]:
        begin_date = _convert_to_utc_string(begin_date)
        end_date = _convert_to_utc_string(end_date)
        if refreshed_since:
            refreshed_since = _convert_to_utc_string(refreshed_since)
        get_compliance_query = """
            query GetCompliance($beginDate: Date, $endDate: Date, $refreshedSince: Date, $first: Int, $after: ID) {
                Compliance(beginDate: $beginDate, endDate: $endDate, refreshedSince: $refreshedSince, first: $first, after: $after) {
                    nodes {
                        complianceId
                        effective
                        due
                        expire
                        typeCode
                        lastRefreshed
                        person {
                            personId
                            first
                            last
                            email
                        }
                        topic {
                            title
                            description
                        }
                        courseInfo {
                            courseInfoId
                            title
                        }
                    }
                    pageInfo {
                        hasNextPage
                        endCursor
                    }
                }
            }
        """
        variables = {
            "beginDate": begin_date,
            "endDate": end_date,
            "refreshedSince": refreshed_since,
            "first": 100,
            "after": None,
        }
        has_next_page = True
        while has_next_page:
            result = query_vector_graphql(
                get_compliance_query, variables=variables
            )
            compliance_data = result["data"]["Compliance"]
            if not compliance_data:
                break
            nodes = compliance_data["nodes"]
            for node in nodes:
                yield node
            page_info = compliance_data["pageInfo"]
            has_next_page = page_info["hasNextPage"]
            variables["after"] = page_info["endCursor"]

    @dlt.resource(name="raw_vector_completions")
    def completions(
        start_date: str | datetime,
        end_date: str | datetime,
        location_id: Optional[str] = None,
        position_id: Optional[str] = None,
    ) -> Iterator[Dict]:
        start_date = _convert_to_utc_string(start_date)
        end_date = _convert_to_utc_string(end_date)
        get_completions_query = """
            query GetCompletions($locationId: ID, $positionId: ID, $startDate: DateTime!, $endDate: DateTime!, $first: Int, $after: ID) {
                Completions(locationId: $locationId, positionId: $positionId, startDate: $startDate, endDate: $endDate, first: $first, after: $after) {
                    nodes {
                        progressId
                        completed
                        completeTime
                        maxQuizScore
                        person {
                            personId
                            first
                            last
                            email
                        }
                        courseInfo {
                            courseInfoId
                            title
                        }
                    }
                    pageInfo {
                        hasNextPage
                        endCursor
                    }
                }
            }
        """
        variables = {
            "locationId": location_id,
            "positionId": position_id,
            "startDate": start_date,
            "endDate": end_date,
            "first": 100,
            "after": None,
        }
        has_next_page = True
        while has_next_page:
            result = query_vector_graphql(
                get_completions_query, variables=variables
            )
            completions_data = result["data"]["Completions"]
            if not completions_data:
                break
            nodes = completions_data["nodes"]
            for node in nodes:
                yield node
            page_info = completions_data["pageInfo"]
            has_next_page = page_info["hasNextPage"]
            variables["after"] = page_info["endCursor"]

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

    @dlt.resource(name="raw_vector_course_info")
    def course_infos() -> Iterator[Dict]:
        get_course_infos_query = """
            query GetCourseInfos($first: Int, $after: ID) {
                CourseInfos(first: $first, after: $after) {
                    nodes {
                        courseInfoId
                        title
                        active
                        topicId
                        topicTitle
                        variantId
                        variantSubtitle
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
            result = query_vector_graphql(
                get_course_infos_query, variables=variables
            )
            course_infos_data = result["data"]["CourseInfos"]
            if not course_infos_data:
                break
            nodes = course_infos_data["nodes"]
            for node in nodes:
                yield node
            page_info = result["data"]["CourseInfos"]["pageInfo"]
            has_next_page = page_info["hasNextPage"]
            variables["after"] = page_info["endCursor"]

    return [vector_people, course_infos, completions, compliance]
