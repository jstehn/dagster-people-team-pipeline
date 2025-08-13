import time
from dataclasses import dataclass
from datetime import date, datetime, timezone
from distutils.util import strtobool
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
    def __convert_to_utc_string(dt):
        """
        Convert a string or datetime to UTC ISO 8601 string.
        Accepts:
        - ISO 8601 string (returns as-is if endswith 'Z')
        - naive datetime (assumes UTC)
        - aware datetime (converts to UTC)
        Always returns a string in '%Y-%m-%dT%H:%M:%S.%fZ' format.
        """
        if isinstance(dt, str):
            parsed = datetime.fromisoformat(dt)
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            else:
                parsed = parsed.astimezone(timezone.utc)
            return parsed.strftime("%Y-%m-%d %H:%M:%S")
        elif isinstance(dt, datetime):
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            else:
                dt = dt.astimezone(timezone.utc)
            return dt.strftime("%Y-%m-%d %H:%M:%S")
        else:
            raise TypeError(f"Unsupported type for date conversion: {type(dt)}")

    def __convert_from_utc_string(utc_string: str) -> datetime:
        """
        Convert a UTC ISO 8601 string to a naive datetime object.
        """
        return datetime.strptime(utc_string, "%Y-%m-%d %H:%M:%S").replace(
            tzinfo=timezone.utc
        )

    def __convert_date(value: str | date) -> date:
        if isinstance(value, date):
            return value
        elif isinstance(value, str):
            return datetime.strptime(value, "%Y-%m-%d").date()
        else:
            raise TypeError(
                f"Unsupported type for date conversion: {type(value)}"
            )

    def __convert_bool(value: str | bool) -> bool:
        if isinstance(value, bool):
            return value
        elif isinstance(value, str):
            return bool(strtobool(value))
        else:
            return bool(value)

    @dlt.resource(name="raw_vector_compliance", primary_key="complianceId")
    def compliance(
        begin_date: str | datetime,
        end_date: str | datetime,
        refreshed_since: Optional[str | datetime] = None,
    ) -> Iterator[Dict]:
        begin_date = __convert_to_utc_string(begin_date)
        end_date = __convert_to_utc_string(end_date)
        if refreshed_since:
            refreshed_since = __convert_to_utc_string(refreshed_since)
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
                }
                topic {
                    title
                    description
                }
                courseInfo {
                    courseInfoId
                }
                progress {
                    progressId
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
                person_node = node.get("person") or {}
                topic_node = node.get("topic") or {}
                course_info_node = node.get("courseInfo") or {}
                progress_node = node.get("progress") or {}
                yield {
                    "complianceId": str(node.get("complianceId")),
                    "personId": str(person_node.get("personId")),
                    "topicId": str(topic_node.get("topicId")),
                    "courseInfoId": str(course_info_node.get("courseInfoId")),
                    "progressId": str(progress_node.get("progressId")),
                    "effective": node.get("effective"),
                    "due": node.get("due"),
                    "expire": node.get("expire"),
                    "typeCode": str(node.get("typeCode")),
                    "lastRefreshed": node.get("lastRefreshed"),
                }
            page_info = compliance_data["pageInfo"]
            has_next_page = page_info["hasNextPage"]
            variables["after"] = page_info["endCursor"]

    @dlt.resource(name="raw_vector_progress", primary_key="progressId")
    def progress(
        start_date: str | datetime,
        end_date: str | datetime,
    ) -> Iterator[Dict]:
        start_date = __convert_to_utc_string(start_date)
        end_date = __convert_to_utc_string(end_date)
        get_progress_query = """
        query GetProgress($startDate: DateTime!, $endDate: DateTime!, $first: Int, $after: ID) {
            Progress(startDate: $startDate, endDate: $endDate, first: $first, after: $after) {
                nodes {
                    progressId
                    completed
                    completeTime
                    person {
                        personId
                    }
                    courseInfo {
                        courseInfoId
                    }
                    compliance {
                        complianceId
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
            "startDate": start_date,
            "endDate": end_date,
            "first": 100,
            "after": None,
        }
        has_next_page = True
        while has_next_page:
            result = query_vector_graphql(
                get_progress_query, variables=variables
            )
            data = result.get("data") or {}
            completions_data = data.get("Progress")
            if not completions_data:
                break
            nodes = completions_data["nodes"]
            for node in nodes:
                person_node = node.get("person") or {}
                course_info_node = node.get("courseInfo") or {}
                compliance_node = node.get("compliance") or {}
                yield {
                    "progressId": str(node.get("progressId")),
                    "personId": str(person_node.get("personId")),
                    "courseInfoId": str(course_info_node.get("courseInfoId")),
                    "completed": __convert_bool(node.get("completed")),
                    "completeTime": node.get("completeTime"),
                    "complianceId": str(compliance_node.get("complianceId")),
                    "maxQuizScore": float(node.get("maxQuizScore")),
                }
            page_info = completions_data["pageInfo"]
            has_next_page = page_info["hasNextPage"]
            variables["after"] = page_info["endCursor"]

    @dlt.resource(name="raw_vector_people", primary_key="personId")
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

    @dlt.resource(name="raw_vector_course_info", primary_key="courseInfoId")
    def course_info() -> Iterator[Dict]:
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
                yield {
                    "courseInfoId": str(node.get("courseInfoId")),
                    "title": str(node.get("title")),
                    "active": __convert_bool(node.get("active")),
                    "topicId": str(node.get("topicId")),
                    "topicTitle": str(node.get("topicTitle")),
                    "variantId": str(node.get("variantId")),
                    "variantSubtitle": str(node.get("variantSubtitle")),
                }
            page_info = result["data"]["CourseInfos"]["pageInfo"]
            has_next_page = page_info["hasNextPage"]
            variables["after"] = page_info["endCursor"]

    @dlt.resource(name="raw_vector_location", primary_key="locationId")
    def vector_location() -> Iterator[Dict]:
        get_locations_query = """
            query GetLocations($first: Int, $after: ID) {
                Locations(first: $first, after: $after) {
                    nodes {
                        locationId
                        name
                        code
                        parent {
                            locationId
                        }
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
                get_locations_query, variables=variables
            )
            locations_data = result["data"]["Locations"]
            if not locations_data:
                break
            nodes = locations_data["nodes"]
            for node in nodes:
                parent_node = node.get("parent") or {}
                yield {
                    "locationId": str(node.get("locationId")),
                    "name": str(node.get("name")),
                    "code": str(node.get("code")),
                    "parentLocationId": str(parent_node.get("locationId")),
                }
            page_info = result["data"]["Locations"]["pageInfo"]
            has_next_page = page_info["hasNextPage"]
            variables["after"] = page_info["endCursor"]

    @dlt.resource(name="raw_vector_job", primary_key="jobId")
    def vector_job() -> Iterator[Dict]:
        get_jobs_query = """
            query GetJobs($first: Int, $after: ID) {
                Jobs(first: $first, after: $after) {
                    nodes {
                        jobId
                        title
                        beginDate
                        endDate
                        person {
                            personId
                        }
                        position {
                            positionId
                        }
                        location {
                            locationId
                        }
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
            result = query_vector_graphql(get_jobs_query, variables=variables)
            data = result.get("data") or {}
            jobs_data = data.get("Jobs")
            if not jobs_data:
                break
            nodes = jobs_data["nodes"]
            for node in nodes:
                person_node = node.get("person") or {}
                position_node = node.get("position") or {}
                location_node = node.get("location") or {}
                yield {
                    "jobId": str(node.get("jobId")),
                    "title": str(node.get("title")),
                    "beginDate": str(node.get("beginDate")),
                    "endDate": str(node.get("endDate")),
                    "personId": str(person_node.get("personId")),
                    "positionId": str(position_node.get("positionId")),
                    "locationId": str(location_node.get("locationId")),
                }
            page_info = jobs_data["pageInfo"]
            has_next_page = page_info["hasNextPage"]
            variables["after"] = page_info["endCursor"]

    @dlt.resource(name="raw_vector_position", primary_key="positionId")
    def vector_position() -> Iterator[Dict]:
        get_positions_query = """
            query GetPositions($first: Int, $after: ID) {
                Positions(first: $first, after: $after) {
                    nodes {
                        positionId
                        code
                        name
                        parent {
                            positionId
                        }
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
                get_positions_query, variables=variables
            )
            positions_data = result["data"].get("Positions")
            if not positions_data:
                break
            nodes = positions_data["nodes"]
            for node in nodes:
                parent_node = node.get("parent") or {}
                yield {
                    "positionId": str(node.get("positionId")),
                    "code": str(node.get("code")),
                    "name": str(node.get("name")),
                    "parentPositionId": str(parent_node.get("positionId")),
                }
            page_info = positions_data["pageInfo"]
            has_next_page = page_info["hasNextPage"]
            variables["after"] = page_info["endCursor"]

    return [
        vector_people,
        course_info,
        progress,
        compliance,
        vector_location,
        vector_job,
        vector_position,
    ]
