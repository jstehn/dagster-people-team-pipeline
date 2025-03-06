"""Schema definitions for BambooHR resources."""

from typing import Any, Dict


def get_bamboohr_schema() -> Dict[str, Dict[str, Any]]:
    """
    Get the schema definition for BambooHR employee data.

    The schema maps BambooHR API field names to our desired column names and types.
    Each field definition includes:
    - name: The desired column name in snake_case
    - data_type: The DLT data type (text, decimal, bigint, bool, timestamp, date)
    - description: Optional description of the field
    """
    return {
        # Personal Information
        "employeeNumber": {
            "name": "employee_number",
            "data_type": "text",
            "description": "Employee identification number",
        },
        "firstName": {
            "name": "first_name",
            "data_type": "text",
            "description": "Employee's first name",
        },
        "lastName": {
            "name": "last_name",
            "data_type": "text",
            "description": "Employee's last name",
        },
        "middleName": {
            "name": "middle_name",
            "data_type": "text",
            "description": "Employee's middle name",
        },
        "preferredName": {
            "name": "preferred_name",
            "data_type": "text",
            "description": "Employee's preferred name",
        },
        "gender": {
            "name": "gender",
            "data_type": "text",
            "description": "Employee's gender",
        },
        "genderIdentity": {
            "name": "gender_identity",
            "data_type": "text",
            "description": "Employee's gender identity",
        },
        "employeePronouns": {
            "name": "employee_pronouns",
            "data_type": "text",
            "description": "Employee's pronouns",
        },
        # Contact Information
        "email": {
            "name": "work_email",
            "data_type": "text",
            "description": "Work email address",
        },
        "workPhone": {
            "name": "work_phone",
            "data_type": "text",
            "description": "Work phone number",
        },
        "workPhoneExt": {
            "name": "work_phone_ext",
            "data_type": "text",
            "description": "Work phone extension",
        },
        # Employment Information
        "employmentStatus": {
            "name": "employment_status",
            "data_type": "text",
            "description": "Current employment status",
        },
        "hireDate": {
            "name": "hire_date",
            "data_type": "date",
            "description": "Date employee was hired",
        },
        "originalHireDate": {
            "name": "original_hire_date",
            "data_type": "date",
            "description": "Original hire date if rehired",
        },
        "terminationDate": {
            "name": "termination_date",
            "data_type": "date",
            "description": "Date of termination if applicable",
        },
        # Job Information
        "jobInformationDepartment": {
            "name": "department",
            "data_type": "text",
            "description": "Employee's department",
        },
        "jobInformationDivision": {
            "name": "division",
            "data_type": "text",
            "description": "Employee's division",
        },
        "jobInformationJobTitle": {
            "name": "job_title",
            "data_type": "text",
            "description": "Employee's job title",
        },
        "jobInformationLocation": {
            "name": "location",
            "data_type": "text",
            "description": "Employee's work location",
        },
        "jobInformationReportsTo": {
            "name": "reports_to",
            "data_type": "text",
            "description": "Employee's supervisor",
        },
        # Compensation
        "compensationChangeReason": {
            "name": "compensation_change_reason",
            "data_type": "text",
            "description": "Reason for compensation change",
        },
        "compensationEffectiveDate": {
            "name": "compensation_effective_date",
            "data_type": "date",
            "description": "Date when compensation change takes effect",
        },
        "compensationPayRate": {
            "name": "pay_rate",
            "data_type": "decimal",
            "description": "Employee's pay rate",
        },
        "compensationPayType": {
            "name": "pay_type",
            "data_type": "text",
            "description": "Type of pay (salary, hourly, etc.)",
        },
        "compensationPaySchedule": {
            "name": "pay_schedule",
            "data_type": "text",
            "description": "Pay schedule frequency",
        },
        "compensationOvertimeStatus": {
            "name": "overtime_status",
            "data_type": "text",
            "description": "Employee's overtime eligibility status",
        },
        # Custom CALPADS Fields
        "customField4460": {
            "name": "seid",
            "data_type": "text",
            "description": "SEID (State Educator Identifier)",
        },
        "customField4472": {
            "name": "total_years_edu_service",
            "data_type": "text",
            "description": "Total years of education service",
        },
        "customField4473": {
            "name": "total_years_in_lea",
            "data_type": "text",
            "description": "Total years in this LEA",
        },
        "customField4475": {
            "name": "is_hispanic",
            "data_type": "text",
            "description": "Hispanic/Latino ethnicity indicator",
        },
        "customField4476": {
            "name": "ethnicity_and_race",
            "data_type": "text",
            "description": "Employee's ethnicity and race",
        },
        "customField4477": {
            "name": "school",
            "data_type": "text",
            "description": "School assignment",
        },
        "customField4505": {
            "name": "fte",
            "data_type": "decimal",
            "description": "Full-time equivalent value",
        },
        "customField4506": {
            "name": "highest_education",
            "data_type": "text",
            "description": "Highest level of education completed",
        },
        "customField4507": {
            "name": "job_classification_code",
            "data_type": "text",
            "description": "Job classification code",
        },
        "customField4509": {
            "name": "non_classroom_assignment",
            "data_type": "text",
            "description": "Non-classroom based job assignment",
        },
        # Education
        "educationCollegeOrInstitution": {
            "name": "college",
            "data_type": "text",
            "description": "Name of college or institution attended",
        },
        "educationDegree": {
            "name": "degree",
            "data_type": "text",
            "description": "Degree earned",
        },
        "educationStartDate": {
            "name": "education_start_date",
            "data_type": "date",
            "description": "Start date of education",
        },
        "educationEndDate": {
            "name": "education_end_date",
            "data_type": "date",
            "description": "End date of education",
        },
        "educationMajorOrSpecialization": {
            "name": "major",
            "data_type": "text",
            "description": "Field of study or specialization",
        },
        "educationGpa": {
            "name": "gpa",
            "data_type": "decimal",
            "description": "Grade point average",
        },
        # Training
        "completedTrainingName": {
            "name": "training_name",
            "data_type": "text",
            "description": "Name of completed training",
        },
        "completedTrainingDate": {
            "name": "training_date",
            "data_type": "date",
            "description": "Date training was completed",
        },
        "completedTrainingCategory": {
            "name": "training_category",
            "data_type": "text",
            "description": "Category of training",
        },
        "completedTrainingNotes": {
            "name": "training_notes",
            "data_type": "text",
            "description": "Notes about completed training",
        },
        # Credentials and Certifications (Custom Table_11)
        "customTable4463": {
            "name": "credential_effective_date",
            "data_type": "date",
            "description": "Credential effective date",
        },
        "customTable4464": {
            "name": "credential_expiration_date",
            "data_type": "date",
            "description": "Credential expiration date",
        },
        "customTable4465": {
            "name": "credential_renewal_date",
            "data_type": "date",
            "description": "Credential renewal date",
        },
        "customTable4466": {
            "name": "credential_comments",
            "data_type": "text",
            "description": "Comments about credential",
        },
        "customTable4487": {
            "name": "credential_title",
            "data_type": "text",
            "description": "Credential title",
        },
        "customTable4556": {
            "name": "credential_document_type",
            "data_type": "text",
            "description": "Type of credential document",
        },
        "customTable4557": {
            "name": "credential_document_number",
            "data_type": "text",
            "description": "Credential document number",
        },
        "customTable4558": {
            "name": "credential_additional_auth",
            "data_type": "text",
            "description": "Additional authorization details",
        },
        "customTable4559": {
            "name": "credential_term",
            "data_type": "text",
            "description": "Credential term",
        },
        "customTable4560": {
            "name": "credential_subject",
            "data_type": "text",
            "description": "Credential subject",
        },
        "customTable4590": {
            "name": "credential_status",
            "data_type": "text",
            "description": "Credential status",
        },
        # System Fields
        "lastChanged": {
            "name": "last_changed",
            "data_type": "timestamp",
            "description": "Timestamp of last record update",
        },
        "lastChangedIso": {
            "name": "last_changed_iso",
            "data_type": "timestamp",
            "description": "ISO formatted timestamp of last record update",
        },
        "createdByUserId": {
            "name": "created_by_user_id",
            "data_type": "text",
            "description": "ID of user who created the record",
        },
    }


def get_bamboohr_fields() -> list:
    """Get the list of BambooHR fields to request from the API."""
    return list(get_bamboohr_schema().keys())


def transform_field_names(record: Dict[str, Any]) -> Dict[str, Any]:
    """
    Transform BambooHR field names to schema names.

    Args:
        record: A dictionary containing the raw BambooHR field names and values

    Returns:
        A dictionary with transformed field names according to our schema
    """
    schema = get_bamboohr_schema()
    return {schema[k]["name"]: v for k, v in record.items() if k in schema}


def transform_schema() -> Dict[str, Dict[str, Any]]:
    """
    Transform the schema to be used in the BigQuery destination.

    Returns:
        A dictionary mapping our column names to their definitions
    """
    schema = get_bamboohr_schema()
    return {v["name"]: v for k, v in schema.items()}
