from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import yaml

from firmin.utils.logger import get_logger

logger = get_logger(__name__)


@dataclass
class SheetsConfig:
    spreadsheet_id: str
    worksheet_name: str


@dataclass
class ConfidenceThresholds:
    green: int = 80
    yellow: int = 50


@dataclass
class EmailFilters:
    subject_contains: list[str] = field(default_factory=list)
    sender_contains: list[str] = field(default_factory=list)
    has_attachment: bool = True
    attachment_type: str = "pdf"


@dataclass
class ClientProfile:
    client_id: str
    display_name: str
    email_filters: EmailFilters
    job_number_patterns: list[str]
    defaults: dict
    sheets: SheetsConfig
    confidence_thresholds: ConfidenceThresholds
    known_locations: dict[str, str] = field(default_factory=dict)  # postcode -> Description
    conditional_locations: dict[str, list[dict]] = field(default_factory=dict)  # postcode -> [{keyword, result}]
    parser: str = "default"  # "default" = DS Smith AI pipeline, "unipet_manifest" = manifest parser


def _parse_profile(data: dict) -> ClientProfile:
    filters_data = data.get("email_filters", {})
    email_filters = EmailFilters(
        subject_contains=filters_data.get("subject_contains", []),
        sender_contains=filters_data.get("sender_contains", []),
        has_attachment=filters_data.get("has_attachment", True),
        attachment_type=filters_data.get("attachment_type", "pdf"),
    )

    sheets_data = data["sheets"]
    sheets = SheetsConfig(
        spreadsheet_id=sheets_data["spreadsheet_id"],
        worksheet_name=sheets_data["worksheet_name"],
    )

    thresholds_data = data.get("confidence_thresholds", {})
    thresholds = ConfidenceThresholds(
        green=thresholds_data.get("green", 80),
        yellow=thresholds_data.get("yellow", 50),
    )

    return ClientProfile(
        client_id=data["client_id"],
        display_name=data["display_name"],
        email_filters=email_filters,
        job_number_patterns=data.get("job_number_patterns", []),
        defaults=data.get("defaults", {}),
        sheets=sheets,
        confidence_thresholds=thresholds,
        known_locations=data.get("known_locations", {}),
        conditional_locations=data.get("conditional_locations", {}),
        parser=data.get("parser", "default"),
    )


def load_all_profiles(clients_dir: str = "config/clients") -> list[ClientProfile]:
    profiles = []
    clients_path = Path(clients_dir)

    if not clients_path.exists():
        logger.warning("Clients directory not found: %s", clients_dir)
        return profiles

    for yaml_file in clients_path.glob("*.yaml"):
        with open(yaml_file, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f)
        profile = _parse_profile(data)
        profiles.append(profile)
        logger.info("Loaded client profile: %s", profile.display_name)

    return profiles


def match_profile(
    subject: str,
    has_attachment: bool,
    profiles: list[ClientProfile],
    sender: str = "",
) -> Optional[ClientProfile]:
    for profile in profiles:
        filters = profile.email_filters

        if filters.has_attachment and not has_attachment:
            continue

        if filters.subject_contains:
            if not any(kw.lower() in subject.lower() for kw in filters.subject_contains):
                continue

        if filters.sender_contains:
            if not any(kw.lower() in sender.lower() for kw in filters.sender_contains):
                continue

        return profile

    return None
