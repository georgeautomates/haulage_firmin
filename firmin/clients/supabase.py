from __future__ import annotations
import os
import re
from typing import Optional

import psycopg2
import psycopg2.extras

from firmin.utils.logger import get_logger

logger = get_logger(__name__)

_NORMALISE_POSTCODE = re.compile(r'\s+')

# Tier 3: fuzzy query — matches on OrganisationName (primary) + full_address (secondary)
# Returns similarity score so we can reject low-confidence matches.
LOCATION_QUERY = """
SELECT "Description" AS point_name,
       (similarity("OrganisationName", %s) * 0.6 + similarity(full_address, %s) * 0.4) AS score
FROM "Location Points"
WHERE REGEXP_REPLACE("PostCode", '\\s+', ' ', 'g') = %s
ORDER BY score DESC
LIMIT 1
"""

# Tier 3b: org-name-only fallback when postcode extraction fails or is wrong
LOCATION_QUERY_NO_POSTCODE = """
SELECT "Description" AS point_name,
       similarity("OrganisationName", %s) AS score
FROM "Location Points"
WHERE similarity("OrganisationName", %s) > 0.45
ORDER BY score DESC
LIMIT 1
"""

# Minimum combined similarity score to accept a fuzzy match.
# Below this threshold the match is too uncertain and we return None
# rather than risk writing a wrong location to the sheet.
_FUZZY_MIN_SCORE = 0.35

# Tier 3: cache lookup
CACHE_LOOKUP_QUERY = """
SELECT matched_description
FROM location_mappings
WHERE postcode = %s AND client_name = %s AND verified = true
LIMIT 1
"""

# Tier 3: cache insert (no unique constraint, guard with NOT EXISTS)
CACHE_INSERT_QUERY = """
INSERT INTO location_mappings (pdf_address, postcode, matched_description, verified, client_name)
SELECT %s, %s, %s, false, %s
WHERE NOT EXISTS (
    SELECT 1 FROM location_mappings
    WHERE postcode = %s AND client_name = %s AND pdf_address = %s
)
"""


class SupabaseClient:
    def __init__(self):
        self.dsn = os.getenv("SUPABASE_POSTGRES_DSN")
        if not self.dsn:
            raise RuntimeError("SUPABASE_POSTGRES_DSN environment variable not set")

    def _connect(self):
        return psycopg2.connect(self.dsn, cursor_factory=psycopg2.extras.RealDictCursor)

    def lookup_location(
        self,
        postcode: str,
        org_name: str,
        search: str,
        known_locations: dict[str, str] | None = None,
        conditional_locations: dict[str, list[dict]] | None = None,
        client_name: str = "",
        pdf_address: str = "",
    ) -> Optional[str]:
        """
        Three-tier location lookup:
          Tier 1 — known_locations / conditional_locations override from client profile (exact, instant)
          Tier 2 — location_mappings cache (verified human matches)
          Tier 3 — fuzzy Postgres query on OrganisationName + full_address
        """
        normalised = _NORMALISE_POSTCODE.sub(" ", postcode.upper().strip())

        # Tier 1a: conditional overrides — postcode + keyword match in org_name
        if conditional_locations:
            conditions = conditional_locations.get(normalised) or conditional_locations.get(postcode)
            if conditions:
                org_upper = org_name.upper()
                for rule in conditions:
                    if rule["keyword"].upper() in org_upper:
                        logger.debug("Tier 1 conditional override for %s (%s) -> %s", postcode, org_name, rule["result"])
                        return rule["result"]
                # fallback within conditional block if no keyword matched
                fallback = next((r["result"] for r in conditions if not r.get("keyword")), None)
                if fallback:
                    logger.debug("Tier 1 conditional fallback for %s -> %s", postcode, fallback)
                    return fallback

        # Tier 1b: exact known_locations override
        if known_locations:
            override = known_locations.get(normalised) or known_locations.get(postcode)
            if override:
                logger.debug("Tier 1 override for %s -> %s", postcode, override)
                return override

        try:
            with self._connect() as conn:
                with conn.cursor() as cur:
                    # Tier 2: verified cache
                    if client_name:
                        cur.execute(CACHE_LOOKUP_QUERY, (normalised, client_name))
                        row = cur.fetchone()
                        if row:
                            logger.debug("Tier 2 cache hit for %s -> %s", postcode, row["matched_description"])
                            return row["matched_description"]

                    # Tier 3: fuzzy search by postcode + org name
                    cur.execute(LOCATION_QUERY, (org_name, search, normalised))
                    row = cur.fetchone()
                    if row and row["score"] >= _FUZZY_MIN_SCORE:
                        result = row["point_name"]
                        logger.debug(
                            "Tier 3 fuzzy match for %s (score=%.2f) -> %s",
                            postcode, row["score"], result,
                        )
                        # Store in cache as unverified for future human review
                        if client_name and pdf_address:
                            try:
                                cur.execute(CACHE_INSERT_QUERY, (
                                    pdf_address, normalised, result, client_name,
                                    normalised, client_name, pdf_address,
                                ))
                                conn.commit()
                            except Exception:
                                pass  # cache write failure is non-fatal
                        return result
                    elif row:
                        logger.warning(
                            "Tier 3 fuzzy match score too low (%.2f) for postcode %s org '%s' — trying org-only fallback",
                            row["score"], postcode, org_name,
                        )

                    # Tier 3b: org-name-only fallback (handles wrong/missing postcode from AI)
                    cur.execute(LOCATION_QUERY_NO_POSTCODE, (org_name, org_name))
                    row = cur.fetchone()
                    if row and row["score"] >= 0.5:
                        result = row["point_name"]
                        logger.debug(
                            "Tier 3b org-only fallback match (score=%.2f) -> %s",
                            row["score"], result,
                        )
                        if client_name and pdf_address:
                            try:
                                cur.execute(CACHE_INSERT_QUERY, (
                                    pdf_address, normalised, result, client_name,
                                    normalised, client_name, pdf_address,
                                ))
                                conn.commit()
                            except Exception:
                                pass
                        return result

                    logger.debug("No location match for postcode: %s org: %s", postcode, org_name)
                    return None

        except Exception as e:
            logger.error("Supabase lookup failed for postcode %s: %s", postcode, e)
            return None

    # Kept for backwards compatibility
    def lookup_collection_point(self, postcode: str, search: str) -> Optional[str]:
        return self.lookup_location(postcode, org_name=search, search=search)

    def lookup_delivery_point(self, postcode: str, search: str) -> Optional[str]:
        return self.lookup_location(postcode, org_name=search, search=search)
