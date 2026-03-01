"""Shared lookup constants consumed by multiple generator modules.

Centralizing these values keeps geographic, timezone, and shipment-type rules
consistent across the pipeline and simplifies future updates.
"""

US_STATES = [
    "AL",
    "AK",
    "AZ",
    "AR",
    "CA",
    "CO",
    "CT",
    "DE",
    "FL",
    "GA",
    "HI",
    "ID",
    "IL",
    "IN",
    "IA",
    "KS",
    "KY",
    "LA",
    "ME",
    "MD",
    "MA",
    "MI",
    "MN",
    "MS",
    "MO",
    "MT",
    "NE",
    "NV",
    "NH",
    "NJ",
    "NM",
    "NY",
    "NC",
    "ND",
    "OH",
    "OK",
    "OR",
    "PA",
    "RI",
    "SC",
    "SD",
    "TN",
    "TX",
    "UT",
    "VT",
    "VA",
    "WA",
    "WV",
    "WI",
    "WY",
]

# Geographic regions used across modules
REGIONS = [
    "Northeast",
    "Southeast",
    "Midwest",
    "Southwest",
    "West",
    "Pacific Northwest",
]

# Mapping of time zone names to example states (used when assigning DC time zones) # noqa: E501
TIME_ZONES = {
    "Eastern": ["NY", "NJ", "MA", "PA", "GA", "FL", "NC", "SC", "VA", "OH", "MI"],
    "Central": ["TX", "IL", "WI", "MN", "MO", "TN", "AL", "LA"],
    "Mountain": ["CO", "UT", "AZ", "NM"],
    "Pacific": ["CA", "WA", "OR", "NV"],
}

# Common shipment types
SHIPMENT_TYPES = [
    "local",
    "wrap_and_label",
    "drop_ship",
    "standard",
    "expedited",
    "freight",
]
