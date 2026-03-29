"""Player name mapping between CricketData.org API names and our display names.

CricketData.org uses full player names (e.g., "Virat Kohli"), which generally
match our display names in teams.py. This mapping handles any edge cases where
the API name differs from what we use internally.
"""

# API name -> display name (only needed when they differ)
API_TO_DISPLAY = {
    # CricketData.org differences
    "Nithish Kumar Reddy": "Nitish Kumar Reddy",
    "Vaibhav Suryavanshi": "Vaibhav Sooryavanshi",
    # Cricbuzz API differences (name field vs our display name)
    "Philip Salt": "Phil Salt",
    "Lokesh Rahul": "KL Rahul",
    "Shahrukh Khan": "M Shahrukh Khan",
    "Abhishek Porel": "Abishek Porel",
    "Varun Chakaravarthy": "Varun Chakravarthy",
}

# Reverse: display name -> API name
DISPLAY_TO_API = {v: k for k, v in API_TO_DISPLAY.items()}


def get_display_name(api_name: str) -> str:
    """Convert API player name to our display name."""
    return API_TO_DISPLAY.get(api_name, api_name)


def get_api_name(display_name: str) -> str:
    """Convert our display name to the API player name."""
    return DISPLAY_TO_API.get(display_name, display_name)
