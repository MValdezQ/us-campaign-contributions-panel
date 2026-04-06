"""Name cleaning utilities for entity resolution.

This module provides functions to standardize organization names
before fuzzy matching/clustering.
"""

import re
from typing import Optional


# Legal suffixes to strip
LEGAL_SUFFIXES = [
    r'\b(inc|incorporated|corp|corporation|ltd|limited|llc|lllp|llp|lp|plc|sa|ag|gmbh|kgaa|se|nv|bv|oy|ab|as|asa|doo|sarl|sprl|srl|sro|d\.o\.o|s\.l|s\.a|s\.p\.a|n\.v|b\.v|c\.v|s\.c\.s)\b',
    r'\.com$',
    r'\.net$',
    r'\.org$',
    r'\.gov$',
]

# Common prefixes to consider stripping
COMMON_PREFIXES = [r'\b(the|a|an)\b']  # noqa: W605

# Character normalization patterns
CHAR_PATTERNS = [
    (r'&', 'and'),        # & -> and
    (r'@', 'at'),         # @ -> at
    (r'\s+', ' '),        # Multiple spaces -> single space
    (r'[^\w\s]', ' '),    # Non-word chars (except spaces) -> space
]


def clean_org_name(name: Optional[str]) -> str:
    """
    Standardize an organization name for clustering.

    Steps:
    1. Handle None/empty
    2. Lowercase
    3. Trim whitespace
    4. Normalize special characters (& -> and, @ -> at)
    5. Remove legal suffixes
    6. Remove common articles
    7. Collapse multiple spaces
    8. Strip again

    Parameters
    ----------
    name : str or None
        Raw organization name

    Returns
    -------
    str
        Cleaned name, ready for clustering

    Examples
    --------
    >>> clean_org_name("Apple Inc.")
    'apple'
    >>> clean_org_name("Apple Incorporated")
    'apple'
    >>> clean_org_name("The Boeing Company, Inc.")
    'boeing company'
    """
    if not name or not isinstance(name, str):
        return ""

    # Lowercase and trim
    cleaned = name.strip().lower()

    # Normalize special characters
    for pattern, replacement in CHAR_PATTERNS:
        cleaned = re.sub(pattern, replacement, cleaned)

    # Strip legal suffixes
    for suffix_pattern in LEGAL_SUFFIXES:
        cleaned = re.sub(suffix_pattern, '', cleaned, flags=re.IGNORECASE)

    # Strip common articles/prefixes
    for prefix_pattern in COMMON_PREFIXES:
        cleaned = re.sub(rf'^{prefix_pattern}\s+', '', cleaned, flags=re.IGNORECASE)

    # Collapse multiple spaces and trim
    cleaned = re.sub(r'\s+', ' ', cleaned).strip()

    return cleaned


def clean_batch(names):
    """
    Clean a batch of organization names (pandas Series or list).

    Parameters
    ----------
    names : pd.Series or list
        Organization names to clean

    Returns
    -------
    list
        Cleaned names
    """
    if hasattr(names, 'apply'):
        # pandas Series
        return names.apply(clean_org_name).tolist()
    else:
        # list or iterable
        return [clean_org_name(n) for n in names]
