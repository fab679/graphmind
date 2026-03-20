"""Cypher injection prevention utilities."""

from __future__ import annotations

import re

_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

_WRITE_KEYWORDS = frozenset({
    "CREATE", "DELETE", "DETACH", "SET", "REMOVE",
    "MERGE", "DROP", "CALL", "FOREACH", "LOAD",
})


def escape_string(value: str | None) -> str:
    """Escape a value for safe inclusion in a Cypher string literal.

    Handles ``"``, ``'``, ``\\``, newlines, and carriage returns.
    Returns empty string for ``None``.
    """
    if value is None:
        return ""
    return (
        value
        .replace("\\", "\\\\")
        .replace('"', '\\"')
        .replace("'", "\\'")
        .replace("\n", "\\n")
        .replace("\r", "\\r")
    )


def validate_identifier(name: str) -> str:
    """Validate a Cypher identifier (label, property, type name).

    Raises ``ValueError`` if *name* contains characters that could enable
    Cypher injection.
    """
    if not _IDENTIFIER_RE.match(name):
        raise ValueError(f"Invalid Cypher identifier: {name!r}")
    return name


def is_readonly_cypher(cypher: str) -> bool:
    """Return ``True`` if *cypher* contains no write keywords.

    String literals are stripped before checking so that keywords inside
    quoted values are ignored.
    """
    cleaned = re.sub(r'"[^"]*"', "", cypher)
    cleaned = re.sub(r"'[^']*'", "", cleaned)
    tokens = cleaned.upper().split()
    return not any(token in _WRITE_KEYWORDS for token in tokens)
