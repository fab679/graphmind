"""Tests for graphmind_mcp.escape — Cypher injection prevention."""

import pytest

from graphmind_mcp.escape import escape_string, is_readonly_cypher, validate_identifier


# ── escape_string ─────────────────────────────────────────────────────


class TestEscapeString:
    def test_plain_text(self):
        assert escape_string("hello") == "hello"

    def test_double_quotes(self):
        assert escape_string('say "hello"') == 'say \\"hello\\"'

    def test_single_quotes(self):
        assert escape_string("it's fine") == "it\\'s fine"

    def test_backslash(self):
        assert escape_string("a\\b") == "a\\\\b"

    def test_newline(self):
        assert escape_string("line1\nline2") == "line1\\nline2"

    def test_carriage_return(self):
        assert escape_string("a\rb") == "a\\rb"

    def test_none_returns_empty(self):
        assert escape_string(None) == ""

    def test_empty_string(self):
        assert escape_string("") == ""

    def test_combined(self):
        # Input: "a<newline>b\c'd
        inp = "\"a\nb\\c'd"
        # Expected: \"a\nb\\c\'d
        exp = "\\\"a\\nb\\\\c\\'d"
        assert escape_string(inp) == exp

    def test_unicode_passthrough(self):
        assert escape_string("héllo wörld") == "héllo wörld"


# ── validate_identifier ──────────────────────────────────────────────


class TestValidateIdentifier:
    def test_simple_label(self):
        assert validate_identifier("Person") == "Person"

    def test_underscore_start(self):
        assert validate_identifier("_Internal") == "_Internal"

    def test_with_digits(self):
        assert validate_identifier("node123") == "node123"

    def test_rejects_leading_digit(self):
        with pytest.raises(ValueError, match="Invalid Cypher identifier"):
            validate_identifier("123label")

    def test_rejects_hyphen(self):
        with pytest.raises(ValueError):
            validate_identifier("my-label")

    def test_rejects_space(self):
        with pytest.raises(ValueError):
            validate_identifier("my label")

    def test_rejects_dot(self):
        with pytest.raises(ValueError):
            validate_identifier("a.b")

    def test_rejects_injection(self):
        with pytest.raises(ValueError):
            validate_identifier("label}) RETURN n //")

    def test_rejects_empty(self):
        with pytest.raises(ValueError):
            validate_identifier("")

    def test_rejects_braces(self):
        with pytest.raises(ValueError):
            validate_identifier("a{b}")


# ── is_readonly_cypher ───────────────────────────────────────────────


class TestIsReadonlyCypher:
    def test_simple_match(self):
        assert is_readonly_cypher("MATCH (n) RETURN n") is True

    def test_match_where(self):
        assert is_readonly_cypher(
            'MATCH (n:Person) WHERE n.name = "Alice" RETURN n'
        ) is True

    def test_create_rejected(self):
        assert is_readonly_cypher("CREATE (n:Person {name: 'x'})") is False

    def test_delete_rejected(self):
        assert is_readonly_cypher("MATCH (n) DELETE n") is False

    def test_detach_delete_rejected(self):
        assert is_readonly_cypher("MATCH (n) DETACH DELETE n") is False

    def test_set_rejected(self):
        assert is_readonly_cypher("MATCH (n) SET n.age = 30") is False

    def test_remove_rejected(self):
        assert is_readonly_cypher("MATCH (n) REMOVE n.age") is False

    def test_merge_rejected(self):
        assert is_readonly_cypher("MERGE (n:Person {name: 'x'})") is False

    def test_drop_rejected(self):
        assert is_readonly_cypher("DROP INDEX my_index") is False

    def test_keyword_inside_string_ignored(self):
        assert is_readonly_cypher(
            'MATCH (n) WHERE n.name = "CREATE" RETURN n'
        ) is True

    def test_keyword_inside_single_quotes_ignored(self):
        assert is_readonly_cypher(
            "MATCH (n) WHERE n.action = 'DELETE' RETURN n"
        ) is True

    def test_case_insensitive(self):
        assert is_readonly_cypher("match (n) create (m)") is False

    def test_explain_readonly(self):
        assert is_readonly_cypher("EXPLAIN MATCH (n) RETURN n") is True

    def test_with_clause_readonly(self):
        assert is_readonly_cypher(
            "MATCH (n) WITH n.name AS name RETURN name"
        ) is True
