"""
JSON file reader implementation.

Responsibilities:
- Read JSON files (array-of-objects or newline-delimited JSON).
- Parse JSON records into dictionaries.
- Normalize JSON structures into flat records if required.

Important Behavior:
- Must handle large JSON files via streaming where possible.
- Must not perform validation or data cleaning.
- Should fail fast on invalid JSON structure.
- JSON structure assumptions must be documented and configurable.
"""
