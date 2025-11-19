#!/usr/bin/env python3
"""
Runtime Vocabulary Schema Validator

Enforces VOC-001 through VOC-008 constraints from Spec 23 v1.1.0.
Validates user's config/tag-vocabulary.yaml during initialization.

This is a TEMPLATE - copied to dist/.context-engine/scripts/ by template_copy_generator.

Usage:
    python3 runtime-script-vocabulary-validator.py <vocabulary.yaml>

Exit Codes:
    0 - Vocabulary valid
    1 - Vocabulary invalid (errors printed to stdout)
"""

import sys
import re
import yaml
from pathlib import Path
from typing import Dict, List, Any


def validate_vocabulary_schema(vocabulary: Dict[str, Any]) -> List[str]:
    """
    Validate vocabulary schema (VOC-001 through VOC-008).

    Args:
        vocabulary: Loaded vocabulary dictionary

    Returns:
        List of error messages (empty if valid)
    """
    errors = []

    # VOC-008: Schema version
    version = vocabulary.get('schema_version')
    if version is None:
        errors.append("Missing 'schema_version' field")
    elif not isinstance(version, str):
        errors.append(f"schema_version must be string, got {type(version).__name__}")
    elif not re.match(r'^\d+\.\d+\.\d+$', version):
        errors.append(f"schema_version '{version}' must match pattern X.Y.Z")

    # VOC-001: Structure validation
    domains = vocabulary.get('tier_1_domains')
    if domains is None:
        errors.append("Missing 'tier_1_domains' key")
        return errors  # Can't continue without domains

    if not isinstance(domains, dict):
        errors.append(
            f"tier_1_domains must be dict (structured format).\n"
            f"Got {type(domains).__name__}.\n"
            f"See example: config/tag-vocabulary.yaml.example"
        )
        return errors  # Can't continue with wrong format

    # Validate each domain
    for domain_name, spec in domains.items():
        # VOC-001: Spec is dict
        if not isinstance(spec, dict):
            errors.append(f"{domain_name}: spec must be dict, got {type(spec).__name__}")
            continue

        # VOC-001: Required keys
        if 'description' not in spec:
            errors.append(f"{domain_name}: missing required 'description' key")
        if 'aliases' not in spec:
            errors.append(f"{domain_name}: missing required 'aliases' key")

        # VOC-002: Description validation
        description = spec.get('description', '')
        if description:
            word_count = len(description.split())
            if word_count < 5:
                errors.append(f"{domain_name}: description too short ({word_count} words, need 5-50)")
            elif word_count > 50:
                errors.append(f"{domain_name}: description too long ({word_count} words, need 5-50)")

        # VOC-003: Aliases validation
        aliases = spec.get('aliases', [])
        if not isinstance(aliases, list):
            errors.append(f"{domain_name}: aliases must be list, got {type(aliases).__name__}")
        else:
            for alias in aliases:
                if not isinstance(alias, str):
                    errors.append(f"{domain_name}: alias must be string, got {type(alias).__name__}")
                elif not alias:
                    errors.append(f"{domain_name}: empty alias")
                elif alias != alias.lower():
                    errors.append(f"{domain_name}: alias '{alias}' must be lowercase")

    # VOC-007: Alias uniqueness
    alias_errors = validate_alias_uniqueness(vocabulary)
    errors.extend(alias_errors)

    return errors


def validate_alias_uniqueness(vocabulary: Dict[str, Any]) -> List[str]:
    """
    Validate no alias collisions (VOC-007).

    Args:
        vocabulary: Loaded vocabulary dictionary

    Returns:
        List of error messages
    """
    errors = []
    seen_aliases = {}  # alias → domain mapping
    domain_names = set(vocabulary['tier_1_domains'].keys())

    for domain, spec in vocabulary['tier_1_domains'].items():
        for alias in spec.get('aliases', []):
            # Check 1: Alias doesn't match another domain's primary name
            if alias in domain_names and alias != domain:
                errors.append(
                    f"Alias '{alias}' in domain '{domain}' "
                    f"conflicts with primary domain name"
                )

            # Check 2: Alias not used by another domain
            if alias in seen_aliases:
                errors.append(
                    f"Alias '{alias}' used by both "
                    f"'{domain}' and '{seen_aliases[alias]}'"
                )
            else:
                seen_aliases[alias] = domain

    return errors


def load_vocabulary(vocab_path: Path) -> Dict[str, Any]:
    """
    Load vocabulary (dict format only).

    Implements INVARIANT-02: No backward compatibility with legacy list format.

    Args:
        vocab_path: Path to tag-vocabulary.yaml file

    Returns:
        Dictionary with tier_1_domains, tier_2_tags, etc.

    Raises:
        ValueError: If vocabulary has invalid schema (list format or missing fields)
    """
    with open(vocab_path) as f:
        vocabulary = yaml.safe_load(f)

    domains = vocabulary.get('tier_1_domains')

    # ONLY dict format supported (INVARIANT-02)
    if not isinstance(domains, dict):
        raise ValueError(
            f"tier_1_domains must be dict (structured format).\n"
            f"Got {type(domains).__name__}.\n"
            f"See example: config/tag-vocabulary.yaml.example"
        )

    # Validate schema (VOC-001 through VOC-008)
    errors = validate_vocabulary_schema(vocabulary)
    if errors:
        error_msg = "Validation errors:\n" + "\n".join(f"  - {e}" for e in errors)
        raise ValueError(error_msg)

    return vocabulary


def main():
    if len(sys.argv) != 2:
        print("Usage: runtime-script-vocabulary-validator.py <vocabulary.yaml>")
        sys.exit(1)

    vocab_path = Path(sys.argv[1])

    if not vocab_path.exists():
        print(f"Error: Vocabulary file not found: {vocab_path}")
        sys.exit(1)

    # Validate vocabulary schema
    try:
        vocabulary = load_vocabulary(vocab_path)
        print(f"✓ Vocabulary valid")
        sys.exit(0)
    except ValueError as e:
        print(f"✗ Vocabulary invalid")
        print(str(e))
        sys.exit(1)
    except Exception as e:
        print(f"✗ Unexpected error validating vocabulary")
        print(f"  {type(e).__name__}: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
