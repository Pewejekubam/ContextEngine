#!/usr/bin/env python3
"""
Quality classifier with heuristic fast-path and Claude batching

Implements constraints: CLS-001 through CLS-012
Generated from: specs/modules/runtime-script-quality-classifier-v1.0.0.yaml
"""

import sys
import json
import sqlite3
import re
import subprocess
import argparse
from pathlib import Path
from datetime import datetime, UTC
from typing import Dict, List, Optional, Tuple

# INV-023: Check Python version
if sys.version_info < (3, 8):
    print("Error: Python 3.8+ required", file=sys.stderr)
    sys.exit(1)

import yaml

# INV-021: Absolute paths only - read from config
# First, determine config path relative to this script
SCRIPT_DIR = Path(__file__).parent
BASE_DIR = SCRIPT_DIR.parent
CONFIG_PATH = BASE_DIR / "config" / "deployment.yaml"

# Load config to get project root and context engine home
with open(CONFIG_PATH) as f:
    _config = yaml.safe_load(f)
    PROJECT_ROOT = Path(_config['paths']['project_root'])
    # Read context_engine_home from config - allows .context-engine to be placed anywhere
    BASE_DIR = Path(_config['paths']['context_engine_home'])


def load_config():
    """Load deployment configuration and vocabulary."""
    with open(CONFIG_PATH) as f:
        config = yaml.safe_load(f)
    return config


# ============================================================================
# RUNTIME-SCRIPT-QUALITY-CLASSIFIER MODULE IMPLEMENTATION
# ============================================================================

# CLS-011: Hardcoded heuristic patterns for v1.0.0
HEURISTIC_PATTERNS = [
    (r'\buse\s+descriptive\s+(variable|function|class|method)?\s*names?\b', 1.0),
    (r'\bwrite\s+unit\s+tests?\b', 1.0),
    (r'\bfollow\s+best\s+practices?\b', 1.0),
    (r'\bkeep\s+code\s+clean\b', 1.0),
    (r'\bhandle\s+(exceptions?|errors?)\s+gracefully\b', 1.0),
    (r'\bavoid\s+(magic\s+numbers?|hardcoded\s+values?)\b', 1.0),
    (r'\bcomment\s+your\s+code\b', 1.0),
    (r'\bdocument\s+functions?\b', 1.0),
    (r'\bfollow\s+(SOLID|DRY)\s+principles?\b', 1.0),
    (r'\buse\s+meaningful\s+commit\s+messages?\b', 1.0),
    (r'\brefactor\s+code\s+regularly\b', 1.0),
    (r'\bavoid\s+code\s+duplication\b', 1.0),
    (r'\buse\s+(linters?|static\s+analysis\s+tools?)\b', 1.0),
]


class QualityClassifier:
    """Hybrid heuristic + Claude quality classification for noise filtering."""

    def __init__(self, config, batch_size: int = 15):
        """Initialize classifier with configuration."""
        self.config = config
        self.batch_size = batch_size  # CLS-001

        # Database path - try new structure first, fallback to old
        if 'database_path' in config.get('structure', {}):
            self.db_path = BASE_DIR / config['structure']['database_path']
        else:
            # Fallback to old structure
            self.db_path = BASE_DIR / config['paths']['database']

        # Template path
        self.template_path = BASE_DIR / config['structure']['templates_dir'] / 'runtime-template-quality-classification.txt'

        # Vocabulary path (CLS-004a) - hardcoded per OPT-019b pattern
        self.vocab_path = BASE_DIR / 'config' / 'tag-vocabulary.yaml'

        # Connect to database
        self.conn = sqlite3.connect(str(self.db_path))
        self.conn.row_factory = sqlite3.Row

        # Load vocabulary
        self.vocabulary = self._load_vocabulary()

        # Statistics
        self.stats = {
            'total_processed': 0,
            'heuristic_classified': 0,
            'claude_classified': 0,
            'errors': 0,
            'by_relevance': {'project_specific': 0, 'general_advice': 0, 'noise': 0}
        }

    def _load_vocabulary(self) -> dict:
        """Load vocabulary file (CLS-004a)."""
        try:
            with open(self.vocab_path) as f:
                vocab = yaml.safe_load(f)
            return vocab
        except Exception as e:
            print(f"ERROR: Failed to load vocabulary: {e}", file=sys.stderr)
            sys.exit(1)

    def _format_tier1_domains(self) -> str:
        """Format tier_1_domains for template (CLS-004c)."""
        tier_1_domains = self.vocabulary.get('tier_1_domains', {})

        # Format as YAML string with domain names and descriptions only
        formatted_domains = {}
        for domain_name, domain_spec in tier_1_domains.items():
            if isinstance(domain_spec, dict):
                formatted_domains[domain_name] = domain_spec.get('description', '')
            else:
                formatted_domains[domain_name] = str(domain_spec)

        return yaml.dump(formatted_domains, default_flow_style=False, sort_keys=False)

    def apply_heuristic_filter(self, rule: dict) -> Optional[dict]:
        """Apply heuristic fast-path filtering (CLS-009, CLS-010, CLS-011, CLS-012)."""
        text = f"{rule['title']} {rule['description']}".lower()

        score = 0.0
        matched_patterns = []

        # Check each pattern
        for pattern, weight in HEURISTIC_PATTERNS:
            if re.search(pattern, text, re.IGNORECASE):
                score += weight
                matched_patterns.append(pattern)

        # CLS-012: threshold >= 0.7 triggers classification without Claude
        if score >= 0.7:
            # CLS-010: Heuristics classify with confidence >= 0.8 (generic advice)
            confidence = min(0.8 + (score - 0.7) * 0.2, 1.0)

            return {
                'rule_id': rule['id'],
                'classification': 'general_advice',
                'confidence': confidence,
                'scope': 'historical',
                'reasoning': f"Generic software engineering platitude (matched {len(matched_patterns)} patterns)",
                'method': 'heuristic'
            }

        return None

    def classify_batch_with_claude(self, rules: List[dict]) -> List[dict]:
        """Classify rules batch using Claude (CLS-001, CLS-002, CLS-005, CLS-006)."""
        try:
            # Load template
            with open(self.template_path) as f:
                template = f.read()

            # CLS-004c: Format tier_1_domains
            tier_1_domains_formatted = self._format_tier1_domains()

            # Format rules batch as JSON
            rules_batch_formatted = json.dumps([
                {
                    'rule_id': rule['id'],
                    'type': rule['type'],
                    'title': rule['title'],
                    'description': rule['description'],
                    'domain': rule['domain']
                }
                for rule in rules
            ], indent=2)

            # Substitute template variables
            prompt = template.format(
                tier_1_domains_with_descriptions=tier_1_domains_formatted,
                batch_size=len(rules),
                rules_batch_formatted=rules_batch_formatted
            )

            # Write prompt to temporary file
            prompt_file = BASE_DIR / 'data' / f'quality-classify-prompt-{datetime.now(UTC).isoformat()}.txt'
            prompt_file.parent.mkdir(parents=True, exist_ok=True)

            with open(prompt_file, 'w') as f:
                f.write(prompt)

            # Invoke Claude CLI
            result = subprocess.run(
                ['claude', '--print'],
                stdin=open(prompt_file),
                capture_output=True,
                text=True,
                timeout=120
            )

            # Clean up prompt file
            prompt_file.unlink()

            if result.returncode != 0:
                raise RuntimeError(f"Claude CLI failed: {result.stderr}")

            # Parse response
            response_text = result.stdout.strip()

            # Extract JSON from markdown code blocks if present
            if '```json' in response_text:
                json_match = re.search(r'```json\s*(\[.*?\])\s*```', response_text, re.DOTALL)
                if json_match:
                    response_text = json_match.group(1)
            elif '```' in response_text:
                json_match = re.search(r'```\s*(\[.*?\])\s*```', response_text, re.DOTALL)
                if json_match:
                    response_text = json_match.group(1)

            # Parse JSON
            classifications = json.loads(response_text)

            # CLS-005: Validate array order preservation
            if not isinstance(classifications, list) or len(classifications) != len(rules):
                raise ValueError(f"Expected {len(rules)} classifications, got {len(classifications)}")

            # Add method tag
            for c in classifications:
                c['method'] = 'claude'

            return classifications

        except Exception as e:
            # CLS-006: Classification failures default to confidence 0.5
            print(f"WARNING: Claude classification failed: {e}", file=sys.stderr)
            return [
                {
                    'rule_id': rule['id'],
                    'classification': 'general_advice',
                    'confidence': 0.5,
                    'scope': 'project_wide',
                    'reasoning': f"Classification failed: {str(e)}",
                    'method': 'fallback'
                }
                for rule in rules
            ]

    def store_classification(self, rule_id: str, classification: dict):
        """Store classification in metadata.quality_classification (CLS-007)."""
        # Build metadata structure
        metadata_update = {
            'quality_classification': {
                'relevance': classification['classification'],
                'confidence': classification['confidence'],
                'reasoning': classification['reasoning'],
                'method': classification['method'],
                'classified_at': datetime.now(UTC).strftime('%Y-%m-%dT%H:%M:%SZ')
            }
        }

        # Merge with existing metadata
        cursor = self.conn.execute(
            "UPDATE rules SET metadata = json_patch(COALESCE(metadata, '{}'), ?) WHERE id = ?",
            (json.dumps(metadata_update), rule_id)
        )
        self.conn.commit()

    def classify_all(self, limit: Optional[int] = None):
        """Classify all unclassified rules."""
        # Query unclassified rules
        query = """
            SELECT id, type, title, description, domain, metadata
            FROM rules
            WHERE metadata IS NULL
               OR json_extract(metadata, '$.quality_classification') IS NULL
            ORDER BY created_at DESC
        """

        if limit:
            query += f" LIMIT {limit}"

        cursor = self.conn.execute(query)
        all_rules = [dict(row) for row in cursor.fetchall()]

        total = len(all_rules)

        if total == 0:
            print("No unclassified rules found.")
            return

        print(f"\nClassifying {total} rules...")
        print(f"Batch size: {self.batch_size}")
        print()

        processed = 0
        heuristic_count = 0

        # Process in batches
        batch = []
        batch_rules = []

        for rule in all_rules:
            # Try heuristic first (CLS-009)
            heuristic_result = self.apply_heuristic_filter(rule)

            if heuristic_result:
                # Heuristic classified (CLS-010)
                self.store_classification(rule['id'], heuristic_result)
                processed += 1
                heuristic_count += 1
                self.stats['heuristic_classified'] += 1
                self.stats['by_relevance'][heuristic_result['classification']] += 1

                print(f"  [{processed}/{total}] {rule['id']}: {rule['title'][:50]} | heuristic | {heuristic_result['classification']} | confidence={heuristic_result['confidence']:.2f}")
            else:
                # Add to batch for Claude classification
                batch_rules.append(rule)

                # Process batch when full
                if len(batch_rules) >= self.batch_size:
                    classifications = self.classify_batch_with_claude(batch_rules)

                    for rule, classification in zip(batch_rules, classifications):
                        self.store_classification(rule['id'], classification)
                        processed += 1
                        self.stats['claude_classified'] += 1
                        self.stats['by_relevance'][classification['classification']] += 1

                        print(f"  [{processed}/{total}] {rule['id']}: {rule['title'][:50]} | claude | {classification['classification']} | confidence={classification['confidence']:.2f}")

                    batch_rules = []

        # Process remaining batch
        if batch_rules:
            classifications = self.classify_batch_with_claude(batch_rules)

            for rule, classification in zip(batch_rules, classifications):
                self.store_classification(rule['id'], classification)
                processed += 1
                self.stats['claude_classified'] += 1
                self.stats['by_relevance'][classification['classification']] += 1

                print(f"  [{processed}/{total}] {rule['id']}: {rule['title'][:50]} | claude | {classification['classification']} | confidence={classification['confidence']:.2f}")

        self.stats['total_processed'] = processed

        # Print summary
        print()
        print("="*70)
        print("Quality Classification Summary")
        print("="*70)
        print(f"Total Processed: {self.stats['total_processed']}")
        print(f"Heuristic Classified: {self.stats['heuristic_classified']} ({100*self.stats['heuristic_classified']/max(1,self.stats['total_processed']):.1f}%)")
        print(f"Claude Classified: {self.stats['claude_classified']} ({100*self.stats['claude_classified']/max(1,self.stats['total_processed']):.1f}%)")
        print()
        print("By Relevance:")
        print(f"  project_specific: {self.stats['by_relevance']['project_specific']}")
        print(f"  general_advice: {self.stats['by_relevance']['general_advice']}")
        print(f"  noise: {self.stats['by_relevance']['noise']}")
        print()


def main():
    """Hybrid heuristic + Claude quality classification for noise filtering before tag optimization"""
    parser = argparse.ArgumentParser(
        description='Quality classifier with heuristic fast-path and Claude batching'
    )
    parser.add_argument('--limit', type=int, help='Limit number of rules to classify')
    parser.add_argument('--batch-size', type=int, help='Override batch size (default from config)')

    args = parser.parse_args()

    print("Context Engine - Runtime-script-quality-classifier Module")
    print("="*70)

    # Load configuration
    try:
        config = load_config()
    except Exception as e:
        print(f"Error loading configuration: {e}", file=sys.stderr)
        sys.exit(1)

    # Load batch size from build-constants.yaml (CLS-001)
    try:
        build_constants_path = BASE_DIR.parent / 'build' / 'config' / 'build-constants.yaml'
        if build_constants_path.exists():
            with open(build_constants_path) as f:
                build_config = yaml.safe_load(f)
                batch_size = build_config.get('tag_optimization', {}).get('classification_batch_size', 15)
        else:
            batch_size = 15
    except Exception:
        batch_size = 15

    # Override from command line if provided
    if args.batch_size:
        batch_size = args.batch_size

    # Initialize classifier
    classifier = QualityClassifier(config, batch_size=batch_size)

    # Classify all unclassified rules
    classifier.classify_all(limit=args.limit)

    return 0


if __name__ == '__main__':
    sys.exit(main())
