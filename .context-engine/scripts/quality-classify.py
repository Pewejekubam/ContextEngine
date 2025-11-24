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
import os
from pathlib import Path
from datetime import datetime, UTC

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
# HEURISTIC PATTERNS (CLS-011)
# ============================================================================

# CLS-011: 12 hardcoded generic advice patterns for v1.0.0
GENERIC_ADVICE_PATTERNS = [
    # 1. Descriptive naming
    re.compile(r'\buse\s+descriptive\s+(variable|function|class|method)?\s*names?\b', re.IGNORECASE),
    # 2. Unit testing
    re.compile(r'\bwrite\s+unit\s+tests?\b', re.IGNORECASE),
    # 3. Best practices
    re.compile(r'\bfollow\s+best\s+practices?\b', re.IGNORECASE),
    # 4. Code cleanliness
    re.compile(r'\bkeep\s+code\s+clean\b', re.IGNORECASE),
    # 5. Error handling
    re.compile(r'\bhandle\s+(exceptions?|errors?)\s+gracefully\b', re.IGNORECASE),
    # 6. Magic numbers
    re.compile(r'\bavoid\s+(magic\s+numbers?|hardcoded\s+values?)\b', re.IGNORECASE),
    # 7. Documentation
    re.compile(r'\bcomment\s+your\s+code\b|\bdocument\s+functions?\b', re.IGNORECASE),
    # 8. Design principles
    re.compile(r'\bfollow\s+(SOLID|DRY)\s+principles?\b', re.IGNORECASE),
    # 9. Commit messages
    re.compile(r'\buse\s+meaningful\s+commit\s+messages?\b', re.IGNORECASE),
    # 10. Refactoring
    re.compile(r'\brefactor\s+code\s+regularly\b', re.IGNORECASE),
    # 11. Code duplication
    re.compile(r'\bavoid\s+code\s+duplication\b', re.IGNORECASE),
    # 12. Static analysis
    re.compile(r'\buse\s+(linters?|static\s+analysis\s+tools?)\b', re.IGNORECASE),
]


# ============================================================================
# QUALITY CLASSIFIER CLASS
# ============================================================================

class QualityClassifier:
    """Hybrid heuristic + Claude quality classification for noise filtering."""

    def __init__(self, config):
        """Initialize classifier with configuration."""
        self.config = config

        # Database path
        self.db_path = BASE_DIR / config['structure']['database_path']

        # Template path
        self.template_path = BASE_DIR / 'templates' / 'runtime-template-quality-classification.txt'

        # Vocabulary path (CLS-004a)
        # Default to tag-vocabulary.yaml if not specified in config
        vocab_filename = 'tag-vocabulary.yaml'
        if 'structure' in config and 'vocabulary_file' in config['structure']:
            vocab_filename = config['structure']['vocabulary_file']
        self.vocab_path = BASE_DIR / 'config' / vocab_filename

        # Load build constants for batch size (CLS-001)
        self.batch_size = self.load_batch_size()

        # Load tier_1_domains (CLS-004a, CLS-004b)
        self.tier_1_domains = self.load_tier_1_domains()

        # Statistics
        self.stats = {
            'total_rules': 0,
            'heuristic_classified': 0,
            'claude_classified': 0,
            'project_specific': 0,
            'general_advice': 0,
            'noise': 0,
            'high_confidence': 0,
            'low_confidence': 0,
        }

    def load_batch_size(self):
        """Load batch size from build-constants.yaml (CLS-001)."""
        # Try to load from build-constants.yaml
        build_constants_path = BASE_DIR / '..' / '..' / 'build' / 'config' / 'build-constants.yaml'

        # Default batch size
        default_batch_size = 15

        try:
            if build_constants_path.exists():
                with open(build_constants_path) as f:
                    build_constants = yaml.safe_load(f)
                    return build_constants.get('tag_optimization', {}).get('classification_batch_size', default_batch_size)
        except Exception:
            pass

        return default_batch_size

    def load_tier_1_domains(self):
        """Load tier_1_domains from vocabulary file (CLS-004a, CLS-004b)."""
        try:
            with open(self.vocab_path) as f:
                vocab = yaml.safe_load(f)
                return vocab.get('tier_1_domains', {})
        except Exception as e:
            print(f"Warning: Could not load vocabulary file: {e}", file=sys.stderr)
            return {}

    def format_tier_1_domains_yaml(self):
        """Format tier_1_domains as YAML string for template (CLS-004c)."""
        if not self.tier_1_domains:
            return "# No domains configured yet"

        lines = []
        for domain_name, domain_spec in self.tier_1_domains.items():
            description = domain_spec.get('description', 'No description')
            lines.append(f"{domain_name}: {description}")

        return "\n".join(lines)

    def heuristic_classify(self, rule):
        """Apply heuristic fast-path classification (CLS-009, CLS-010, CLS-011, CLS-012)."""
        # Combine title and description for pattern matching
        text = f"{rule['title']} {rule.get('description', '')}"

        # CLS-012: Score based on pattern matches
        score = 0.0
        matches = []

        for pattern in GENERIC_ADVICE_PATTERNS:
            match = pattern.search(text)
            if match:
                matched_text = match.group(0).lower()

                # CLS-012: exact phrase = 1.0, partial match = 0.5
                # Consider it exact if the matched text is a substantial portion of the title
                # or if it's a complete phrase (15+ chars)
                title_lower = rule['title'].lower()

                # Exact match criteria:
                # 1. Match is majority of title (>60% of title length)
                # 2. Match is a complete phrase (15+ characters)
                if (len(matched_text) / len(title_lower) > 0.6) or (len(matched_text) >= 15):
                    score += 1.0
                    matches.append(matched_text)
                else:
                    score += 0.5
                    matches.append(matched_text)

        # CLS-010: High confidence threshold
        if score >= 0.7:
            # This is generic advice with high confidence
            confidence = min(0.8 + (score * 0.1), 0.95)  # 0.8-0.95 range
            return {
                'relevance': 'general_advice',
                'confidence': confidence,
                'scope': 'historical',
                'reasoning': f"Matches generic software advice patterns: {', '.join(matches[:2])}",
                'method': 'heuristic'
            }

        # No high-confidence heuristic match
        return None

    def claude_classify_batch(self, rules_batch):
        """Classify batch of rules using Claude API (CLS-001, CLS-002, CLS-005, CLS-006)."""
        # Load template
        try:
            with open(self.template_path) as f:
                template_content = f.read()
        except Exception as e:
            print(f"Error loading template: {e}", file=sys.stderr)
            return self.fallback_classifications(rules_batch)

        # Format tier_1_domains (CLS-004c)
        tier_1_domains_yaml = self.format_tier_1_domains_yaml()

        # Format rules batch as JSON (CLS-004d)
        rules_batch_json = json.dumps([{
            'id': r['id'],
            'type': r['type'],
            'title': r['title'],
            'description': r.get('description', ''),
            'domain': r.get('domain', '')
        } for r in rules_batch], indent=2)

        # Substitute template variables
        prompt = template_content.replace('{tier_1_domains_with_descriptions}', tier_1_domains_yaml)
        prompt = prompt.replace('{batch_size}', str(len(rules_batch)))
        prompt = prompt.replace('{rules_batch_formatted}', rules_batch_json)

        # Call Claude API
        try:
            api_key = os.environ.get('ANTHROPIC_API_KEY')
            if not api_key:
                print("Warning: ANTHROPIC_API_KEY not set, using fallback classifications", file=sys.stderr)
                return self.fallback_classifications(rules_batch)

            # Use Anthropic SDK
            try:
                import anthropic
            except ImportError:
                print("Warning: anthropic package not installed, using fallback classifications", file=sys.stderr)
                return self.fallback_classifications(rules_batch)

            client = anthropic.Anthropic(api_key=api_key)

            message = client.messages.create(
                model="claude-sonnet-4-20250514",
                max_tokens=4096,
                messages=[
                    {"role": "user", "content": prompt}
                ]
            )

            # Parse response (CLS-005)
            response_text = message.content[0].text

            # Extract JSON array from response
            json_match = re.search(r'\[[\s\S]*\]', response_text)
            if json_match:
                classifications = json.loads(json_match.group(0))

                # Map classifications to rules (CLS-005: order preservation)
                results = []
                for i, rule in enumerate(rules_batch):
                    if i < len(classifications):
                        cls = classifications[i]
                        results.append({
                            'relevance': cls.get('classification', 'noise'),
                            'confidence': float(cls.get('confidence', 0.5)),
                            'scope': cls.get('scope', 'historical'),
                            'reasoning': cls.get('reasoning', 'Claude classification'),
                            'method': 'claude'
                        })
                    else:
                        # Missing classification, use fallback
                        results.append(self.fallback_classification(rule))

                return results
            else:
                # Malformed JSON (CLS-006)
                print("Warning: Could not parse Claude response as JSON", file=sys.stderr)
                return self.fallback_classifications(rules_batch)

        except Exception as e:
            # API failure (CLS-006)
            print(f"Warning: Claude API call failed: {e}", file=sys.stderr)
            return self.fallback_classifications(rules_batch)

    def fallback_classification(self, rule):
        """Fallback classification with confidence 0.5 (CLS-006)."""
        return {
            'relevance': 'noise',
            'confidence': 0.5,
            'scope': 'historical',
            'reasoning': 'Classification failed, requires manual review',
            'method': 'fallback'
        }

    def fallback_classifications(self, rules_batch):
        """Generate fallback classifications for entire batch (CLS-006)."""
        return [self.fallback_classification(r) for r in rules_batch]

    def classify_rules(self):
        """Main classification workflow (CLS-008, CLS-009)."""
        # Connect to database
        try:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
        except Exception as e:
            print(f"Error connecting to database: {e}", file=sys.stderr)
            return False

        try:
            # Get all rules without quality classification
            cursor = conn.cursor()
            cursor.execute("""
                SELECT id, type, title, description, domain, metadata
                FROM rules
                WHERE lifecycle = 'active'
                AND (metadata IS NULL OR json_extract(metadata, '$.quality_classification') IS NULL)
            """)

            rules = cursor.fetchall()
            self.stats['total_rules'] = len(rules)

            if not rules:
                print("No rules to classify.")
                return True

            print(f"Found {len(rules)} rules to classify")
            print(f"Batch size: {self.batch_size}")
            print()

            # Process rules in two passes: heuristic, then Claude
            rules_for_claude = []
            heuristic_updates = []

            # Pass 1: Heuristic classification (CLS-009)
            print("Pass 1: Heuristic classification...")
            for rule in rules:
                rule_dict = dict(rule)
                heuristic_result = self.heuristic_classify(rule_dict)

                if heuristic_result:
                    # High-confidence heuristic match
                    self.stats['heuristic_classified'] += 1
                    self.stats[heuristic_result['relevance']] += 1
                    if heuristic_result['confidence'] >= 0.7:
                        self.stats['high_confidence'] += 1
                    else:
                        self.stats['low_confidence'] += 1

                    heuristic_updates.append((rule_dict['id'], heuristic_result))
                else:
                    # Needs Claude classification
                    rules_for_claude.append(rule_dict)

            print(f"  Heuristic classified: {self.stats['heuristic_classified']}")
            print(f"  Remaining for Claude: {len(rules_for_claude)}")
            print()

            # Pass 2: Claude batch classification (CLS-001)
            if rules_for_claude:
                print("Pass 2: Claude API classification...")

                # Process in batches
                for i in range(0, len(rules_for_claude), self.batch_size):
                    batch = rules_for_claude[i:i + self.batch_size]
                    batch_num = (i // self.batch_size) + 1
                    total_batches = (len(rules_for_claude) + self.batch_size - 1) // self.batch_size

                    print(f"  Batch {batch_num}/{total_batches} ({len(batch)} rules)...")

                    classifications = self.claude_classify_batch(batch)

                    # Update statistics and prepare database updates
                    for rule, classification in zip(batch, classifications):
                        self.stats['claude_classified'] += 1
                        self.stats[classification['relevance']] += 1
                        if classification['confidence'] >= 0.7:
                            self.stats['high_confidence'] += 1
                        else:
                            self.stats['low_confidence'] += 1

                        heuristic_updates.append((rule['id'], classification))

                print()

            # Pass 3: Update database with all classifications (CLS-007)
            print("Pass 3: Updating database...")
            for rule_id, classification in heuristic_updates:
                # Get existing metadata
                cursor.execute("SELECT metadata FROM rules WHERE id = ?", (rule_id,))
                row = cursor.fetchone()

                if row['metadata']:
                    metadata = json.loads(row['metadata'])
                else:
                    metadata = {}

                # Add quality_classification (CLS-007)
                metadata['quality_classification'] = {
                    'relevance': classification['relevance'],
                    'confidence': classification['confidence'],
                    'reasoning': classification['reasoning'],
                    'method': classification['method'],
                    'classified_at': datetime.now(UTC).strftime('%Y-%m-%dT%H:%M:%SZ')
                }

                # Update database
                cursor.execute(
                    "UPDATE rules SET metadata = ? WHERE id = ?",
                    (json.dumps(metadata), rule_id)
                )

            conn.commit()
            print(f"  Updated {len(heuristic_updates)} rules")
            print()

            # Print statistics
            print("Classification Statistics:")
            print("="*70)
            print(f"Total rules classified:    {self.stats['total_rules']}")
            print(f"  Heuristic method:        {self.stats['heuristic_classified']}")
            print(f"  Claude API method:       {self.stats['claude_classified']}")
            print()
            print("Relevance Distribution:")
            print(f"  project_specific:        {self.stats['project_specific']}")
            print(f"  general_advice:          {self.stats['general_advice']}")
            print(f"  noise:                   {self.stats['noise']}")
            print()
            print("Confidence Distribution:")
            print(f"  High confidence (>=0.7): {self.stats['high_confidence']}")
            print(f"  Low confidence (<0.7):   {self.stats['low_confidence']}")
            print()

            # CLS-003: Warn about low confidence rules
            if self.stats['low_confidence'] > 0:
                print(f"Warning: {self.stats['low_confidence']} rules have confidence < 0.7")
                print("These rules require manual review before auto-approval in tag optimization")
                print()

            return True

        finally:
            conn.close()


# ============================================================================
# MAIN ENTRY POINT
# ============================================================================

def main():
    """Hybrid heuristic + Claude quality classification for noise filtering before tag optimization"""
    print("Context Engine - Quality Classifier")
    print("="*70)
    print("Hybrid heuristic + Claude quality classification")
    print("Implements constraints: CLS-001 through CLS-012")
    print()

    # Load configuration
    try:
        config = load_config()
    except Exception as e:
        print(f"Error loading configuration: {e}", file=sys.stderr)
        return 1

    # Create classifier and run
    try:
        classifier = QualityClassifier(config)
        success = classifier.classify_rules()

        if success:
            print("Quality classification complete.")
            return 0
        else:
            print("Quality classification failed.", file=sys.stderr)
            return 1

    except Exception as e:
        print(f"Error during classification: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        return 1


if __name__ == '__main__':
    sys.exit(main())
