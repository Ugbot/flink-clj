#!/bin/bash
#
# Run flink-clj examples
#
# Usage:
#   ./run-example.sh word-count
#   ./run-example.sh kafka-example
#   ./run-example.sh full-pipeline-example
#
# List examples:
#   ./run-example.sh --list

set -e

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
EXAMPLES_DIR="${SCRIPT_DIR}/examples"

# List available examples
list_examples() {
    echo "Available examples:"
    echo ""
    for f in "${EXAMPLES_DIR}"/*.clj; do
        if [[ -f "$f" ]]; then
            name=$(basename "$f" .clj | tr '_' '-')
            # Extract description from docstring if present
            desc=$(head -20 "$f" | grep -A1 '(ns' | grep '"' | head -1 | sed 's/.*"\([^"]*\)".*/\1/' | cut -c1-50)
            printf "  %-25s %s\n" "$name" "$desc"
        fi
    done
    echo ""
    echo "Job examples (in examples/jobs/):"
    for f in "${EXAMPLES_DIR}"/jobs/*_job.clj; do
        if [[ -f "$f" ]]; then
            name=$(basename "$f" .clj | tr '_' '-')
            printf "  %-25s\n" "$name"
        fi
    done
}

# Show help
show_help() {
    echo "Run flink-clj examples"
    echo ""
    echo "Usage:"
    echo "  ./run-example.sh <example-name>    Run an example"
    echo "  ./run-example.sh --list            List available examples"
    echo "  ./run-example.sh --help            Show this help"
    echo ""
    echo "Examples:"
    echo "  ./run-example.sh word-count"
    echo "  ./run-example.sh kafka-example"
    echo ""
    list_examples
}

# Handle arguments
case "${1:-}" in
    --list|-l)
        list_examples
        exit 0
        ;;
    --help|-h)
        show_help
        exit 0
        ;;
    "")
        echo -e "${RED}Error: No example specified${NC}"
        echo ""
        show_help
        exit 1
        ;;
esac

EXAMPLE="$1"
shift

# Convert kebab-case to snake_case for filename
EXAMPLE_FILE="${EXAMPLES_DIR}/${EXAMPLE//-/_}.clj"

# Also check in jobs directory
if [[ ! -f "$EXAMPLE_FILE" ]]; then
    EXAMPLE_FILE="${EXAMPLES_DIR}/jobs/${EXAMPLE//-/_}.clj"
fi

if [[ ! -f "$EXAMPLE_FILE" ]]; then
    echo -e "${RED}Error: Example not found: $EXAMPLE${NC}"
    echo ""
    list_examples
    exit 1
fi

echo -e "${GREEN}Running: $EXAMPLE${NC}"
echo "========================================="
echo ""

# Build classpath (suppress lein output)
echo "Building classpath..."
CP=$(lein with-profile +dev,+flink-1.20 classpath 2>/dev/null)

# Extract namespace from file
NAMESPACE=$(head -5 "$EXAMPLE_FILE" | grep '(ns' | sed 's/(ns \([^ )"]*\).*/\1/')

if [[ -z "$NAMESPACE" ]]; then
    echo -e "${RED}Error: Could not determine namespace from $EXAMPLE_FILE${NC}"
    exit 1
fi

echo "Namespace: $NAMESPACE"
echo ""

# Run the example
# Load the file and call -main with any additional args
java -cp "$CP" clojure.main -e "
(load-file \"$EXAMPLE_FILE\")
(if-let [main-fn (resolve (symbol \"$NAMESPACE\" \"-main\"))]
  (apply main-fn (list $@))
  (println \"No -main function found in $NAMESPACE\"))
"
