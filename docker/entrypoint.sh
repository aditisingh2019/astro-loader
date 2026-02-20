#!/bin/bash
set -e

# Color output for logs
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Logging functions
log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Wait for PostgreSQL to be ready
wait_for_postgres() {
    log_info "Waiting for PostgreSQL to be ready..."
    max_attempts=30
    attempt=1
    
    while [ $attempt -le $max_attempts ]; do
        if pg_isready -h ${DB_HOST:-postgres} -p ${DB_PORT:-5432} -U ${DB_USER:-postgres} &> /dev/null; then
            log_info "PostgreSQL is ready!"
            return 0
        fi
        
        echo -n "."
        sleep 1
        attempt=$((attempt + 1))
    done
    
    log_error "PostgreSQL failed to start after $max_attempts attempts"
    return 1
}

# Setup database (create tables, indexes, etc.)
setup_database() {
    log_info "Setting up database..."
    python -c "from src.db.connection import setup_database; setup_database()"
    log_info "Database setup complete"
}

# Run ingestion pipeline
run_ingestion() {
    local file=$1
    local chunksize=${2:-10000}
    
    if [ -z "$file" ]; then
        log_error "No file specified for ingestion"
        log_info "Usage: docker-compose run ingestion ingest <file> [chunksize]"
        log_info "Example: docker-compose run ingestion ingest ./data/raw/ncr_ride_bookings.csv 25000"
        exit 1
    fi
    
    if [ ! -f "$file" ]; then
        log_error "File not found: $file"
        exit 1
    fi
    
    log_info "Starting ingestion pipeline..."
    log_info "File: $file"
    log_info "Chunk size: $chunksize"
    
    python -m main --file "$file" --chunksize "$chunksize"
}

# Run analytics
run_analytics() {
    log_info "Starting analytics suite..."
    python -c "from src.db.connection import get_engine; from analysis.runner import run_all_analyses; run_all_analyses(get_engine())"
}

# Run tests
run_tests() {
    log_info "Running test suite..."
    pytest tests/ -v --tb=short
}

# Main entrypoint logic
main() {
    local command=$1
    shift || true
    
    # Always wait for PostgreSQL
    wait_for_postgres || exit 1
    
    # Setup database on first run
    setup_database
    
    case $command in
        ingest)
            run_ingestion "$@"
            ;;
        analyze)
            run_analytics
            ;;
        test)
            run_tests "$@"
            ;;
        shell)
            log_info "Starting interactive shell..."
            /bin/bash
            ;;
        *)
            log_error "Unknown command: $command"
            log_info "Available commands:"
            log_info "  ingest <file> [chunksize]  - Run ingestion pipeline"
            log_info "  analyze                    - Run analytics suite"
            log_info "  test                       - Run test suite"
            log_info "  shell                      - Start interactive bash shell"
            exit 1
            ;;
    esac
}

# Execute main function with all arguments
main "$@"
