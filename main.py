"""
Purpose:
--------
Application entry point for the data ingestion subsystem.

Responsibilities:
-----------------
- Load configuration.
- Initialize logging.
- Trigger pipeline execution.

Important Behavior:
-------------------
- Minimal logic by design.
- Exits with meaningful status codes.
- Suitable for CLI or scheduled execution.

Design Notes:
-------------
- Keeps runtime concerns separate from pipeline logic.
"""

from __future__ import annotations

import sys
import argparse
import logging
from src.db.connection import setup_database
setup_database()

from src.utils.logger import setup_logger
from src.ingestion.pipeline import run_pipeline
from src.db.connection import get_engine
from analysis.runner import run_all_analyses


# Exit codes
EXIT_SUCCESS = 0
EXIT_FAILURE = 1

# Parse CLI arguments.
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run the ride bookings ingestion pipeline."
    )

    parser.add_argument(
        "--file",
        help="Path to input data file"
    )

    parser.add_argument(
        "--chunksize",
        type=int,
        default=10000,
        help="Chunk size for processing large files (default=10000)"
    )

    parser.add_argument(
    "--analyze",
    action="store_true",
    help="Run analytics after ingestion"
)

    return parser.parse_args()

# Application entry point. Initializes logging, loads configuration, and triggers pipeline execution.
def main() -> int:
    
    engine = get_engine()

    logger = setup_logger(engine)
    

    try:
        args = parse_args()

        logger.info("Starting ingestion application.")
        logger.info(f"Input file: {args.file}")
        logger.info(f"Chunksize: {args.chunksize}")

        if args.file:
            run_pipeline(
                filename=args.file,
                chunksize=args.chunksize,
                engine=engine
            )

        logger.info("Ingestion completed successfully.")

        if args.analyze:
            run_all_analyses(engine)

        return EXIT_SUCCESS

    except Exception:
        logging.exception("Fatal error during ingestion execution.")
        return EXIT_FAILURE


if __name__ == "__main__":
    sys.exit(main())

