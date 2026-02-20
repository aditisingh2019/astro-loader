"""
Analytics Runner.
- Orchestrate all analysis modules
- Execute analyses in logical order
- Provide unified reporting interface
"""

import logging
import sys
from sqlalchemy.engine import Engine

from analysis.revenue import analyze_revenue
from analysis.bookings import analyze_bookings
from analysis.cancellations import analyze_cancellations
from analysis.incomplete import analyze_incomplete_rides
from analysis.correlations import analyze_correlations

logger = logging.getLogger(__name__)


def run_all_analyses(engine: Engine) -> None:
    """Run all analytics modules in sequence."""
    
    logger.info("="*60)
    logger.info("Starting comprehensive analytics suite")
    logger.info("="*60)

    analyses = [
        ("Bookings Insights", analyze_bookings),
        ("Revenue Analysis", analyze_revenue),
        ("Cancellation Analysis", analyze_cancellations),
        ("Incomplete Rides Analysis", analyze_incomplete_rides),
        ("Correlation Analysis", analyze_correlations),
    ]

    failed_analyses = []

    try:
        with engine.begin() as connection:
            for name, analysis_func in analyses:
                try:
                    logger.info(f"Running: {name}")
                    analysis_func(connection)
                    logger.info(f"✓ {name} completed successfully")
                except Exception as e:
                    logger.exception(f"✗ {name} failed: {str(e)}")
                    failed_analyses.append(name)

        # Summary
        logger.info("\n" + "="*60)
        if failed_analyses:
            logger.warning(f"Analytics completed with {len(failed_analyses)} failure(s):")
            for name in failed_analyses:
                logger.warning(f"  - {name}")
        else:
            logger.info("✓ All analytics completed successfully")
        logger.info("="*60)

    except Exception as e:
        logger.critical("Failed to execute analytics suite")
        logger.exception(str(e))
        raise
