import logging
from sqlalchemy.engine import Engine

from analysis.revenue import run_revenue_analysis
from analysis.cancellations import run_cancellation_analysis
from analysis.correlations import run_correlation_analysis
from analysis.bookings import run_booking_analysis

logger = logging.getLogger(__name__)


def run_all_analyses(engine: Engine) -> None:
    logger.info("Starting analytics suite")

    run_revenue_analysis(engine)
    run_cancellation_analysis(engine)
    run_correlation_analysis(engine)

    logger.info("Analytics completed successfully")
