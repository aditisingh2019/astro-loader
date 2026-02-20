"""
Analysis Package.
Modules for comprehensive data analysis and insights generation.
"""

from analysis.runner import run_all_analyses
from analysis.revenue import analyze_revenue
from analysis.bookings import analyze_bookings
from analysis.cancellations import analyze_cancellations
from analysis.incomplete import analyze_incomplete_rides
from analysis.correlations import analyze_correlations

__all__ = [
    "run_all_analyses",
    "analyze_revenue",
    "analyze_bookings",
    "analyze_cancellations",
    "analyze_incomplete_rides",
    "analyze_correlations",
]