import pytest

from unittest.mock import patch, MagicMock, call
from src.utils.logger import setup_logger

@patch("src.ingestion.pipeline.get_engine")
def test_logger_setup(mock_get_engine):
    
    engine = MagicMock()
    mock_get_engine.return_value = engine

    setup_logger(engine)