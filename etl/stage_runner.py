import time

from collections.abc import Callable
from typing import Any

from utils.logger import get_logger


logger = get_logger(__name__)


def run_stage(
    stage_name: str,
    stage_function: Callable[..., Any],
    *args: Any,
    **kwargs: Any,
) -> tuple[Any, float]:
    """
    Execute a pipeline stage, measure its duration,
    log the result, and return the result with duration.
    """

    start_time = time.perf_counter()

    result = stage_function(
        *args,
        **kwargs,
    )

    duration = (
        time.perf_counter()
        - start_time
    )

    logger.info(
        "%s stage completed in %.2f seconds.",
        stage_name,
        duration,
    )

    return result, duration