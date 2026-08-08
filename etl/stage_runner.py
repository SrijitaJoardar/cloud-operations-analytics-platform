import time

from collections.abc import Callable
from typing import Any

from utils.logger import get_logger


logger = get_logger(__name__)


def run_stage(
    stage_name: str,
    stage_function: Callable[..., Any],
    *args: Any,
    stage_durations: dict[str, float] | None = None,
    **kwargs: Any,
) -> tuple[Any, float]:
    """
    Execute a pipeline stage, measure its duration,
    log the result, and optionally store the duration.
    """

    start_time = time.perf_counter()

    try:

        result = stage_function(
            *args,
            **kwargs,
        )

    except Exception as exc:

        logger.exception(
            "%s stage failed: %s",
            stage_name,
            str(exc),
        )

        raise

    duration = (
        time.perf_counter()
        - start_time
    )

    if stage_durations is not None:
        stage_durations[
            stage_name
        ] = duration

    logger.info(
        "%s stage completed in %.2f seconds.",
        stage_name,
        duration,
    )

    return result, duration