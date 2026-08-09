from etl.stage_runner import run_stage


def test_run_stage_returns_result_and_duration():
    """
    Verify that run_stage executes a function,
    returns its result, and records duration.
    """

    stage_durations = {}

    def sample_stage(value):
        return value * 2

    result, duration = run_stage(
        "Test",
        sample_stage,
        5,
        stage_durations=stage_durations,
    )

    assert result == 10
    assert duration >= 0
    assert "Test" in stage_durations
    assert stage_durations["Test"] == duration