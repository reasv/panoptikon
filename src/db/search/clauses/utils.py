from typing import List, Sequence, Tuple


def filter_targets_by_type(
    model_types: Sequence[str], targets: Sequence[Tuple[str, str]]
):
    """
    Filter a list of targets based on the given model types.
    """
    return [
        (model_type, setter)
        for model_type, setter in targets
        if model_type in model_types
    ]


def should_include_subclause(
    targets: List[Tuple[str, str]] | None, own_target_types: List[str]
):
    """
    Check if a subclause should be included based on the given targets.
    """
    if targets:
        own_targets = filter_targets_by_type(own_target_types, targets)
        if not own_targets:
            # If targets were provided, but none of them are our own targets,
            # Then this subclause was specifically not requested
            return False, None
        return True, own_targets
    else:
        # If no targets are provided, it means can match on any
        # Since no specific targets were requested
        return True, None
