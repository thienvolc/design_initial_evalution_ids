from __future__ import annotations

from ids_platform.offline.config import load_feature_list
from ids_platform.offline.paths import Paths


def load_selected_feature_names(paths: Paths) -> list[str]:
    """Load the configured feature registry for the current offline run."""

    return load_feature_list(paths.feature_registry_path)


__all__ = ["load_selected_feature_names"]
