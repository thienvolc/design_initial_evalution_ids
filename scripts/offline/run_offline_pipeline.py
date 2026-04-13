from __future__ import annotations

from _common import ensure_src_on_path

ensure_src_on_path()
from ids_platform.offline.pipeline import main  # noqa: E402


if __name__ == "__main__":
    main()
