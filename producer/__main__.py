"""Allow running the producer via ``python -m producer``."""

from .cli import main

if __name__ == "__main__":
    raise SystemExit(main())
