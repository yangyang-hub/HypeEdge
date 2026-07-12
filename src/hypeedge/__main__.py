"""HypeEdge CLI entry point."""

import asyncio
import contextlib

from hypeedge.app import HypeEdgeApp


def main() -> None:
    """Run the HypeEdge application."""
    app = HypeEdgeApp()
    with contextlib.suppress(KeyboardInterrupt):
        asyncio.run(app.run())


if __name__ == "__main__":
    main()
