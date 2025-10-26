"""Nurion Platform ASGI entrypoint."""

import uvicorn

from aether import create_app


def main() -> None:
    """Run the Nurion Platform service with uvicorn."""

    uvicorn.run(
        "aether.app:create_app",
        factory=True,
        host="0.0.0.0",
        port=8000,
        reload=True,
    )


if __name__ == "__main__":
    main()
