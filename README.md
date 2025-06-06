# Fudstop

Fudstop is a collection of Python utilities for market data, market
research and trading automation.  It bundles asynchronous wrappers for
popular APIs and a set of helper functions for quick data analysis.

## Features

- Async clients for Polygon.io and Webull
- Options Clearing Corporation and Yahoo Finance SDKs
- Economic data helpers (New York Fed, FRED and more)
- Utility functions for working with trading data
- Example scripts in `fudstop/examples`

## Installation

```bash
pip install fudstop4
```

## Configuration

Create a `.env` file in the project root and populate it with the API
keys you plan to use.  A template is provided in this repository.

## Quick start

```python
from fudstop.apis.polygonio.async_polygon_sdk import Polygon

poly = Polygon()

async def main():
    trade = await poly.last_trade("AAPL")
    print(trade)

# asyncio.run(main())
```

More examples can be found in the [examples](fudstop/examples) folder.

## Web interface

A small Flask app is provided in the `webapp/` directory for viewing
records stored in your PostgreSQL database. Populate the `DB_HOST`,
`DB_USER`, `DB_PW`, `DB_NAME` and `DB_PORT` variables in `.env` and run:

```bash
python -m webapp.app
```

The homepage shows a snapshot from the `wb_opts` table.

## License

This project is released under the [MIT License](LICENSE).

*The code is provided for educational purposes only and is not financial
advice.*
