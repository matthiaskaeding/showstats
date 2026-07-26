# Example data

Data vendored purely so the README example demonstrates showstats on
something recognisable. Not used by the test suite — the tests build their
own frames from `tests/conftest.py`.

## seattle-weather.csv

Daily weather observations for Seattle, 2012-2015. 1,461 rows.

- Source: [vega-datasets](https://github.com/vega/vega-datasets/blob/main/data/seattle-weather.csv)
- Licence: BSD-3-Clause
- Derived from NOAA GHCN daily records

Vendored rather than downloaded at render time so that rendering the README
needs no network, and so the documentation cannot break because a URL moved.

| column | type | notes |
| --- | --- | --- |
| `date` | Date | one row per day |
| `precipitation` | Float64 | mm |
| `temp_max`, `temp_min` | Float64 | °C |
| `wind` | Float64 | m/s |
| `weather` | String | one of drizzle, rain, sun, snow, fog |

It covers all three of showstats' tables — date, numerical and categorical —
which is why it was chosen over more familiar alternatives like penguins or
iris, neither of which has a date column.
