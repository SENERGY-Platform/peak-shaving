# Peak Shaving

Simulates peak shaving of the power a consumer draws from the grid, together with the
[battery simulation](https://github.com/SENERGY-Platform/battery-simulation). The two operators form a loop in one
flow: `battery_power` and `trigger_battery` of this operator go to `Power` and `trigger_battery` of the battery
simulation, its `Capacity` comes back as `capacity`.

The goal is a low highest mean grid draw of a quarter hour, the peak that is billed. The grid draw is the load plus
the battery power, which is positive while the battery charges.

- **Limit:** the grid draw the operator keeps every quarter hour below on average. Above it the battery discharges
  the excess, below it the battery charges, but only up to the limit. From the mean load of every quarter hour of
  the last 14 days, the operator computes the lowest limit down to which the battery, starting full, would have
  shaved every quarter hour without running empty: above the limit it covers the excess, below it it recharges with
  what the load leaves up to the limit, within its capacity and powers. A quarter hour that exceeds the limit by
  more than the discharging power raises it, for the whole 14 days: that peak is reached anyway, and a lower limit
  would only drain the battery on the other days. The limit is computed without `energy_reserve` percent of the
  energy, which covers a day that needs somewhat more than any in the history; live, the battery uses all of it.
  The limit is set at the start and again every day (UTC), so it follows the load both ways.
- **Within a quarter hour:** the operator counts the energy drawn from the grid since the quarter hour began. With
  each value it computes the battery power that keeps the mean of the quarter hour at the limit if the load stays as
  it is: the battery discharges what the load and the energy drawn so far leave above the limit, and charges up to
  the limit below it. The powers and the capacity of the battery bound it; within 10 Wh of full or empty it is
  neither charged nor discharged, as in the battery simulation.
- **Controls:** the battery is controlled (`trigger_battery` = `yes`) at least once a minute, and at once when the
  power it needs differs from the one set by more than a tenth of its discharging power, as when a peak begins.

Until the battery simulation has reported a capacity, and until there is a complete quarter hour, the battery is
not used. `max_capacity`, `max_charging_power` and `max_discharging_power` have to be those of the battery
simulation.

## History and restart

At every start, the operator reads the last 14 days of `power` from Timescale and accounts for them as for live
values, with the battery unused; live values that the history already holds are left out. So a restart or a new
volume loses nothing, and the initial phase counts from the first value of the history, which ends it at once for a
device with enough history. Only the time of the first value is kept on the volume.

The history is read over the database connection of the deployment (`TS_CONN` or `ts_conn`), or through
timescale-wrapper with `ts_wrapper_url` and `SENERGY_TOKEN`. Without either, or when `power` comes from more than one
device or not from a device, the operator starts without history.

A value holds until the next one, but no longer than five minutes; a longer gap counts as no data. Times are UTC:
the time of a value is the time the platform received it, and the quarter hours are those of UTC, which are those
of German time as well.

## Input

| key      | type  | description                                                 |
|----------|-------|-------------------------------------------------------------|
| power    | float | power of the consumer, in W; one device                     |
| capacity | float | capacity of the battery, in Wh, from the battery simulation |

## Output

| key             | type   | description                                                                    |
|-----------------|--------|--------------------------------------------------------------------------------|
| battery_power   | float  | power to charge (> 0) or discharge (< 0) the battery with, in W               |
| timestamp       | string | time of the value, UTC, `%Y-%m-%dT%H:%M:%S.%fZ`                                |
| trigger_battery | string | `yes` for a control, when the battery is to take `battery_power`; else `no`  |
| initial_phase   | string | the end of the initial phase while it lasts, else empty                        |

## Config options

| key                   | type   | default | description                                         |
|-----------------------|--------|---------|-----------------------------------------------------|
| init_phase_length     | float  | 2       | length of the initial phase                         |
| init_phase_level      | string | d       | unit of `init_phase_length`, e.g. `d`, `h`          |
| max_capacity          | float  | 500     | capacity of the full battery, in Wh                 |
| max_charging_power    | float  | 1000    | highest charging power of the battery, in W         |
| max_discharging_power | float  | 1000    | highest discharging power of the battery, in W      |
| energy_reserve        | float  | 10      | percent of the energy the limit is computed without |
| logger_level          | string | warning | log level                                           |

## Development

`operator-manifest.json` is generated from the declarations in `main.py` with
`python -m operator_lib manifest main.py`; the CI regenerates it before each release. Tests:
`python -m unittest discover -s tests`.
