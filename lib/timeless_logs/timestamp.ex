defmodule TimelessLogs.Timestamp do
  @moduledoc false

  # Canonical internal timestamp unit: microseconds since the Unix epoch.
  #
  # Entries arrive from the logger handler (already microseconds), the
  # NDJSON HTTP API, and programmatic ingest where callers may pass unix
  # seconds, milliseconds, microseconds, nanoseconds, or a DateTime.
  # Everything is normalized here so block ts_min/ts_max, per-entry time
  # filters, ordering, and age-based retention all agree on one unit.
  #
  # The magnitude heuristic is unambiguous for dates between 1973 and
  # roughly year 5138 in every unit.

  @spec to_microseconds(DateTime.t() | integer()) :: integer()
  def to_microseconds(%DateTime{} = dt), do: DateTime.to_unix(dt, :microsecond)

  def to_microseconds(ts) when is_integer(ts) do
    cond do
      # seconds (< ~year 5138)
      ts < 100_000_000_000 -> ts * 1_000_000
      # milliseconds
      ts < 100_000_000_000_000 -> ts * 1_000
      # microseconds
      ts < 100_000_000_000_000_000 -> ts
      # nanoseconds
      true -> div(ts, 1_000)
    end
  end
end
