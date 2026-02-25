# Telemetry

TimelessLogs emits [telemetry](https://hex.pm/packages/telemetry) events for monitoring and observability.

## Events

### Flush

Emitted when the buffer flushes entries to a block.

**Event:** `[:timeless_logs, :flush, :stop]`

| Measurement | Type | Description |
|-------------|------|-------------|
| `duration` | integer (native time) | Time taken to write the block |
| `entry_count` | integer | Number of entries in the block |
| `byte_size` | integer | Size of the written block in bytes |

| Metadata | Type | Description |
|----------|------|-------------|
| `block_id` | integer | ID of the written block |

### Query

Emitted when a query completes.

**Event:** `[:timeless_logs, :query, :stop]`

| Measurement | Type | Description |
|-------------|------|-------------|
| `duration` | integer (native time) | Total query execution time |
| `total` | integer | Total matching entries (before pagination) |
| `blocks_read` | integer | Number of blocks decompressed |

| Metadata | Type | Description |
|----------|------|-------------|
| `filters` | keyword | The query filters used |

### Compaction

Emitted when a compaction run completes.

**Event:** `[:timeless_logs, :compaction, :stop]`

| Measurement | Type | Description |
|-------------|------|-------------|
| `duration` | integer (native time) | Time taken for compaction |
| `raw_blocks` | integer | Number of raw blocks compacted |
| `entry_count` | integer | Total entries in compacted blocks |
| `byte_size` | integer | Size of compressed output |

### Retention

Emitted when a retention run completes.

**Event:** `[:timeless_logs, :retention, :stop]`

| Measurement | Type | Description |
|-------------|------|-------------|
| `duration` | integer (native time) | Time taken for retention enforcement |
| `blocks_deleted` | integer | Number of blocks removed |

### Block error

Emitted when a block fails to read or decompress.

**Event:** `[:timeless_logs, :block, :error]`

| Metadata | Type | Description |
|----------|------|-------------|
| `file_path` | string | Path to the block file |
| `reason` | term | Error reason |

## Attaching handlers

### Basic logging

```elixir
:telemetry.attach_many(
  "timeless-logs-logger",
  [
    [:timeless_logs, :flush, :stop],
    [:timeless_logs, :query, :stop],
    [:timeless_logs, :compaction, :stop],
    [:timeless_logs, :retention, :stop],
    [:timeless_logs, :block, :error]
  ],
  fn event, measurements, metadata, _config ->
    require Logger
    Logger.info("#{inspect(event)}: #{inspect(measurements)} #{inspect(metadata)}")
  end,
  nil
)
```

### With telemetry_metrics

```elixir
defmodule MyApp.Telemetry do
  use Supervisor
  import Telemetry.Metrics

  def start_link(arg) do
    Supervisor.start_link(__MODULE__, arg, name: __MODULE__)
  end

  def init(_arg) do
    children = [
      {Telemetry.Metrics.ConsoleReporter, metrics: metrics()}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end

  defp metrics do
    [
      summary("timeless_logs.flush.stop.duration",
        unit: {:native, :millisecond}),
      summary("timeless_logs.query.stop.duration",
        unit: {:native, :millisecond}),
      counter("timeless_logs.flush.stop.entry_count"),
      counter("timeless_logs.block.error")
    ]
  end
end
```

### With TimelessMetrics

If you're also running [TimelessMetrics](https://github.com/awksedgreep/timeless_metrics), you can write telemetry events as metrics:

```elixir
:telemetry.attach(
  "timeless-logs-to-metrics",
  [:timeless_logs, :flush, :stop],
  fn _event, %{entry_count: count, byte_size: bytes}, _metadata, _config ->
    TimelessMetrics.write(:metrics, "timeless_logs_flush_entries", %{}, count / 1)
    TimelessMetrics.write(:metrics, "timeless_logs_flush_bytes", %{}, bytes / 1)
  end,
  nil
)
```
