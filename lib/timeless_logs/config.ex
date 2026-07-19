defmodule TimelessLogs.Config do
  @moduledoc false

  @spec storage() :: :disk | :memory
  def storage do
    Application.get_env(:timeless_logs, :storage, :disk)
  end

  @spec data_dir() :: String.t()
  def data_dir do
    Application.get_env(:timeless_logs, :data_dir, "priv/log_stream")
  end

  @spec flush_interval() :: pos_integer()
  def flush_interval do
    Application.get_env(:timeless_logs, :flush_interval, 1_000)
  end

  @spec max_buffer_size() :: pos_integer()
  def max_buffer_size do
    Application.get_env(:timeless_logs, :max_buffer_size, 1_000)
  end

  @spec query_timeout() :: pos_integer()
  def query_timeout do
    Application.get_env(:timeless_logs, :query_timeout, 30_000)
  end

  # Queued entries per shard (buffer + pending batches + in-flight flushes)
  # above which batch ingest switches from cast to call, pacing producers
  # to the durable drain rate. Sized to absorb bursts at full speed.
  @spec ingest_soft_watermark() :: pos_integer()
  def ingest_soft_watermark do
    Application.get_env(:timeless_logs, :ingest_soft_watermark, 50_000)
  end

  # Raw (uncompacted) block bytes on disk above which batch ingest also
  # paces, letting the compactor catch up before queries drown in raw
  # blocks. The compactor maintains the gauge.
  @spec ingest_raw_debt_limit() :: pos_integer()
  def ingest_raw_debt_limit do
    Application.get_env(:timeless_logs, :ingest_raw_debt_limit, 2_000_000_000)
  end

  # How long a paced producer waits for drain capacity before accepting
  # anyway (with a loud error). Only reachable when the pipeline has
  # stalled outright.
  @spec ingest_backpressure_timeout() :: pos_integer()
  def ingest_backpressure_timeout do
    Application.get_env(:timeless_logs, :ingest_backpressure_timeout, 60_000)
  end

  # 7 days in seconds
  @default_retention_max_age 7 * 86_400

  @spec retention_max_age() :: pos_integer() | nil
  def retention_max_age do
    Application.get_env(:timeless_logs, :retention_max_age, @default_retention_max_age)
  end

  @spec retention_max_size() :: pos_integer() | nil
  def retention_max_size do
    Application.get_env(:timeless_logs, :retention_max_size, nil)
  end

  @spec retention_check_interval() :: pos_integer()
  def retention_check_interval do
    Application.get_env(:timeless_logs, :retention_check_interval, 120_000)
  end

  @spec compaction_threshold() :: pos_integer()
  def compaction_threshold do
    Application.get_env(:timeless_logs, :compaction_threshold, 500)
  end

  @spec compaction_interval() :: pos_integer()
  def compaction_interval do
    Application.get_env(:timeless_logs, :compaction_interval, 30_000)
  end

  @spec compaction_max_raw_age() :: pos_integer()
  def compaction_max_raw_age do
    Application.get_env(:timeless_logs, :compaction_max_raw_age, 60)
  end

  @spec zstd_compression_level() :: 1..22
  def zstd_compression_level do
    Application.get_env(:timeless_logs, :zstd_compression_level, 3)
  end

  @spec openzl_compression_level() :: 1..22
  def openzl_compression_level do
    Application.get_env(:timeless_logs, :openzl_compression_level, 9)
  end

  @spec compaction_format() :: :zstd | :openzl
  def compaction_format do
    Application.get_env(:timeless_logs, :compaction_format, :openzl)
  end

  @spec index_publish_interval() :: pos_integer()
  def index_publish_interval do
    Application.get_env(:timeless_logs, :index_publish_interval, 2_000)
  end

  @spec merge_compaction_target_size() :: pos_integer()
  def merge_compaction_target_size do
    Application.get_env(:timeless_logs, :merge_compaction_target_size, 2_000)
  end

  @spec merge_compaction_min_blocks() :: pos_integer()
  def merge_compaction_min_blocks do
    Application.get_env(:timeless_logs, :merge_compaction_min_blocks, 4)
  end

  # Queryable hot tail: recent entries served from memory. The lag is the
  # partition point between tail and disk (must comfortably exceed
  # flush_interval + index flush); window/max_entries bound memory.
  @spec hot_tail?() :: boolean()
  def hot_tail? do
    Application.get_env(:timeless_logs, :hot_tail, true)
  end

  @spec hot_tail_lag_ms() :: pos_integer()
  def hot_tail_lag_ms do
    Application.get_env(:timeless_logs, :hot_tail_lag_ms, 5_000)
  end

  @spec hot_tail_window_seconds() :: pos_integer()
  def hot_tail_window_seconds do
    Application.get_env(:timeless_logs, :hot_tail_window_seconds, 30)
  end

  @spec hot_tail_max_entries() :: pos_integer()
  def hot_tail_max_entries do
    Application.get_env(:timeless_logs, :hot_tail_max_entries, 250_000)
  end

  # Parallel block decompressions per query. Half the cores by default so
  # scan-heavy queries, the flush pipeline, and the compactor can't
  # mutually starve each other under sustained ingest.
  @spec query_concurrency() :: pos_integer()
  def query_concurrency do
    Application.get_env(
      :timeless_logs,
      :query_concurrency,
      max(div(System.schedulers_online(), 2), 1)
    )
  end

  # Compression level used when raw debt is past half the ingest limit:
  # trade a little ratio for much higher compaction throughput so the
  # backlog drains before backpressure has to engage.
  @spec compaction_pressure_level() :: 1..22
  def compaction_pressure_level do
    Application.get_env(:timeless_logs, :compaction_pressure_level, 3)
  end

  @spec compaction_max_backoff() :: pos_integer()
  def compaction_max_backoff do
    Application.get_env(:timeless_logs, :compaction_max_backoff, 300_000)
  end

  @spec max_term_index_entries() :: pos_integer() | nil
  def max_term_index_entries do
    Application.get_env(:timeless_logs, :max_term_index_entries, nil)
  end

  # Scales with cores (i185 audit: 8 shards beat 4 by ~11% on 22
  # schedulers), clamped so small hosts keep the old default and big
  # hosts don't multiply idle GenServers.
  @spec ingest_shard_count() :: pos_integer()
  def ingest_shard_count do
    default = System.schedulers_online() |> div(2) |> max(4) |> min(8)
    Application.get_env(:timeless_logs, :ingest_shard_count, default)
  end
end
