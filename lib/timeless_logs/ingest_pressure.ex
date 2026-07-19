defmodule TimelessLogs.IngestPressure do
  @moduledoc false

  # Shared gauges for ingest backpressure, readable without touching any
  # GenServer: one slot per buffer shard holding its queued entry count
  # (buffer + pending batches + in-flight flushes), plus one slot for the
  # compactor's raw-block debt in bytes.
  #
  # Below the watermark, ingest is a cast (bursts absorbed at full speed).
  # At or above it — or when raw debt is past its limit — batch ingest
  # switches to a call, so producers pace-match the durable drain rate
  # instead of converting overload into unbounded memory. Nothing is
  # dropped or refused.

  @key {__MODULE__, :gauges}

  @spec install(pos_integer()) :: :ok
  def install(shard_count) do
    ref = :atomics.new(shard_count + 1, [])
    :persistent_term.put(@key, {ref, shard_count})
    :ok
  end

  @spec set_queued(non_neg_integer(), non_neg_integer()) :: :ok
  def set_queued(shard, n) do
    {ref, _count} = :persistent_term.get(@key)
    :atomics.put(ref, shard + 1, n)
  end

  @spec queued(non_neg_integer()) :: non_neg_integer()
  def queued(shard) do
    {ref, _count} = :persistent_term.get(@key)
    :atomics.get(ref, shard + 1)
  end

  @spec set_raw_debt(non_neg_integer()) :: :ok
  def set_raw_debt(bytes) do
    {ref, count} = :persistent_term.get(@key)
    :atomics.put(ref, count + 1, bytes)
  end

  @spec raw_debt() :: non_neg_integer()
  def raw_debt do
    {ref, count} = :persistent_term.get(@key)
    :atomics.get(ref, count + 1)
  end

  @spec overloaded?(non_neg_integer()) :: boolean()
  def overloaded?(shard) do
    queued(shard) >= TimelessLogs.Config.ingest_soft_watermark() or
      raw_debt() >= TimelessLogs.Config.ingest_raw_debt_limit()
  end
end
