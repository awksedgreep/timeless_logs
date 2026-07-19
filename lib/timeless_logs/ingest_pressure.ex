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

  # The gauge is a producer-side counter, incremented before the message
  # is sent and decremented when entries reach disk (or are dropped).
  # Counting at the send point is what bounds the shard MAILBOX — a
  # server-side mirror of processed-but-unflushed entries misses casts
  # queued in the mailbox, which is exactly where overload accumulates.
  @spec add(non_neg_integer(), pos_integer()) :: :ok
  def add(shard, n) do
    {ref, _count} = :persistent_term.get(@key)
    :atomics.add(ref, shard + 1, n)
  end

  @spec sub(non_neg_integer(), pos_integer()) :: :ok
  def sub(shard, n) do
    {ref, _count} = :persistent_term.get(@key)
    :atomics.sub(ref, shard + 1, n)
  end

  @spec reset(non_neg_integer()) :: :ok
  def reset(shard) do
    {ref, _count} = :persistent_term.get(@key)
    :atomics.put(ref, shard + 1, 0)
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

  @spec any_overloaded?() :: boolean()
  def any_overloaded? do
    {_ref, count} = :persistent_term.get(@key)
    Enum.any?(0..(count - 1), &overloaded?/1)
  end
end
