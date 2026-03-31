defmodule TimelessLogs.BufferShard do
  @moduledoc false

  def count do
    TimelessLogs.Config.ingest_shard_count()
  end

  def via(entry) do
    shard = shard_for(entry)
    {:global, {:timeless_logs_buffer, shard}}
  end

  def shard_for(entry) do
    rem(:erlang.phash2(shard_key(entry)), count())
  end

  defp shard_key(%{metadata: metadata} = entry) when is_map(metadata) do
    Map.get(metadata, "request_id") ||
      Map.get(metadata, "trace_id") ||
      Map.get(metadata, "service") ||
      entry.message
  end

  defp shard_key(entry), do: entry
end
