defmodule TimelessLogs.BufferShard do
  @moduledoc false

  @names_key {__MODULE__, :names}
  @message_prefix_bytes 128

  def count do
    TimelessLogs.Config.ingest_shard_count()
  end

  def via(entry) do
    entry |> shard_for() |> name()
  end

  def name(shard) when is_integer(shard) do
    names = names()
    elem(names, shard)
  end

  @doc false
  def install_names(shard_count \\ count()) do
    names =
      0..(shard_count - 1)
      |> Enum.map(&String.to_atom("timeless_logs_buffer_#{&1}"))
      |> List.to_tuple()

    :persistent_term.put(@names_key, names)
    :ok
  end

  def shard_for(entry) do
    rem(:erlang.phash2(shard_key(entry)), count())
  end

  defp shard_key(%{metadata: metadata} = entry) when is_map(metadata) do
    Map.get(metadata, "request_id") ||
      Map.get(metadata, "trace_id") ||
      Map.get(metadata, "service") ||
      bounded_message_key(entry.message)
  end

  defp shard_key(entry), do: entry

  defp bounded_message_key(message) when is_binary(message) do
    prefix_size = min(byte_size(message), @message_prefix_bytes)
    {byte_size(message), binary_part(message, 0, prefix_size)}
  end

  defp names do
    shard_count = count()

    case :persistent_term.get(@names_key, nil) do
      names when is_tuple(names) and tuple_size(names) == shard_count ->
        names

      _ ->
        install_names(shard_count)
        :persistent_term.get(@names_key)
    end
  end
end
