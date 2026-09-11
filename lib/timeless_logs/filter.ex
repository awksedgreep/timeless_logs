defmodule TimelessLogs.Filter do
  @moduledoc false

  @spec filter([map()], keyword()) :: [map()]
  def filter(entries, filters) do
    prepared = prepare(filters)
    Enum.filter(entries, &matches?(&1, prepared))
  end

  @doc false
  def prepare(filters) do
    Enum.map(filters, fn
      {:message, pattern} when is_binary(pattern) ->
        {:message_downcase, String.downcase(pattern)}

      {:metadata, map} ->
        {:metadata_prepared, prepare_metadata(map)}

      {:metadata_any, pairs} ->
        {:metadata_any_prepared, prepare_metadata(pairs)}

      prepared ->
        prepared
    end)
  end

  @spec matches?(map(), keyword()) :: boolean()
  def matches?(entry, filters) do
    Enum.all?(filters, fn
      {:level, level} ->
        entry.level == level

      # Message only. It used to also match any metadata value, which meant the
      # predicate could not be pushed into the storage engine — the engine
      # matches the message — so every search decoded the whole store. Metadata
      # is searched with :metadata / :metadata_any, which push down through the
      # indexed key columns.
      {:message, pattern} ->
        String.contains?(String.downcase(entry.message), String.downcase(pattern))

      {:message_downcase, pattern} ->
        String.contains?(String.downcase(entry.message), pattern)

      {:since, ts} ->
        TimelessLogs.Timestamp.to_microseconds(entry.timestamp) >= to_unix(ts)

      {:until, ts} ->
        TimelessLogs.Timestamp.to_microseconds(entry.timestamp) <= to_unix(ts)

      {:metadata, map} ->
        Enum.all?(map, fn {k, v} ->
          metadata_matches?(entry.metadata, {k, v, to_string(v)})
        end)

      {:metadata_prepared, pairs} ->
        Enum.all?(pairs, &metadata_matches?(entry.metadata, &1))

      {:metadata_any, pairs} ->
        Enum.any?(pairs, fn {k, v} ->
          metadata_matches?(entry.metadata, {k, v, to_string(v)})
        end)

      {:metadata_any_prepared, pairs} ->
        Enum.any?(pairs, &metadata_matches?(entry.metadata, &1))

      _ ->
        true
    end)
  end

  # Look up under both key shapes without creating atoms from
  # client-controlled filter keys (atoms are never GC'd).
  defp prepare_metadata(map_or_pairs) do
    Enum.map(map_or_pairs, fn {key, value} -> {key, value, to_string(value)} end)
  end

  defp metadata_matches?(metadata, {key, raw_value, string_value}) do
    case metadata_value(metadata, key) do
      nil -> false
      ^raw_value -> true
      actual -> to_string(actual) == string_value
    end
  end

  defp metadata_value(metadata, k) when is_atom(k) do
    case Map.fetch(metadata, k) do
      {:ok, value} -> value
      :error -> Map.get(metadata, Atom.to_string(k))
    end
  end

  defp metadata_value(metadata, k) when is_binary(k) do
    case Map.fetch(metadata, k) do
      {:ok, v} ->
        v

      :error ->
        try do
          Map.get(metadata, String.to_existing_atom(k))
        rescue
          ArgumentError -> nil
        end
    end
  end

  defp to_unix(ts), do: TimelessLogs.Timestamp.to_microseconds(ts)
end
