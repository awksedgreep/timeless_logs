defmodule TimelessLogs.Filter do
  @moduledoc false

  @spec filter([map()], keyword()) :: [map()]
  def filter(entries, filters) do
    Enum.filter(entries, &matches?(&1, filters))
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

      {:since, ts} ->
        TimelessLogs.Timestamp.to_microseconds(entry.timestamp) >= to_unix(ts)

      {:until, ts} ->
        TimelessLogs.Timestamp.to_microseconds(entry.timestamp) <= to_unix(ts)

      {:metadata, map} ->
        Enum.all?(map, fn {k, v} ->
          to_string(metadata_value(entry.metadata, k)) == to_string(v)
        end)

      {:metadata_any, pairs} ->
        Enum.any?(pairs, fn {k, v} ->
          to_string(metadata_value(entry.metadata, k)) == to_string(v)
        end)

      _ ->
        true
    end)
  end

  # Look up under both key shapes without creating atoms from
  # client-controlled filter keys (atoms are never GC'd).
  defp metadata_value(metadata, k) when is_atom(k) do
    Map.get(metadata, k) || Map.get(metadata, Atom.to_string(k))
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
