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

      {:message, pattern} ->
        downcased = String.downcase(pattern)

        String.contains?(String.downcase(entry.message), downcased) or
          Enum.any?(entry.metadata, fn {_k, v} ->
            is_binary(v) and String.contains?(String.downcase(v), downcased)
          end)

      {:since, ts} ->
        entry.timestamp >= to_unix(ts)

      {:until, ts} ->
        entry.timestamp <= to_unix(ts)

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
