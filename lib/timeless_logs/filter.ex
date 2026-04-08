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
          str_key = to_string(k)
          atom_key = if is_atom(k), do: k, else: String.to_atom(str_key)
          val = Map.get(entry.metadata, atom_key) || Map.get(entry.metadata, str_key)
          to_string(val) == to_string(v)
        end)

      {:metadata_any, pairs} ->
        Enum.any?(pairs, fn {k, v} ->
          str_key = to_string(k)
          atom_key = if is_atom(k), do: k, else: String.to_atom(str_key)
          val = Map.get(entry.metadata, atom_key) || Map.get(entry.metadata, str_key)
          to_string(val) == to_string(v)
        end)

      _ ->
        true
    end)
  end

  defp to_unix(%DateTime{} = dt), do: DateTime.to_unix(dt)
  defp to_unix(ts) when is_integer(ts), do: ts
end
