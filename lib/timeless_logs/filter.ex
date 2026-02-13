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
          Map.get(entry.metadata, to_string(k)) == to_string(v)
        end)

      _ ->
        true
    end)
  end

  defp to_unix(%DateTime{} = dt), do: DateTime.to_unix(dt)
  defp to_unix(ts) when is_integer(ts), do: ts
end
