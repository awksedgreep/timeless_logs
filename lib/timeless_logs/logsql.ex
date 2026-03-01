defmodule TimelessLogs.LogsQL do
  @moduledoc """
  Parser for the subset of LogsQL that DDNet sends.

  Parses queries like:
      _time:1h level:error service:dhcp "search term" | sort by (_time) desc | limit 50 offset 0
      _time:[2024-01-01T00:00:00Z, 2024-01-02T00:00:00Z) level:warning
      _time:24h | stats count() as total
      *

  Returns `{:query, filters}` or `{:stats_count, filters}` where filters
  is a keyword list compatible with `TimelessLogs.query/1`.
  """

  @duration_units %{
    "s" => 1,
    "m" => 60,
    "h" => 3600,
    "d" => 86400
  }

  @spec parse(String.t()) :: {:query, keyword()} | {:stats_count, keyword()}
  def parse(query) when is_binary(query) do
    query = String.trim(query)

    {filter_part, pipes} = split_pipes(query)

    {pipe_opts, stats?} = parse_pipes(pipes)

    filter_opts = parse_filters(filter_part)

    filters = Keyword.merge(filter_opts, pipe_opts)

    if stats? do
      {:stats_count, filters}
    else
      {:query, filters}
    end
  end

  # Split on " | " to separate filter section from pipe commands
  defp split_pipes(query) do
    case String.split(query, " | ", parts: 2) do
      [filters, pipes] -> {String.trim(filters), String.split(pipes, " | ")}
      [filters] -> {String.trim(filters), []}
    end
  end

  defp parse_pipes(pipes) do
    Enum.reduce(pipes, {[], false}, fn pipe, {opts, stats?} ->
      pipe = String.trim(pipe)

      cond do
        String.starts_with?(pipe, "sort by") ->
          order = if String.ends_with?(pipe, "asc"), do: :asc, else: :desc
          {[{:order, order} | opts], stats?}

        String.starts_with?(pipe, "limit") ->
          case Integer.parse(String.trim_leading(pipe, "limit ")) do
            {n, _} -> {[{:limit, n} | opts], stats?}
            :error -> {opts, stats?}
          end

        String.starts_with?(pipe, "offset") ->
          case Integer.parse(String.trim_leading(pipe, "offset ")) do
            {n, _} -> {[{:offset, n} | opts], stats?}
            :error -> {opts, stats?}
          end

        String.starts_with?(pipe, "stats count(") ->
          {opts, true}

        true ->
          {opts, stats?}
      end
    end)
  end

  defp parse_filters("*"), do: []
  defp parse_filters(""), do: []

  defp parse_filters(filter_str) do
    filter_str
    |> tokenize()
    |> Enum.reduce([], fn token, acc ->
      parse_token(token, acc)
    end)
  end

  # Tokenize respecting quoted strings and bracket expressions
  defp tokenize(str) do
    tokenize(str, [], "")
  end

  defp tokenize("", tokens, current) do
    tokens ++ if current == "", do: [], else: [current]
  end

  # Bracket expression for time ranges: _time:[start, end)
  defp tokenize(<<"[", rest::binary>>, tokens, current) do
    {bracket_content, remaining} = consume_until_bracket_close(rest, "")
    tokenize(remaining, tokens, current <> "[" <> bracket_content)
  end

  # Quoted string as bare search term
  defp tokenize(<<"\"", rest::binary>>, tokens, "") do
    {quoted, remaining} = consume_quoted(rest, "")
    tokenize(remaining, tokens ++ ["\"" <> quoted <> "\""], "")
  end

  # Quoted value after field:
  defp tokenize(<<"\"", rest::binary>>, tokens, current) do
    {quoted, remaining} = consume_quoted(rest, "")
    tokenize(remaining, tokens, current <> "\"" <> quoted <> "\"")
  end

  # Space separates tokens
  defp tokenize(<<" ", rest::binary>>, tokens, current) do
    tokens = if current == "", do: tokens, else: tokens ++ [current]
    tokenize(rest, tokens, "")
  end

  defp tokenize(<<c::utf8, rest::binary>>, tokens, current) do
    tokenize(rest, tokens, current <> <<c::utf8>>)
  end

  defp consume_quoted("", acc), do: {acc, ""}
  defp consume_quoted(<<"\\\"", rest::binary>>, acc), do: consume_quoted(rest, acc <> "\"")
  defp consume_quoted(<<"\"", rest::binary>>, acc), do: {acc, rest}

  defp consume_quoted(<<c::utf8, rest::binary>>, acc),
    do: consume_quoted(rest, acc <> <<c::utf8>>)

  defp consume_until_bracket_close("", acc), do: {acc, ""}

  defp consume_until_bracket_close(<<")", rest::binary>>, acc),
    do: {acc <> ")", rest}

  defp consume_until_bracket_close(<<"]", rest::binary>>, acc),
    do: {acc <> "]", rest}

  defp consume_until_bracket_close(<<c::utf8, rest::binary>>, acc),
    do: consume_until_bracket_close(rest, acc <> <<c::utf8>>)

  # Parse individual tokens into filter opts
  defp parse_token("_time:" <> value, acc) do
    parse_time_filter(value, acc)
  end

  defp parse_token("level:" <> value, acc) do
    level = value |> unquote_value() |> String.to_existing_atom()
    [{:level, level} | acc]
  end

  # Bare quoted string → message search
  defp parse_token("\"" <> _ = token, acc) do
    msg = token |> String.trim("\"")
    [{:message, msg} | acc]
  end

  # Other field:value → metadata
  defp parse_token(token, acc) do
    case String.split(token, ":", parts: 2) do
      [field, value] ->
        meta = Keyword.get(acc, :metadata, %{})
        key = String.to_atom(field)
        val = unquote_value(value)
        Keyword.put(acc, :metadata, Map.put(meta, key, val))

      _ ->
        acc
    end
  end

  # Time filter parsing
  defp parse_time_filter("[" <> _ = value, acc) do
    # Range: [start, end) or [start, end]
    inner =
      value
      |> String.trim_leading("[")
      |> String.trim_trailing(")")
      |> String.trim_trailing("]")

    case String.split(inner, ",", parts: 2) do
      [start_str, end_str] ->
        since = parse_iso_to_microseconds(String.trim(start_str))
        until_ts = parse_iso_to_microseconds(String.trim(end_str))

        acc = if since, do: [{:since, since} | acc], else: acc
        if until_ts, do: [{:until, until_ts} | acc], else: acc

      _ ->
        acc
    end
  end

  defp parse_time_filter(">=" <> ts, acc) do
    case parse_iso_to_microseconds(ts) do
      nil -> acc
      since -> [{:since, since} | acc]
    end
  end

  defp parse_time_filter("<" <> ts, acc) do
    case parse_iso_to_microseconds(ts) do
      nil -> acc
      until_ts -> [{:until, until_ts} | acc]
    end
  end

  # Duration: 15m, 1h, 6h, 24h, 7d, 30d
  defp parse_time_filter(duration, acc) do
    case parse_duration(duration) do
      nil -> acc
      seconds -> [{:since, System.os_time(:microsecond) - seconds * 1_000_000} | acc]
    end
  end

  defp parse_duration(str) do
    case Integer.parse(str) do
      {n, unit} ->
        case Map.get(@duration_units, unit) do
          nil -> nil
          multiplier -> n * multiplier
        end

      :error ->
        nil
    end
  end

  defp parse_iso_to_microseconds(str) do
    case DateTime.from_iso8601(str) do
      {:ok, dt, _offset} -> DateTime.to_unix(dt, :microsecond)
      _ -> nil
    end
  end

  defp unquote_value("\"" <> rest), do: String.trim_trailing(rest, "\"")
  defp unquote_value(value), do: value
end
