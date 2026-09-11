defmodule TimelessLogs.LogsQL do
  @moduledoc """
  Parser for the subset of LogsQL that DDNet sends.

  Parses queries like:
      _time:1h level:error service:dhcp "search term" | sort by (_time) desc | limit 50 offset 0
      _time:[2024-01-01T00:00:00Z, 2024-01-02T00:00:00Z) level:warning
      _time:24h | stats count() as total
      *

  Returns `{:query, filters}`, `{:stats_count, filters}`, or
  `{:stats_group, filters, aggregate}` where filters is a keyword list
  compatible with `TimelessLogs.query/1`.
  """

  @duration_units %{
    "s" => 1,
    "m" => 60,
    "h" => 3600,
    "d" => 86400
  }

  @levels %{
    "debug" => :debug,
    "info" => :info,
    "notice" => :notice,
    "warning" => :warning,
    "warn" => :warning,
    "error" => :error,
    "critical" => :critical,
    "alert" => :alert,
    "emergency" => :emergency
  }

  @type parse_error :: {:invalid_query, String.t()} | {:unsupported_capability, String.t()}

  @spec parse(String.t()) ::
          {:query, keyword()}
          | {:stats_count, keyword()}
          | {:stats_group, keyword(), map()}
          | {:error, parse_error()}
  def parse(query) when is_binary(query) do
    query = String.trim(query)
    {filter_part, pipes} = split_pipes(query)

    with {:ok, filter_opts} <- parse_filters(filter_part),
         {:ok, pipe_opts, command} <- parse_pipes(pipes) do
      filters = Keyword.merge(filter_opts, pipe_opts)

      case command do
        :query -> {:query, filters}
        :stats_count -> {:stats_count, filters}
        {:stats_group, aggregate} -> {:stats_group, filters, aggregate}
      end
    end
  end

  # Split on a pipe with optional surrounding whitespace, but never on a
  # literal pipe inside a quoted value or a time-range bracket.
  defp split_pipes(query) do
    case split_pipe_parts(query) do
      [filters | pipes] -> {String.trim(filters), Enum.map(pipes, &String.trim/1)}
      [] -> {"", []}
    end
  end

  defp split_pipe_parts(query) do
    {parts, current, _quoted, _escaped, _bracket_depth} =
      query
      |> String.graphemes()
      |> Enum.reduce({[], [], false, false, 0}, fn char,
                                                   {parts, current, quoted, escaped, depth} ->
        cond do
          escaped ->
            {parts, [char | current], quoted, false, depth}

          quoted and char == "\\" ->
            {parts, [char | current], quoted, true, depth}

          char == "\"" ->
            {parts, [char | current], not quoted, false, depth}

          not quoted and char == "[" ->
            {parts, [char | current], quoted, false, depth + 1}

          not quoted and depth > 0 and char in ["]", ")"] ->
            {parts, [char | current], quoted, false, depth - 1}

          not quoted and depth == 0 and char == "|" ->
            {[current |> Enum.reverse() |> Enum.join() | parts], [], quoted, false, depth}

          true ->
            {parts, [char | current], quoted, false, depth}
        end
      end)

    [current |> Enum.reverse() |> Enum.join() | parts]
    |> Enum.reverse()
  end

  defp parse_pipes(pipes) do
    initial = %{opts: [], stats: nil, extracts: []}

    with {:ok, state} <- Enum.reduce_while(pipes, {:ok, initial}, &parse_pipe/2) do
      command =
        case state.stats do
          nil ->
            :query

          %{group_by: nil} ->
            :stats_count

          stats ->
            {:stats_group, Map.put(stats, :extracts, Enum.reverse(state.extracts))}
        end

      {:ok, Enum.reverse(state.opts), command}
    end
  end

  defp parse_pipe(pipe, {:ok, state}) do
    pipe = String.trim(pipe)

    cond do
      captures = Regex.run(~r/^sort\s+by\s+\(_time\)\s+(asc|desc)$/i, pipe) ->
        order = captures |> Enum.at(1) |> String.downcase() |> String.to_atom()
        {:cont, {:ok, %{state | opts: [{:order, order} | state.opts]}}}

      captures = Regex.run(~r/^limit\s+(\d+)$/i, pipe) ->
        {:cont, {:ok, put_pipe_integer(state, :limit, Enum.at(captures, 1))}}

      captures = Regex.run(~r/^offset\s+(\d+)$/i, pipe) ->
        {:cont, {:ok, put_pipe_integer(state, :offset, Enum.at(captures, 1))}}

      Regex.match?(~r/^stats\s+count\(\)(?:\s+as\s+[A-Za-z_][\w]*)?$/i, pipe) ->
        {:cont, {:ok, %{state | stats: %{group_by: nil, as: stats_alias(pipe)}}}}

      captures =
          Regex.run(
            ~r/^stats\s+by\s+\(([A-Za-z_][\w.-]*)\)\s+count\(\)(?:\s+as\s+([A-Za-z_][\w]*))?$/i,
            pipe
          ) ->
        aggregate = %{group_by: Enum.at(captures, 1), as: Enum.at(captures, 2) || "count"}
        {:cont, {:ok, %{state | stats: aggregate}}}

      captures =
          Regex.run(
            ~r/^extract\s+"((?:\\.|[^"])*)"\s+from\s+([A-Za-z_][\w.-]*)$/i,
            pipe
          ) ->
        case build_extract(Enum.at(captures, 1), Enum.at(captures, 2)) do
          {:ok, extract} ->
            {:cont, {:ok, %{state | extracts: [extract | state.extracts]}}}

          {:error, message} ->
            {:halt, {:error, {:invalid_query, message}}}
        end

      true ->
        {:halt, {:error, {:unsupported_capability, "unsupported LogsQL pipe #{inspect(pipe)}"}}}
    end
  end

  defp put_pipe_integer(state, key, value) do
    {integer, ""} = Integer.parse(value)
    %{state | opts: [{key, integer} | state.opts]}
  end

  defp stats_alias(pipe) do
    case Regex.run(~r/\s+as\s+([A-Za-z_][\w]*)$/i, pipe) do
      [_, name] -> name
      _ -> "total"
    end
  end

  defp parse_filters("*"), do: {:ok, []}
  defp parse_filters(""), do: {:ok, []}

  defp parse_filters(filter_str) do
    filter_str
    |> tokenize()
    |> Enum.reduce_while({:ok, []}, fn token, {:ok, acc} ->
      case parse_token(token, acc) do
        {:ok, next} -> {:cont, {:ok, next}}
        {:error, _} = error -> {:halt, error}
      end
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
    {:ok, parse_time_filter(value, acc)}
  end

  defp parse_token("level:" <> value, acc) do
    original = unquote_value(value)

    case Map.fetch(@levels, String.downcase(original)) do
      {:ok, level} -> {:ok, [{:level, level} | acc]}
      :error -> {:error, {:invalid_query, "unknown log level #{inspect(original)}"}}
    end
  end

  # Bare quoted string → message search
  defp parse_token("\"" <> _ = token, acc) do
    msg = token |> String.trim("\"")
    {:ok, [{:message, msg} | acc]}
  end

  # Other field:value → metadata
  defp parse_token(token, acc) do
    cond do
      String.downcase(token) in ["and", "or", "not"] ->
        operator = String.downcase(token)

        {:error,
         {:unsupported_capability,
          "LogsQL logical operator #{inspect(operator)} is not implemented yet"}}

      token == "*" ->
        {:ok, acc}

      true ->
        parse_field_or_message(token, acc)
    end
  end

  defp parse_field_or_message(token, acc) do
    case String.split(token, ":", parts: 2) do
      [field, value] when field != "" and value != "" ->
        # String keys: filter and term lookup handle both shapes, and
        # query fields are client-controlled (no atom creation).
        meta = Keyword.get(acc, :metadata, %{})
        val = unquote_value(value)
        {:ok, Keyword.put(acc, :metadata, Map.put(meta, field, val))}

      _ ->
        # VictoriaLogs accepts unquoted words as message terms. Keeping each
        # as a separate keyword entry gives the existing Filter its AND
        # semantics without silently broadening the query.
        {:ok, [{:message, token} | acc]}
    end
  end

  @doc false
  def aggregate_grouped(entries, %{group_by: group_by, as: as, extracts: extracts}) do
    entries
    |> Enum.reduce(%{}, fn entry, counts ->
      extracted = apply_extracts(entry, extracts)
      value = Map.get(extracted, group_by) || entry_field(entry, group_by)

      if is_nil(value), do: counts, else: Map.update(counts, value, 1, &(&1 + 1))
    end)
    |> Enum.map(fn {value, count} -> %{group_by => value, as => count} end)
    |> Enum.sort_by(&to_string(Map.fetch!(&1, group_by)))
  end

  defp apply_extracts(entry, extracts) do
    Enum.reduce(extracts, %{}, fn %{source: source, regex: regex}, fields ->
      case entry_field(entry, source) do
        value when is_binary(value) ->
          Map.merge(fields, Regex.named_captures(regex, value) || %{})

        _ ->
          fields
      end
    end)
  end

  defp entry_field(entry, "_msg"), do: entry.message
  defp entry_field(entry, "_time"), do: entry.timestamp
  defp entry_field(entry, "level"), do: entry.level

  defp entry_field(entry, field) do
    metadata = Map.get(entry, :metadata, %{})

    case Map.fetch(metadata, field) do
      {:ok, value} ->
        value

      :error ->
        try do
          Map.get(metadata, String.to_existing_atom(field))
        rescue
          ArgumentError -> nil
        end
    end
  end

  defp build_extract(pattern, source) do
    pattern = String.replace(pattern, "\\\"", "\"")
    {regex_source, fields} = extract_regex(pattern, [], [])

    if fields == [] do
      {:error, "extract pattern must contain at least one <field> capture"}
    else
      case Regex.compile(IO.iodata_to_binary(regex_source), "u") do
        {:ok, regex} -> {:ok, %{source: source, regex: regex}}
        {:error, reason} -> {:error, "invalid extract pattern: #{inspect(reason)}"}
      end
    end
  end

  defp extract_regex(pattern, source, fields) do
    case Regex.run(~r/<([A-Za-z_][A-Za-z0-9_]*)>/, pattern, return: :index) do
      [{start, length}, {field_start, field_length}] ->
        literal = binary_part(pattern, 0, start)
        field = binary_part(pattern, field_start, field_length)
        rest_start = start + length
        rest = binary_part(pattern, rest_start, byte_size(pattern) - rest_start)

        extract_regex(
          rest,
          [source, Regex.escape(literal), "(?<", field, ">.+?)"],
          [field | fields]
        )

      nil ->
        {[source, Regex.escape(pattern)], fields}
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
