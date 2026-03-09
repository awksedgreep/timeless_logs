defmodule TimelessLogs.VLogsCompatTest do
  @moduledoc """
  Compatibility tests that verify TimelessLogs HTTP responses are parseable
  by DDNet's VLogsClient. These tests replicate DDNet's actual parsing logic
  against our response bodies to catch wire-format mismatches.
  """
  use ExUnit.Case, async: false

  @data_dir "test/tmp/vlogs_compat_test"
  @port 19_429

  setup do
    Application.stop(:timeless_logs)
    File.rm_rf!(@data_dir)
    Application.put_env(:timeless_logs, :data_dir, @data_dir)
    Application.put_env(:timeless_logs, :flush_interval, 60_000)
    Application.put_env(:timeless_logs, :max_buffer_size, 10_000)
    Application.put_env(:timeless_logs, :http, false)
    Application.ensure_all_started(:timeless_logs)

    :persistent_term.put({TimelessLogs.HTTP, :bearer_token}, nil)

    # Remove logger handler so Rocket startup logs don't pollute the buffer
    :logger.remove_handler(TimelessLogs.Handler.handler_id())

    start_supervised!({TimelessLogs.HTTP, port: @port})

    on_exit(fn ->
      Application.stop(:timeless_logs)
      File.rm_rf!(@data_dir)
    end)

    :ok
  end

  defp post(path, body, opts) do
    TimelessLogs.TestHTTP.post(@port, path, body, opts)
  end

  defp ingest_realistic_data do
    lines =
      Enum.join(
        [
          ~s({"_msg":"GET /api/v1/users","level":"info","service":"api","app":"myapp","node":"node1@host","request_id":"abc123"}),
          ~s({"_msg":"connection timeout","level":"error","service":"dhcp","app":"myapp","node":"node1@host","correlation_id":"corr456"}),
          ~s({"_msg":"POST /api/v1/sessions","level":"info","service":"web","app":"frontend","node":"node2@host","request_id":"def789"}),
          ~s({"_msg":"cache miss for key user:42","level":"warning","service":"api","app":"myapp","node":"node1@host"}),
          ~s({"_msg":"scheduled job completed","level":"info","service":"worker","app":"myapp","node":"node3@host","custom_field":"extra_data"})
        ],
        "\n"
      )

    post("/insert/jsonline", lines, content_type: "application/x-ndjson")

    TimelessLogs.flush()
  end

  # ===========================================================================
  # DDNet VLogsClient parsing functions (replicated exactly)
  # These are the actual parsers from ddnet/lib/ddnet/vlogs_client.ex
  # ===========================================================================

  defp ddnet_parse_ndjson(body) when is_binary(body) do
    body
    |> String.split("\n", trim: true)
    |> Enum.map(fn line ->
      case :json.decode(line) do
        map when is_map(map) -> ddnet_normalize_log(map)
        _ -> nil
      end
    end)
    |> Enum.reject(&is_nil/1)
  end

  defp ddnet_normalize_log(log) when is_map(log) do
    %{
      id: ddnet_generate_id(log),
      inserted_at: ddnet_parse_timestamp(Map.get(log, "_time")),
      level: Map.get(log, "level"),
      message: Map.get(log, "_msg"),
      service: Map.get(log, "service"),
      app: Map.get(log, "app"),
      node: Map.get(log, "node"),
      request_id: Map.get(log, "request_id"),
      correlation_id: Map.get(log, "correlation_id"),
      meta: ddnet_extract_meta(log)
    }
  end

  defp ddnet_generate_id(log) do
    time = Map.get(log, "_time", "")
    msg = Map.get(log, "_msg", "")
    :erlang.phash2({time, msg}) |> Integer.to_string()
  end

  defp ddnet_parse_timestamp(nil), do: nil

  defp ddnet_parse_timestamp(timestamp) when is_binary(timestamp) do
    case DateTime.from_iso8601(timestamp) do
      {:ok, dt, _} -> dt
      _ -> nil
    end
  end

  defp ddnet_parse_timestamp(_), do: nil

  defp ddnet_extract_meta(log) do
    standard_fields =
      ~w(_time _msg level service app node request_id correlation_id _stream _stream_id)

    log
    |> Map.drop(standard_fields)
    |> case do
      meta when map_size(meta) > 0 -> meta
      _ -> nil
    end
  end

  defp ddnet_parse_count(body) do
    case :json.decode(String.trim(body)) do
      %{"total" => total} when is_integer(total) -> total
      %{"total" => total} when is_binary(total) -> String.to_integer(total)
      _ -> 0
    end
  end

  defp ddnet_parse_field_values(body) when is_binary(body) do
    case :json.decode(String.trim(body)) do
      %{"values" => values} when is_list(values) ->
        values
        |> Enum.map(fn
          %{"value" => value} when is_binary(value) -> value
          %{"value" => value} when is_integer(value) -> Integer.to_string(value)
          _ -> nil
        end)
        |> Enum.reject(&is_nil/1)
        |> Enum.uniq()
        |> Enum.sort()

      _ ->
        []
    end
  end

  defp ddnet_parse_field_names(body) when is_binary(body) do
    case :json.decode(String.trim(body)) do
      %{"values" => values} when is_list(values) ->
        values
        |> Enum.map(fn
          %{"value" => value} when is_binary(value) -> value
          _ -> nil
        end)
        |> Enum.reject(&is_nil/1)
        |> Enum.uniq()
        |> Enum.sort()

      _ ->
        []
    end
  end

  # ===========================================================================
  # Compatibility Tests
  # ===========================================================================

  describe "POST /select/logsql/query NDJSON response compatibility" do
    setup do
      ingest_realistic_data()
      :ok
    end

    test "response is parseable by DDNet's NDJSON parser" do
      resp = post("/select/logsql/query", "query=*",
        content_type: "application/x-www-form-urlencoded")

      assert resp.status == 200
      logs = ddnet_parse_ndjson(resp.body)
      assert length(logs) == 5
    end

    test "each log has required fields for DDNet" do
      resp = post("/select/logsql/query", "query=*",
        content_type: "application/x-www-form-urlencoded")

      logs = ddnet_parse_ndjson(resp.body)

      for log <- logs do
        assert is_binary(log.id), "id must be a string"
        assert %DateTime{} = log.inserted_at, "_time must parse to DateTime"
        assert is_binary(log.level), "level must be a string"
        assert is_binary(log.message), "_msg must be a string"
      end
    end

    test "metadata fields propagate correctly through DDNet parser" do
      resp = post("/select/logsql/query", "query=*",
        content_type: "application/x-www-form-urlencoded")

      logs = ddnet_parse_ndjson(resp.body)

      # Find the log with service=api, request_id=abc123
      api_log = Enum.find(logs, &(&1.message == "GET /api/v1/users"))
      assert api_log != nil
      assert api_log.service == "api"
      assert api_log.app == "myapp"
      assert api_log.node == "node1@host"
      assert api_log.request_id == "abc123"

      # Find the log with correlation_id
      dhcp_log = Enum.find(logs, &(&1.message == "connection timeout"))
      assert dhcp_log != nil
      assert dhcp_log.correlation_id == "corr456"
      assert dhcp_log.service == "dhcp"
    end

    test "custom metadata ends up in DDNet meta field" do
      resp = post("/select/logsql/query", "query=*",
        content_type: "application/x-www-form-urlencoded")

      logs = ddnet_parse_ndjson(resp.body)

      worker_log = Enum.find(logs, &(&1.message == "scheduled job completed"))
      assert worker_log != nil
      assert worker_log.meta != nil
      assert worker_log.meta["custom_field"] == "extra_data"
    end

    test "level filter produces correct subset" do
      query = URI.encode_query(%{"query" => "level:error"})

      resp = post("/select/logsql/query", query,
        content_type: "application/x-www-form-urlencoded")

      logs = ddnet_parse_ndjson(resp.body)
      assert length(logs) == 1
      assert hd(logs).level == "error"
      assert hd(logs).message == "connection timeout"
    end

    test "sort and limit pipes work with DDNet parser" do
      query = URI.encode_query(%{"query" => "* | sort by (_time) desc | limit 2"})

      resp = post("/select/logsql/query", query,
        content_type: "application/x-www-form-urlencoded")

      logs = ddnet_parse_ndjson(resp.body)
      assert length(logs) == 2

      # Verify descending order
      [first, second] = logs
      assert DateTime.compare(first.inserted_at, second.inserted_at) in [:gt, :eq]
    end

    test "empty result set returns parseable empty body" do
      query = URI.encode_query(%{"query" => "level:debug"})

      resp = post("/select/logsql/query", query,
        content_type: "application/x-www-form-urlencoded")

      assert resp.status == 200
      logs = ddnet_parse_ndjson(resp.body)
      assert logs == []
    end
  end

  describe "POST /select/logsql/query stats count compatibility" do
    setup do
      ingest_realistic_data()
      :ok
    end

    test "stats count response is parseable by DDNet" do
      query = URI.encode_query(%{"query" => "* | stats count() as total"})

      resp = post("/select/logsql/query", query,
        content_type: "application/x-www-form-urlencoded")

      assert resp.status == 200
      count = ddnet_parse_count(resp.body)
      assert count == 5
    end

    test "filtered stats count returns correct total" do
      query = URI.encode_query(%{"query" => "level:info | stats count() as total"})

      resp = post("/select/logsql/query", query,
        content_type: "application/x-www-form-urlencoded")

      count = ddnet_parse_count(resp.body)
      assert count == 3
    end

    test "stats count with no matches returns 0" do
      query = URI.encode_query(%{"query" => "level:debug | stats count() as total"})

      resp = post("/select/logsql/query", query,
        content_type: "application/x-www-form-urlencoded")

      count = ddnet_parse_count(resp.body)
      assert count == 0
    end
  end

  describe "POST /select/logsql/field_values compatibility" do
    setup do
      ingest_realistic_data()
      :ok
    end

    test "level field_values parseable by DDNet" do
      resp = post("/select/logsql/field_values?field=level", "query=*",
        content_type: "application/x-www-form-urlencoded")

      assert resp.status == 200
      values = ddnet_parse_field_values(resp.body)
      assert "info" in values
      assert "error" in values
      assert "warning" in values
    end

    test "service field_values parseable by DDNet" do
      resp = post("/select/logsql/field_values?field=service", "query=*",
        content_type: "application/x-www-form-urlencoded")

      values = ddnet_parse_field_values(resp.body)
      assert "api" in values
      assert "dhcp" in values
      assert "web" in values
      assert "worker" in values
    end

    test "app field_values parseable by DDNet" do
      resp = post("/select/logsql/field_values?field=app", "query=*",
        content_type: "application/x-www-form-urlencoded")

      values = ddnet_parse_field_values(resp.body)
      assert "myapp" in values
      assert "frontend" in values
    end

    test "node field_values parseable by DDNet" do
      resp = post("/select/logsql/field_values?field=node", "query=*",
        content_type: "application/x-www-form-urlencoded")

      values = ddnet_parse_field_values(resp.body)
      assert "node1@host" in values
      assert "node2@host" in values
      assert "node3@host" in values
    end

    test "field with no values returns empty list" do
      resp = post("/select/logsql/field_values?field=nonexistent", "query=*",
        content_type: "application/x-www-form-urlencoded")

      values = ddnet_parse_field_values(resp.body)
      assert values == []
    end
  end

  describe "POST /select/logsql/field_names compatibility" do
    setup do
      ingest_realistic_data()
      :ok
    end

    test "field_names parseable by DDNet and includes standard fields" do
      resp = post("/select/logsql/field_names", "query=*",
        content_type: "application/x-www-form-urlencoded")

      assert resp.status == 200
      names = ddnet_parse_field_names(resp.body)

      # Standard fields always present
      assert "_msg" in names
      assert "_time" in names
      assert "level" in names
    end

    test "field_names includes metadata keys" do
      resp = post("/select/logsql/field_names", "query=*",
        content_type: "application/x-www-form-urlencoded")

      names = ddnet_parse_field_names(resp.body)

      assert "service" in names
      assert "app" in names
      assert "node" in names
      assert "request_id" in names
      assert "correlation_id" in names
      assert "custom_field" in names
    end
  end

  # Tests that verify our parser handles the exact query strings DDNet builds.
  # These replicate DDNet.VLogsClient.build_query/build_base_query logic.
  describe "LogsQL query building compatibility" do
    setup do
      ingest_realistic_data()
      :ok
    end

    test "DDNet default query: _time:1h | sort by (_time) desc | limit 50" do
      query = URI.encode_query(%{"query" => "_time:1h | sort by (_time) desc | limit 50"})

      resp = post("/select/logsql/query", query,
        content_type: "application/x-www-form-urlencoded")

      assert resp.status == 200
      logs = ddnet_parse_ndjson(resp.body)
      # All 5 logs were just ingested, within 1h
      assert length(logs) == 5
    end

    test "DDNet query with level filter: _time:1h level:error | sort by (_time) desc | limit 50" do
      query =
        URI.encode_query(%{
          "query" => "_time:1h level:error | sort by (_time) desc | limit 50"
        })

      resp = post("/select/logsql/query", query,
        content_type: "application/x-www-form-urlencoded")

      logs = ddnet_parse_ndjson(resp.body)
      assert length(logs) == 1
      assert hd(logs).level == "error"
    end

    test "DDNet query with service filter: _time:1h service:api | sort by (_time) desc | limit 50" do
      query =
        URI.encode_query(%{
          "query" => "_time:1h service:api | sort by (_time) desc | limit 50"
        })

      resp = post("/select/logsql/query", query,
        content_type: "application/x-www-form-urlencoded")

      logs = ddnet_parse_ndjson(resp.body)
      assert length(logs) == 2
      assert Enum.all?(logs, &(&1.service == "api"))
    end

    test "DDNet query with offset: _time:1h | sort by (_time) desc | offset 2 | limit 50" do
      query =
        URI.encode_query(%{
          "query" => "_time:1h | sort by (_time) desc | offset 2 | limit 50"
        })

      resp = post("/select/logsql/query", query,
        content_type: "application/x-www-form-urlencoded")

      logs = ddnet_parse_ndjson(resp.body)
      assert length(logs) == 3
    end

    test "DDNet count query: _time:1h level:info | stats count() as total" do
      query =
        URI.encode_query(%{
          "query" => "_time:1h level:info | stats count() as total"
        })

      resp = post("/select/logsql/query", query,
        content_type: "application/x-www-form-urlencoded")

      count = ddnet_parse_count(resp.body)
      assert count == 3
    end

    test "DDNet field_values with time filter" do
      resp = post("/select/logsql/field_values?field=level", "query=_time%3A1h",
        content_type: "application/x-www-form-urlencoded")

      values = ddnet_parse_field_values(resp.body)
      assert "info" in values
      assert "error" in values
      assert "warning" in values
    end

    test "DDNet search with quoted string" do
      query =
        URI.encode_query(%{"query" => "_time:1h \"timeout\" | sort by (_time) desc | limit 50"})

      resp = post("/select/logsql/query", query,
        content_type: "application/x-www-form-urlencoded")

      logs = ddnet_parse_ndjson(resp.body)
      assert length(logs) == 1
      assert hd(logs).message =~ "timeout"
    end
  end
end
