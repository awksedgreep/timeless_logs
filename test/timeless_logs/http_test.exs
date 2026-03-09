defmodule TimelessLogs.HTTPTest do
  use ExUnit.Case, async: false

  require Logger

  @data_dir "test/tmp/http_test"
  @port 19_428

  setup do
    Application.stop(:timeless_logs)
    File.rm_rf!(@data_dir)
    Application.put_env(:timeless_logs, :data_dir, @data_dir)
    Application.put_env(:timeless_logs, :flush_interval, 60_000)
    Application.put_env(:timeless_logs, :max_buffer_size, 10_000)
    Application.put_env(:timeless_logs, :http, false)
    Application.ensure_all_started(:timeless_logs)

    # Clear any auth token from previous tests
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

  defp get(path, opts \\ []) do
    TimelessLogs.TestHTTP.get(@port, path, opts)
  end

  defp post(path, body, opts) do
    TimelessLogs.TestHTTP.post(@port, path, body, opts)
  end

  defp json_body(resp) do
    :json.decode(resp.body)
  end

  # --- Health ---

  describe "GET /health" do
    test "returns ok with stats" do
      resp = get("/health")

      assert resp.status == 200
      body = json_body(resp)
      assert body["status"] == "ok"
      assert is_integer(body["blocks"])
      assert is_integer(body["entries"])
      assert is_integer(body["disk_size"])
    end
  end

  # --- Ingest ---

  describe "POST /insert/jsonline" do
    test "ingests NDJSON lines" do
      body =
        ~s({"_msg":"hello world","level":"info","app":"test"}\n{"_msg":"oh no","level":"error","app":"test"})

      resp = post("/insert/jsonline", body, content_type: "application/x-ndjson")

      assert resp.status == 204

      TimelessLogs.flush()

      {:ok, %{entries: entries}} = TimelessLogs.query([])
      assert length(entries) == 2
      messages = Enum.map(entries, & &1.message)
      assert "hello world" in messages
      assert "oh no" in messages
    end

    test "uses custom _msg_field and _time_field" do
      body = ~s({"body":"custom msg","ts":"2024-06-15T12:00:00Z","level":"warning"})

      resp =
        post("/insert/jsonline?_msg_field=body&_time_field=ts", body,
          content_type: "application/x-ndjson"
        )

      assert resp.status == 204

      TimelessLogs.flush()

      {:ok, %{entries: entries}} = TimelessLogs.query(level: :warning)
      assert length(entries) == 1
      assert hd(entries).message == "custom msg"
    end

    test "reports errors for malformed lines" do
      body = "not json at all\n{\"_msg\":\"valid\",\"level\":\"info\"}\n{broken"

      resp = post("/insert/jsonline", body, content_type: "application/x-ndjson")

      assert resp.status == 200
      body = json_body(resp)
      assert body["entries"] == 1
      assert body["errors"] == 2
    end

    test "extra fields become metadata" do
      body = ~s({"_msg":"tagged","level":"info","service":"api","region":"us-east"})

      resp = post("/insert/jsonline", body, content_type: "application/x-ndjson")

      assert resp.status == 204

      TimelessLogs.flush()

      {:ok, %{entries: [entry]}} = TimelessLogs.query([])
      assert entry.metadata[:service] == "api"
      assert entry.metadata[:region] == "us-east"
    end

    test "parses ISO8601 timestamps" do
      body = ~s({"_msg":"timestamped","_time":"2024-01-15T10:30:00Z","level":"info"})

      resp = post("/insert/jsonline", body, content_type: "application/x-ndjson")

      assert resp.status == 204

      TimelessLogs.flush()

      {:ok, %{entries: [entry]}} = TimelessLogs.query([])
      # 2024-01-15T10:30:00Z as microseconds
      assert entry.timestamp == 1_705_314_600_000_000
    end

    test "parses unix second timestamps" do
      body = ~s({"_msg":"unix ts","_time":1700000000,"level":"info"})

      resp = post("/insert/jsonline", body, content_type: "application/x-ndjson")

      assert resp.status == 204

      TimelessLogs.flush()

      {:ok, %{entries: [entry]}} = TimelessLogs.query([])
      assert entry.timestamp == 1_700_000_000_000_000
    end
  end

  # --- Query (GET) ---

  describe "GET /select/logsql/query" do
    setup :ingest_test_data

    test "returns all logs as NDJSON" do
      resp = get("/select/logsql/query")
      assert resp.status == 200

      ct = List.keyfind(resp.headers, "content-type", 0)
      assert ct && elem(ct, 1) =~ "ndjson"

      lines = String.split(resp.body, "\n", trim: true)
      assert length(lines) == 3

      parsed = Enum.map(lines, &:json.decode/1)
      assert Enum.all?(parsed, &Map.has_key?(&1, "_msg"))
      assert Enum.all?(parsed, &Map.has_key?(&1, "_time"))
      assert Enum.all?(parsed, &Map.has_key?(&1, "level"))
    end

    test "filters by level" do
      resp = get("/select/logsql/query?level=error")
      assert resp.status == 200

      lines = String.split(resp.body, "\n", trim: true)
      assert length(lines) == 1
      assert :json.decode(hd(lines))["_msg"] == "error boom"
    end

    test "respects limit" do
      resp = get("/select/logsql/query?limit=2")
      assert resp.status == 200

      lines = String.split(resp.body, "\n", trim: true)
      assert length(lines) == 2
    end

    test "respects order=asc" do
      resp = get("/select/logsql/query?order=asc")
      assert resp.status == 200

      lines = String.split(resp.body, "\n", trim: true)
      parsed = Enum.map(lines, &:json.decode/1)
      times = Enum.map(parsed, & &1["_time"])
      assert times == Enum.sort(times)
    end
  end

  # --- Query (POST with LogsQL) ---

  describe "POST /select/logsql/query" do
    setup :ingest_test_data

    test "returns all logs with wildcard query" do
      resp =
        post("/select/logsql/query", "query=*", content_type: "application/x-www-form-urlencoded")

      assert resp.status == 200

      ct = List.keyfind(resp.headers, "content-type", 0)
      assert ct && elem(ct, 1) =~ "ndjson"

      lines = String.split(resp.body, "\n", trim: true)
      assert length(lines) == 3
    end

    test "filters by level via LogsQL" do
      resp =
        post("/select/logsql/query", "query=level%3Aerror",
          content_type: "application/x-www-form-urlencoded"
        )

      assert resp.status == 200

      lines = String.split(resp.body, "\n", trim: true)
      assert length(lines) == 1
      assert :json.decode(hd(lines))["_msg"] == "error boom"
    end

    test "stats count query returns total" do
      query = URI.encode_query(%{"query" => "* | stats count() as total"})

      resp =
        post("/select/logsql/query", query, content_type: "application/x-www-form-urlencoded")

      assert resp.status == 200
      body = :json.decode(resp.body)
      assert body["total"] == 3
    end

    test "respects limit pipe" do
      query = URI.encode_query(%{"query" => "* | limit 1"})

      resp =
        post("/select/logsql/query", query, content_type: "application/x-www-form-urlencoded")

      assert resp.status == 200

      lines = String.split(resp.body, "\n", trim: true)
      assert length(lines) == 1
    end

    test "filters by metadata field" do
      query = URI.encode_query(%{"query" => "service:web"})

      resp =
        post("/select/logsql/query", query, content_type: "application/x-www-form-urlencoded")

      assert resp.status == 200

      lines = String.split(resp.body, "\n", trim: true)
      assert length(lines) == 1
      assert :json.decode(hd(lines))["_msg"] == "info two"
    end
  end

  # --- Field Values ---

  describe "POST /select/logsql/field_values" do
    setup :ingest_test_data

    test "returns distinct values for level field" do
      resp =
        post("/select/logsql/field_values?field=level", "query=*",
          content_type: "application/x-www-form-urlencoded"
        )

      assert resp.status == 200
      body = json_body(resp)
      values = body["values"]

      assert is_list(values)
      value_names = Enum.map(values, & &1["value"])
      assert "info" in value_names
      assert "error" in value_names
    end

    test "returns distinct values for metadata field" do
      resp =
        post("/select/logsql/field_values?field=service", "query=*",
          content_type: "application/x-www-form-urlencoded"
        )

      assert resp.status == 200
      body = json_body(resp)
      values = body["values"]
      value_names = Enum.map(values, & &1["value"])
      assert "api" in value_names
      assert "web" in value_names
    end
  end

  # --- Field Names ---

  describe "POST /select/logsql/field_names" do
    setup :ingest_test_data

    test "returns all field names" do
      resp =
        post("/select/logsql/field_names", "query=*",
          content_type: "application/x-www-form-urlencoded"
        )

      assert resp.status == 200
      body = json_body(resp)
      values = body["values"]

      value_names = Enum.map(values, & &1["value"])
      assert "_msg" in value_names
      assert "_time" in value_names
      assert "level" in value_names
      assert "service" in value_names
    end
  end

  # --- Stats ---

  describe "GET /select/logsql/stats" do
    test "returns storage statistics" do
      resp = get("/select/logsql/stats")
      assert resp.status == 200

      body = json_body(resp)
      assert is_integer(body["total_blocks"])
      assert is_integer(body["total_entries"])
      assert is_integer(body["total_bytes"])
      assert is_integer(body["disk_size"])
      assert is_integer(body["index_size"])
    end

    test "stats reflect ingested data" do
      body = ~s({"_msg":"stat test","level":"info"})
      post("/insert/jsonline", body, content_type: "application/x-ndjson")

      TimelessLogs.flush()

      resp = get("/select/logsql/stats")
      stats = json_body(resp)
      assert stats["total_blocks"] >= 1
      assert stats["total_entries"] >= 1
    end
  end

  # --- Flush ---

  describe "GET /api/v1/flush" do
    test "returns ok" do
      resp = get("/api/v1/flush")
      assert resp.status == 200
      assert json_body(resp)["status"] == "ok"
    end
  end

  # --- Backup ---

  describe "POST /api/v1/backup" do
    test "creates backup with default path" do
      body = ~s({"_msg":"backup test","level":"info"})
      post("/insert/jsonline", body, content_type: "application/x-ndjson")

      TimelessLogs.flush()

      resp = post("/api/v1/backup", "", content_type: "application/json")

      assert resp.status == 200
      result = json_body(resp)
      assert result["status"] == "ok"
      assert is_binary(result["path"])
      assert is_integer(result["total_bytes"])

      # Clean up backup
      File.rm_rf!(result["path"])
    end

    test "creates backup with custom path" do
      backup_dir = Path.join(@data_dir, "custom_backup")

      body = ~s({"_msg":"backup test","level":"info"})
      post("/insert/jsonline", body, content_type: "application/x-ndjson")

      TimelessLogs.flush()

      payload = IO.iodata_to_binary(:json.encode(%{"path" => backup_dir}))

      resp = post("/api/v1/backup", payload, content_type: "application/json")

      assert resp.status == 200
      result = json_body(resp)
      assert result["path"] == backup_dir
      assert File.exists?(backup_dir)

      File.rm_rf!(backup_dir)
    end
  end

  # --- Auth ---

  describe "bearer token auth" do
    test "401 when token required but not provided" do
      :persistent_term.put({TimelessLogs.HTTP, :bearer_token}, "secret")

      resp = get("/select/logsql/stats")
      assert resp.status == 401
      assert json_body(resp)["error"] == "unauthorized"
    end

    test "403 when token is wrong" do
      :persistent_term.put({TimelessLogs.HTTP, :bearer_token}, "secret")

      resp =
        get("/select/logsql/stats",
          headers: [{"authorization", "Bearer wrong"}]
        )

      assert resp.status == 403
      assert json_body(resp)["error"] == "forbidden"
    end

    test "passes with correct bearer token" do
      :persistent_term.put({TimelessLogs.HTTP, :bearer_token}, "secret")

      resp =
        get("/select/logsql/stats",
          headers: [{"authorization", "Bearer secret"}]
        )

      assert resp.status == 200
    end

    test "passes with query param token" do
      :persistent_term.put({TimelessLogs.HTTP, :bearer_token}, "secret")

      resp = get("/select/logsql/stats?token=secret")

      assert resp.status == 200
    end

    test "health endpoint skips auth" do
      :persistent_term.put({TimelessLogs.HTTP, :bearer_token}, "secret")

      resp = get("/health")
      assert resp.status == 200
    end
  end

  # --- 404 ---

  describe "catch-all" do
    test "returns 404 for unknown routes" do
      resp = get("/nonexistent")
      assert resp.status == 404
    end
  end

  # --- Helpers ---

  defp ingest_test_data(_context) do
    lines =
      Enum.join(
        [
          ~s({"_msg":"info one","level":"info","service":"api"}),
          ~s({"_msg":"error boom","level":"error","service":"api"}),
          ~s({"_msg":"info two","level":"info","service":"web"})
        ],
        "\n"
      )

    post("/insert/jsonline", lines, content_type: "application/x-ndjson")

    TimelessLogs.flush()
    :ok
  end
end
