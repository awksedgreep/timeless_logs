defmodule LogStream.HTTPTest do
  use ExUnit.Case, async: false

  import Plug.Test
  import Plug.Conn
  require Logger

  @data_dir "test/tmp/http_test"

  setup do
    Application.stop(:log_stream)
    File.rm_rf!(@data_dir)
    Application.put_env(:log_stream, :data_dir, @data_dir)
    Application.put_env(:log_stream, :flush_interval, 60_000)
    Application.put_env(:log_stream, :max_buffer_size, 10_000)
    Application.put_env(:log_stream, :http, false)
    Application.ensure_all_started(:log_stream)

    on_exit(fn ->
      Application.stop(:log_stream)
      File.rm_rf!(@data_dir)
    end)

    :ok
  end

  defp call(conn, opts \\ []) do
    LogStream.HTTP.call(conn, LogStream.HTTP.init(opts))
  end

  defp json_body(conn) do
    Jason.decode!(conn.resp_body)
  end

  # --- Health ---

  describe "GET /health" do
    test "returns ok with stats" do
      conn = conn(:get, "/health") |> call()

      assert conn.status == 200
      body = json_body(conn)
      assert body["status"] == "ok"
      assert is_integer(body["blocks"])
      assert is_integer(body["entries"])
      assert is_integer(body["disk_size"])
    end
  end

  # --- Ingest ---

  describe "POST /insert/jsonline" do
    test "ingests NDJSON lines" do
      body = ~s({"_msg":"hello world","level":"info","app":"test"}\n{"_msg":"oh no","level":"error","app":"test"})

      conn =
        conn(:post, "/insert/jsonline", body)
        |> put_req_header("content-type", "application/x-ndjson")
        |> call()

      assert conn.status == 204

      LogStream.flush()

      {:ok, %{entries: entries}} = LogStream.query([])
      assert length(entries) == 2
      messages = Enum.map(entries, & &1.message)
      assert "hello world" in messages
      assert "oh no" in messages
    end

    test "uses custom _msg_field and _time_field" do
      body = ~s({"body":"custom msg","ts":"2024-06-15T12:00:00Z","level":"warning"})

      conn =
        conn(:post, "/insert/jsonline?_msg_field=body&_time_field=ts", body)
        |> put_req_header("content-type", "application/x-ndjson")
        |> call()

      assert conn.status == 204

      LogStream.flush()

      {:ok, %{entries: entries}} = LogStream.query(level: :warning)
      assert length(entries) == 1
      assert hd(entries).message == "custom msg"
    end

    test "reports errors for malformed lines" do
      body = "not json at all\n{\"_msg\":\"valid\",\"level\":\"info\"}\n{broken"

      conn =
        conn(:post, "/insert/jsonline", body)
        |> put_req_header("content-type", "application/x-ndjson")
        |> call()

      assert conn.status == 200
      body = json_body(conn)
      assert body["entries"] == 1
      assert body["errors"] == 2
    end

    test "extra fields become metadata" do
      body = ~s({"_msg":"tagged","level":"info","service":"api","region":"us-east"})

      conn =
        conn(:post, "/insert/jsonline", body)
        |> put_req_header("content-type", "application/x-ndjson")
        |> call()

      assert conn.status == 204

      LogStream.flush()

      {:ok, %{entries: [entry]}} = LogStream.query([])
      assert entry.metadata[:service] == "api"
      assert entry.metadata[:region] == "us-east"
    end

    test "parses ISO8601 timestamps" do
      body = ~s({"_msg":"timestamped","_time":"2024-01-15T10:30:00Z","level":"info"})

      conn =
        conn(:post, "/insert/jsonline", body)
        |> put_req_header("content-type", "application/x-ndjson")
        |> call()

      assert conn.status == 204

      LogStream.flush()

      {:ok, %{entries: [entry]}} = LogStream.query([])
      # 2024-01-15T10:30:00Z as microseconds
      assert entry.timestamp == 1_705_314_600_000_000
    end

    test "parses unix second timestamps" do
      body = ~s({"_msg":"unix ts","_time":1700000000,"level":"info"})

      conn =
        conn(:post, "/insert/jsonline", body)
        |> put_req_header("content-type", "application/x-ndjson")
        |> call()

      assert conn.status == 204

      LogStream.flush()

      {:ok, %{entries: [entry]}} = LogStream.query([])
      assert entry.timestamp == 1_700_000_000_000_000
    end
  end

  # --- Query ---

  describe "GET /select/logsql/query" do
    setup do
      lines =
        Enum.join([
          ~s({"_msg":"info one","level":"info","service":"api"}),
          ~s({"_msg":"error boom","level":"error","service":"api"}),
          ~s({"_msg":"info two","level":"info","service":"web"})
        ], "\n")

      conn(:post, "/insert/jsonline", lines)
      |> put_req_header("content-type", "application/x-ndjson")
      |> call()

      LogStream.flush()
      :ok
    end

    test "returns all logs as NDJSON" do
      conn = conn(:get, "/select/logsql/query") |> call()
      assert conn.status == 200
      assert get_resp_header(conn, "content-type") |> hd() =~ "ndjson"

      lines = String.split(conn.resp_body, "\n", trim: true)
      assert length(lines) == 3

      parsed = Enum.map(lines, &Jason.decode!/1)
      assert Enum.all?(parsed, &Map.has_key?(&1, "_msg"))
      assert Enum.all?(parsed, &Map.has_key?(&1, "_time"))
      assert Enum.all?(parsed, &Map.has_key?(&1, "level"))
    end

    test "filters by level" do
      conn = conn(:get, "/select/logsql/query?level=error") |> call()
      assert conn.status == 200

      lines = String.split(conn.resp_body, "\n", trim: true)
      assert length(lines) == 1
      assert Jason.decode!(hd(lines))["_msg"] == "error boom"
    end

    test "respects limit" do
      conn = conn(:get, "/select/logsql/query?limit=2") |> call()
      assert conn.status == 200

      lines = String.split(conn.resp_body, "\n", trim: true)
      assert length(lines) == 2
    end

    test "respects order=asc" do
      conn = conn(:get, "/select/logsql/query?order=asc") |> call()
      assert conn.status == 200

      lines = String.split(conn.resp_body, "\n", trim: true)
      parsed = Enum.map(lines, &Jason.decode!/1)
      times = Enum.map(parsed, & &1["_time"])
      assert times == Enum.sort(times)
    end
  end

  # --- Stats ---

  describe "GET /select/logsql/stats" do
    test "returns storage statistics" do
      conn = conn(:get, "/select/logsql/stats") |> call()
      assert conn.status == 200

      body = json_body(conn)
      assert is_integer(body["total_blocks"])
      assert is_integer(body["total_entries"])
      assert is_integer(body["total_bytes"])
      assert is_integer(body["disk_size"])
      assert is_integer(body["index_size"])
    end

    test "stats reflect ingested data" do
      body = ~s({"_msg":"stat test","level":"info"})

      conn(:post, "/insert/jsonline", body)
      |> put_req_header("content-type", "application/x-ndjson")
      |> call()

      LogStream.flush()

      conn = conn(:get, "/select/logsql/stats") |> call()
      stats = json_body(conn)
      assert stats["total_blocks"] >= 1
      assert stats["total_entries"] >= 1
    end
  end

  # --- Flush ---

  describe "GET /api/v1/flush" do
    test "returns ok" do
      conn = conn(:get, "/api/v1/flush") |> call()
      assert conn.status == 200
      assert json_body(conn)["status"] == "ok"
    end
  end

  # --- Backup ---

  describe "POST /api/v1/backup" do
    test "creates backup with default path" do
      # Ingest some data first
      body = ~s({"_msg":"backup test","level":"info"})

      conn(:post, "/insert/jsonline", body)
      |> put_req_header("content-type", "application/x-ndjson")
      |> call()

      LogStream.flush()

      conn =
        conn(:post, "/api/v1/backup", "")
        |> put_req_header("content-type", "application/json")
        |> call()

      assert conn.status == 200
      result = json_body(conn)
      assert result["status"] == "ok"
      assert is_binary(result["path"])
      assert is_integer(result["total_bytes"])

      # Clean up backup
      File.rm_rf!(result["path"])
    end

    test "creates backup with custom path" do
      backup_dir = Path.join(@data_dir, "custom_backup")

      body = ~s({"_msg":"backup test","level":"info"})

      conn(:post, "/insert/jsonline", body)
      |> put_req_header("content-type", "application/x-ndjson")
      |> call()

      LogStream.flush()

      conn =
        conn(:post, "/api/v1/backup", Jason.encode!(%{path: backup_dir}))
        |> put_req_header("content-type", "application/json")
        |> call()

      assert conn.status == 200
      result = json_body(conn)
      assert result["path"] == backup_dir
      assert File.exists?(backup_dir)

      File.rm_rf!(backup_dir)
    end
  end

  # --- Auth ---

  describe "bearer token auth" do
    test "401 when token required but not provided" do
      conn = conn(:get, "/select/logsql/stats") |> call(bearer_token: "secret")
      assert conn.status == 401
      assert json_body(conn)["error"] == "unauthorized"
    end

    test "403 when token is wrong" do
      conn =
        conn(:get, "/select/logsql/stats")
        |> put_req_header("authorization", "Bearer wrong")
        |> call(bearer_token: "secret")

      assert conn.status == 403
      assert json_body(conn)["error"] == "forbidden"
    end

    test "passes with correct bearer token" do
      conn =
        conn(:get, "/select/logsql/stats")
        |> put_req_header("authorization", "Bearer secret")
        |> call(bearer_token: "secret")

      assert conn.status == 200
    end

    test "passes with query param token" do
      conn =
        conn(:get, "/select/logsql/stats?token=secret")
        |> call(bearer_token: "secret")

      assert conn.status == 200
    end

    test "health endpoint skips auth" do
      conn = conn(:get, "/health") |> call(bearer_token: "secret")
      assert conn.status == 200
    end
  end

  # --- 404 ---

  describe "catch-all" do
    test "returns 404 for unknown routes" do
      conn = conn(:get, "/nonexistent") |> call()
      assert conn.status == 404
    end
  end
end
