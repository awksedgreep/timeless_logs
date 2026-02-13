defmodule TimelessLogs.HTTP do
  require Logger

  @moduledoc """
  Optional HTTP interface compatible with VictoriaLogs.

  ## Usage

  Add to your config:

      config :timeless_logs, http: true                          # port 9428, no auth
      config :timeless_logs, http: [port: 9500, bearer_token: "secret"]

  Or add to your supervision tree directly:

      children = [
        {TimelessLogs.HTTP, port: 9428}
      ]

  ## Endpoints

  ### Ingest
    * `POST /insert/jsonline` - NDJSON log ingest (VictoriaLogs format)

  ### Query
    * `GET /select/logsql/query` - Query logs with filters, returns NDJSON
    * `GET /select/logsql/stats` - Storage statistics

  ### Operational
    * `GET /health` - Health check
    * `POST /api/v1/backup` - Online backup
    * `GET /api/v1/flush` - Force buffer flush
  """

  use Plug.Router

  @max_body_bytes 10 * 1024 * 1024

  plug :match
  plug :authenticate
  plug :dispatch

  def child_spec(opts) do
    port = Keyword.get(opts, :port, 9428)
    bearer_token = Keyword.get(opts, :bearer_token)
    plug_opts = [bearer_token: bearer_token]

    %{
      id: __MODULE__,
      start: {Bandit, :start_link, [[plug: {__MODULE__, plug_opts}, port: port]]},
      type: :supervisor
    }
  end

  @impl Plug
  def init(opts), do: opts

  @impl Plug
  def call(conn, opts) do
    conn
    |> Plug.Conn.put_private(:timeless_logs_token, Keyword.get(opts, :bearer_token))
    |> super(opts)
  end

  # Bearer token authentication plug.
  # Skips auth when no token is configured.
  # Exempts /health for load balancers and monitoring.
  defp authenticate(%{request_path: "/health"} = conn, _opts), do: conn

  defp authenticate(conn, _opts) do
    case conn.private[:timeless_logs_token] do
      nil -> conn
      expected -> check_token(conn, expected)
    end
  end

  defp check_token(conn, expected) do
    case extract_token(conn) do
      nil ->
        conn
        |> put_resp_content_type("application/json")
        |> send_resp(401, ~s({"error":"unauthorized"}))
        |> halt()

      token ->
        if Plug.Crypto.secure_compare(token, expected) do
          conn
        else
          conn
          |> put_resp_content_type("application/json")
          |> send_resp(403, ~s({"error":"forbidden"}))
          |> halt()
        end
    end
  end

  defp extract_token(conn) do
    case Plug.Conn.get_req_header(conn, "authorization") do
      ["Bearer " <> token] ->
        String.trim(token)

      _ ->
        conn = Plug.Conn.fetch_query_params(conn)
        conn.query_params["token"]
    end
  end

  # Health check
  get "/health" do
    {:ok, stats} = TimelessLogs.stats()

    body =
      Jason.encode!(%{
        status: "ok",
        blocks: stats.total_blocks,
        entries: stats.total_entries,
        disk_size: stats.disk_size
      })

    conn
    |> put_resp_content_type("application/json")
    |> send_resp(200, body)
  end

  # NDJSON log ingest (VictoriaLogs format)
  post "/insert/jsonline" do
    conn = Plug.Conn.fetch_query_params(conn)
    msg_field = conn.query_params["_msg_field"] || "_msg"
    time_field = conn.query_params["_time_field"] || "_time"

    case Plug.Conn.read_body(conn, length: @max_body_bytes) do
      {:ok, body, conn} ->
        {count, errors} = ingest_ndjson(body, msg_field, time_field)

        if errors > 0 do
          conn
          |> put_resp_content_type("application/json")
          |> send_resp(200, Jason.encode!(%{entries: count, errors: errors}))
        else
          send_resp(conn, 204, "")
        end

      {:more, _partial, conn} ->
        conn
        |> put_resp_content_type("application/json")
        |> send_resp(413, Jason.encode!(%{error: "body too large", max_bytes: @max_body_bytes}))

      {:error, reason} ->
        json_error(conn, 400, to_string(reason))
    end
  end

  # Query logs with filters, returns NDJSON
  get "/select/logsql/query" do
    conn = Plug.Conn.fetch_query_params(conn)
    params = conn.query_params

    filters = build_query_filters(params)

    case TimelessLogs.query(filters) do
      {:ok, %{entries: entries}} ->
        body =
          entries
          |> Enum.map_join("\n", fn entry ->
            map = %{"_time" => format_timestamp(entry.timestamp), "_msg" => entry.message, "level" => to_string(entry.level)}
            map = Map.merge(map, stringify_metadata(entry.metadata))
            Jason.encode!(map)
          end)

        conn
        |> put_resp_content_type("application/x-ndjson")
        |> send_resp(200, body)

      {:error, reason} ->
        json_error(conn, 500, inspect(reason))
    end
  end

  # Storage statistics
  get "/select/logsql/stats" do
    {:ok, stats} = TimelessLogs.stats()

    conn
    |> put_resp_content_type("application/json")
    |> send_resp(200, Jason.encode!(%{
      total_blocks: stats.total_blocks,
      total_entries: stats.total_entries,
      total_bytes: stats.total_bytes,
      disk_size: stats.disk_size,
      index_size: stats.index_size,
      oldest_timestamp: stats.oldest_timestamp,
      newest_timestamp: stats.newest_timestamp,
      raw_blocks: stats.raw_blocks,
      raw_bytes: stats.raw_bytes,
      zstd_blocks: stats.zstd_blocks,
      zstd_bytes: stats.zstd_bytes
    }))
  end

  # Online backup
  post "/api/v1/backup" do
    parsed_path =
      case Plug.Conn.read_body(conn, length: 64_000) do
        {:ok, "", _} -> nil
        {:ok, body, _} ->
          case Jason.decode(body) do
            {:ok, %{"path" => path}} when is_binary(path) and path != "" -> path
            _ -> nil
          end
        _ -> nil
      end

    target_dir = parsed_path || default_backup_dir()

    case TimelessLogs.backup(target_dir) do
      {:ok, result} ->
        conn
        |> put_resp_content_type("application/json")
        |> send_resp(200, Jason.encode!(%{
          status: "ok",
          path: result.path,
          files: result.files,
          total_bytes: result.total_bytes
        }))

      {:error, reason} ->
        json_error(conn, 500, inspect(reason))
    end
  end

  # Force buffer flush
  get "/api/v1/flush" do
    TimelessLogs.flush()

    conn
    |> put_resp_content_type("application/json")
    |> send_resp(200, Jason.encode!(%{status: "ok"}))
  end

  match _ do
    send_resp(conn, 404, "not found")
  end

  # --- Internals ---

  defp ingest_ndjson(body, msg_field, time_field) do
    body
    |> String.split("\n", trim: true)
    |> Enum.reduce({0, 0}, fn line, {count, errors} ->
      case parse_log_line(line, msg_field, time_field) do
        {:ok, entry} ->
          TimelessLogs.Buffer.log(entry)
          {count + 1, errors}

        :error ->
          {count, errors + 1}
      end
    end)
  end

  defp parse_log_line(line, msg_field, time_field) do
    try do
      case :json.decode(line) do
        map when is_map(map) ->
          message = Map.get(map, msg_field, "")
          time_val = Map.get(map, time_field)
          level_str = Map.get(map, "level", "info")
          level = parse_level(level_str)

          timestamp = parse_ingest_time(time_val)

          metadata =
            map
            |> Map.drop([msg_field, time_field, "level"])
            |> Map.new(fn {k, v} -> {String.to_atom(k), v} end)

          {:ok, %{timestamp: timestamp, level: level, message: to_string(message), metadata: metadata}}

        _ ->
          :error
      end
    rescue
      _ -> :error
    end
  end

  defp parse_level("debug"), do: :debug
  defp parse_level("info"), do: :info
  defp parse_level("warning"), do: :warning
  defp parse_level("warn"), do: :warning
  defp parse_level("error"), do: :error
  defp parse_level(_), do: :info

  defp parse_ingest_time(nil), do: System.os_time(:microsecond)
  defp parse_ingest_time(val) when is_integer(val) do
    # Unix seconds → microseconds
    val * 1_000_000
  end
  defp parse_ingest_time(val) when is_binary(val) do
    case DateTime.from_iso8601(val) do
      {:ok, dt, _offset} -> DateTime.to_unix(dt, :microsecond)
      _ ->
        case Integer.parse(val) do
          {n, _} -> n * 1_000_000
          :error -> System.os_time(:microsecond)
        end
    end
  end
  defp parse_ingest_time(_), do: System.os_time(:microsecond)

  defp build_query_filters(params) do
    filters = []

    filters =
      case params["level"] do
        nil -> filters
        level -> [{:level, String.to_existing_atom(level)} | filters]
      end

    filters =
      case params["message"] do
        nil -> filters
        msg -> [{:message, msg} | filters]
      end

    filters =
      case params["start"] do
        nil -> filters
        start -> [{:since, parse_query_time(start)} | filters]
      end

    filters =
      case params["end"] do
        nil -> filters
        end_time -> [{:until, parse_query_time(end_time)} | filters]
      end

    filters =
      case params["limit"] do
        nil -> filters
        limit ->
          case Integer.parse(limit) do
            {n, _} -> [{:limit, n} | filters]
            :error -> filters
          end
      end

    filters =
      case params["offset"] do
        nil -> filters
        offset ->
          case Integer.parse(offset) do
            {n, _} -> [{:offset, n} | filters]
            :error -> filters
          end
      end

    filters =
      case params["order"] do
        "asc" -> [{:order, :asc} | filters]
        "desc" -> [{:order, :desc} | filters]
        _ -> filters
      end

    filters
  end

  defp parse_query_time(val) when is_binary(val) do
    case DateTime.from_iso8601(val) do
      {:ok, dt, _offset} -> DateTime.to_unix(dt, :microsecond)
      _ ->
        case Integer.parse(val) do
          {n, _} -> n
          :error -> 0
        end
    end
  end

  defp format_timestamp(nil), do: nil
  defp format_timestamp(ts) when is_integer(ts) do
    ts
    |> DateTime.from_unix!(:microsecond)
    |> DateTime.to_iso8601()
  end

  defp stringify_metadata(nil), do: %{}
  defp stringify_metadata(metadata) when is_map(metadata) do
    Map.new(metadata, fn {k, v} -> {to_string(k), v} end)
  end

  defp default_backup_dir do
    data_dir = TimelessLogs.Config.data_dir()
    Path.join([data_dir, "backups", to_string(System.os_time(:second))])
  end

  defp json_error(conn, status, msg) do
    conn
    |> put_resp_content_type("application/json")
    |> send_resp(status, Jason.encode!(%{error: msg}))
  end
end
