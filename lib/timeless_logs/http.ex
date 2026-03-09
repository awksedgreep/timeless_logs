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
    * `GET  /select/logsql/query` - Query logs with filters, returns NDJSON
    * `POST /select/logsql/query` - LogsQL query via form body, returns NDJSON
    * `POST /select/logsql/field_values` - Distinct values for a field
    * `POST /select/logsql/field_names` - All field names from matching entries
    * `GET  /select/logsql/stats` - Storage statistics

  ### Operational
    * `GET /health` - Health check
    * `POST /api/v1/backup` - Online backup
    * `GET /api/v1/flush` - Force buffer flush
  """

  use Rocket.Router

  @max_body_bytes 10 * 1024 * 1024

  def child_spec(opts) do
    port = Keyword.get(opts, :port, 9428)
    bearer_token = Keyword.get(opts, :bearer_token)

    :persistent_term.put({__MODULE__, :bearer_token}, bearer_token)

    %{
      id: __MODULE__,
      start: {Rocket, :start_link, [[port: port, handler: __MODULE__, max_body: @max_body_bytes]]},
      type: :supervisor
    }
  end

  # --- Config access ---

  defp bearer_token, do: :persistent_term.get({__MODULE__, :bearer_token})

  # --- Authentication ---
  # Returns :ok or :halt (response already sent).
  # Skips auth when no token is configured.

  defp check_auth(req) do
    case bearer_token() do
      nil -> :ok
      expected -> verify_token(req, expected)
    end
  end

  defp verify_token(req, expected) do
    case extract_token(req) do
      nil ->
        json_resp(req, 401, %{error: "unauthorized"})
        :halt

      token ->
        if constant_time_compare(token, expected) do
          :ok
        else
          json_resp(req, 403, %{error: "forbidden"})
          :halt
        end
    end
  end

  defp extract_token(req) do
    auth =
      Rocket.Request.get_header(req, "Authorization") ||
        Rocket.Request.get_header(req, "authorization")

    case auth do
      "Bearer " <> token ->
        String.trim(token)

      _ ->
        Rocket.Request.get_query_param(req, "token")
    end
  end

  defp constant_time_compare(a, b) when byte_size(a) == byte_size(b) do
    :crypto.hash_equals(a, b)
  end

  defp constant_time_compare(_a, _b), do: false

  # --- Response helpers ---

  defp json_resp(req, status, term) do
    body = json_encode!(term)

    Rocket.Response.send_iodata(req, status,
      [{"content-type", "application/json"}], body)
  end

  defp ndjson_resp(req, status, body) do
    Rocket.Response.send_iodata(req, status,
      [{"content-type", "application/x-ndjson"}], body)
  end

  defp json_error(req, status, msg) do
    json_resp(req, status, %{error: msg})
  end

  # --- Route Handlers ---

  # Health check (no auth required)
  get "/health" do
    {:ok, stats} = TimelessLogs.stats()

    json_resp(req, 200, %{
      status: "ok",
      blocks: stats.total_blocks,
      entries: stats.total_entries,
      disk_size: stats.disk_size
    })
  end

  # NDJSON log ingest (VictoriaLogs format)
  post "/insert/jsonline" do
    case check_auth(req) do
      :halt -> :ok
      :ok ->
        {params, _} = Rocket.Request.query_params(req)
        msg_field = params["_msg_field"] || "_msg"
        time_field = params["_time_field"] || "_time"

        {count, errors} = ingest_ndjson(req.body, msg_field, time_field)

        if errors > 0 do
          json_resp(req, 200, %{entries: count, errors: errors})
        else
          send_resp(req, 204)
        end
    end
  end

  # Query logs with filters via GET params, returns NDJSON
  get "/select/logsql/query" do
    case check_auth(req) do
      :halt -> :ok
      :ok ->
        {params, _} = Rocket.Request.query_params(req)
        filters = build_query_filters(params)

        case TimelessLogs.query(filters) do
          {:ok, %{entries: entries}} ->
            send_ndjson_response(req, entries)

          {:error, reason} ->
            json_error(req, 500, inspect(reason))
        end
    end
  end

  # Query logs with LogsQL via POST form body, returns NDJSON
  post "/select/logsql/query" do
    case check_auth(req) do
      :halt -> :ok
      :ok ->
        form = URI.decode_query(req.body || "")
        query_str = Map.get(form, "query", "*")

        case TimelessLogs.LogsQL.parse(query_str) do
          {:stats_count, filters} ->
            case TimelessLogs.query(filters) do
              {:ok, %{total: total}} ->
                ndjson_resp(req, 200, json_encode!(%{total: total}))

              {:error, reason} ->
                json_error(req, 500, inspect(reason))
            end

          {:query, filters} ->
            case TimelessLogs.query(filters) do
              {:ok, %{entries: entries}} ->
                send_ndjson_response(req, entries)

              {:error, reason} ->
                json_error(req, 500, inspect(reason))
            end
        end
    end
  end

  # Distinct values for a field
  post "/select/logsql/field_values" do
    case check_auth(req) do
      :halt -> :ok
      :ok ->
        field = Rocket.Request.get_query_param(req, "field")
        form = URI.decode_query(req.body || "")
        query_str = Map.get(form, "query", "*")
        {:query, filters} = TimelessLogs.LogsQL.parse(query_str)
        {:ok, values} = TimelessLogs.field_values(field, filters)

        json_resp(req, 200, %{values: values})
    end
  end

  # All field names from matching entries
  post "/select/logsql/field_names" do
    case check_auth(req) do
      :halt -> :ok
      :ok ->
        form = URI.decode_query(req.body || "")
        query_str = Map.get(form, "query", "*")
        {:query, filters} = TimelessLogs.LogsQL.parse(query_str)
        {:ok, values} = TimelessLogs.field_names(filters)

        json_resp(req, 200, %{values: values})
    end
  end

  # Storage statistics
  get "/select/logsql/stats" do
    case check_auth(req) do
      :halt -> :ok
      :ok ->
        {:ok, stats} = TimelessLogs.stats()

        json_resp(req, 200, %{
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
          zstd_bytes: stats.zstd_bytes,
          openzl_blocks: stats.openzl_blocks,
          openzl_bytes: stats.openzl_bytes
        })
    end
  end

  # Online backup
  post "/api/v1/backup" do
    case check_auth(req) do
      :halt -> :ok
      :ok ->
        body = req.body

        parsed_path =
          case body do
            "" ->
              nil

            _ ->
              case json_decode(body) do
                {:ok, %{"path" => path}} when is_binary(path) and path != "" -> path
                _ -> nil
              end
          end

        target_dir = parsed_path || default_backup_dir()

        case TimelessLogs.backup(target_dir) do
          {:ok, result} ->
            json_resp(req, 200, %{
              status: "ok",
              path: result.path,
              files: result.files,
              total_bytes: result.total_bytes
            })

          {:error, reason} ->
            json_error(req, 500, inspect(reason))
        end
    end
  end

  # Force buffer flush
  get "/api/v1/flush" do
    case check_auth(req) do
      :halt -> :ok
      :ok ->
        TimelessLogs.flush()
        json_resp(req, 200, %{status: "ok"})
    end
  end

  match _ do
    send_resp(req, 404, "not found")
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

          {:ok,
           %{timestamp: timestamp, level: level, message: to_string(message), metadata: metadata}}

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
      {:ok, dt, _offset} ->
        DateTime.to_unix(dt, :microsecond)

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
        nil ->
          filters

        limit ->
          case Integer.parse(limit) do
            {n, _} -> [{:limit, n} | filters]
            :error -> filters
          end
      end

    filters =
      case params["offset"] do
        nil ->
          filters

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
      {:ok, dt, _offset} ->
        DateTime.to_unix(dt, :microsecond)

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

  defp send_ndjson_response(req, entries) do
    body =
      entries
      |> Enum.map_join("\n", fn entry ->
        map = %{
          "_time" => format_timestamp(entry.timestamp),
          "_msg" => entry.message,
          "level" => to_string(entry.level)
        }

        map = Map.merge(map, stringify_metadata(entry.metadata))
        json_encode!(map)
      end)

    ndjson_resp(req, 200, body)
  end

  defp default_backup_dir do
    data_dir = TimelessLogs.Config.data_dir()
    Path.join([data_dir, "backups", to_string(System.os_time(:second))])
  end

  # JSON encoding/decoding using :json NIF (OTP 27+)
  defp json_encode!(term), do: term |> nullify() |> :json.encode() |> IO.iodata_to_binary()

  defp json_decode(data) do
    try do
      {:ok, :json.decode(data)}
    rescue
      _ -> :error
    end
  end

  defp nullify(nil), do: :null
  defp nullify(atom) when is_atom(atom), do: Atom.to_string(atom)
  defp nullify(val) when is_binary(val) or is_number(val) or is_boolean(val), do: val
  defp nullify(list) when is_list(list), do: Enum.map(list, &nullify/1)
  defp nullify(map) when is_map(map), do: Map.new(map, fn {k, v} -> {to_string(k), nullify(v)} end)
  defp nullify(val), do: val
end
