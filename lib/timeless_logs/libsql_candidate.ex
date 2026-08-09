defmodule TimelessLogs.LibsqlCandidate do
  @moduledoc false

  use GenServer

  @table "logs"

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts)
  def sql(pid, sql, params \\ []), do: GenServer.call(pid, {:sql, sql, params}, :infinity)

  def checkpoint(pid, entries, journal, opts \\ []) do
    with {:ok, blob} <- encode_batch(entries) do
      GenServer.call(pid, {:checkpoint, blob, entries == [], journal, opts}, :infinity)
    end
  end

  def command(pid, command),
    do: sql(pid, "INSERT INTO #{@table}(#{@table}) VALUES (?1)", [command])

  def path(pid), do: GenServer.call(pid, :path)
  def phase(pid, phase), do: GenServer.call(pid, {:phase, phase}, :infinity)

  @doc false
  def open_connection(path, extension_path \\ nil) do
    open_connection(path, extension_path, [])
  end

  @doc false
  def open_readonly_connection(path, extension_path \\ nil) do
    open_connection(path, extension_path, mode: :readonly)
  end

  defp open_connection(path, extension_path, open_options) do
    extension = extension_path || extension_path!()

    case Exqlite.Sqlite3.open(path, open_options) do
      {:ok, conn} ->
        case load_and_verify(conn, extension) do
          {:ok, capabilities} ->
            {:ok, conn, capabilities}

          {:error, _} = error ->
            Exqlite.Sqlite3.close(conn)
            error
        end

      {:error, _} = error ->
        error
    end
  end

  defp load_and_verify(conn, extension) do
    with :ok <- load_extension(conn, extension),
         {:ok, capabilities} <- capabilities(conn),
         :ok <- require_capability(capabilities) do
      {:ok, capabilities}
    end
  end

  @impl true
  def init(opts) do
    # Trap exits so terminate/2 closes the connection (and its WAL) even
    # when the linked caller dies mid-migration, not only on the clean
    # GenServer.stop path.
    Process.flag(:trap_exit, true)
    path = Keyword.fetch!(opts, :path)
    extension = Keyword.get(opts, :extension_path) || extension_path!()

    retention_seconds =
      Keyword.get(opts, :retention_seconds, TimelessLogs.Config.retention_max_age())

    File.mkdir_p!(Path.dirname(path))

    with :ok <- validate_retention(retention_seconds),
         {:ok, conn, capabilities} <- open_connection(path, extension),
         :ok <- initialize_database(conn, capabilities, retention_seconds) do
      {:ok, %{conn: conn, path: path}}
    else
      {:error, reason} -> {:stop, reason}
    end
  end

  @impl true
  def handle_call({:sql, sql, params}, _from, state) do
    {:reply, execute(state.conn, sql, params), state}
  end

  def handle_call(:path, _from, state), do: {:reply, state.path, state}

  def handle_call({:phase, phase}, _from, state) do
    now = System.system_time(:nanosecond)
    {:ok, _} = execute(state.conn, "BEGIN IMMEDIATE")

    result =
      with {:ok, _} <-
             execute(
               state.conn,
               "UPDATE _timeless_migration SET phase=?1,updated_at_ns=?2 WHERE singleton=1",
               [phase, now]
             ),
           {:ok, [[cursor, records]]} <-
             execute(
               state.conn,
               "SELECT cursor_json,records_completed FROM _timeless_migration WHERE singleton=1"
             ),
           {:ok, _} <-
             execute(
               state.conn,
               "INSERT INTO _timeless_migration_events(phase,cursor_json,records_completed,at_ns) VALUES (?1,?2,?3,?4)",
               [phase, cursor, records, now]
             ),
           {:ok, _} <- execute(state.conn, "COMMIT") do
        :ok
      else
        {:error, reason} ->
          _ = execute(state.conn, "ROLLBACK")
          {:error, reason}
      end

    {:reply, result, state}
  end

  def handle_call({:checkpoint, blob, empty?, journal, opts}, _from, state) do
    failpoint = Keyword.get(opts, :failpoint)
    final_page? = Keyword.get(opts, :final_page, false)
    {:ok, _} = execute(state.conn, "BEGIN IMMEDIATE")

    try do
      unless empty? do
        {:ok, _} =
          execute(state.conn, "INSERT INTO #{@table}(#{@table}) VALUES (?1)", [{:blob, blob}])
      end

      if final_page? and not empty? do
        {:ok, _} = execute(state.conn, "INSERT INTO #{@table}(#{@table}) VALUES ('flush')")
      end

      failpoint!(failpoint, :disk_full)
      failpoint!(failpoint, :after_batch_before_journal)

      {:ok, _} =
        execute(
          state.conn,
          """
          UPDATE _timeless_migration
          SET phase=?1,cursor_json=?2,records_completed=?3,
              identity_digest=?4,updated_at_ns=?5,checkpoints=checkpoints+1
          WHERE singleton=1
          """,
          [
            journal.phase,
            journal.cursor_json,
            journal.records_completed,
            journal.identity_digest,
            journal.updated_at_ns
          ]
        )

      {:ok, _} =
        execute(
          state.conn,
          "INSERT INTO _timeless_migration_events(phase,cursor_json,records_completed,at_ns) VALUES (?1,?2,?3,?4)",
          [journal.phase, journal.cursor_json, journal.records_completed, journal.updated_at_ns]
        )

      failpoint!(failpoint, :after_journal_before_commit)
      {:ok, _} = execute(state.conn, "COMMIT")
      {:reply, :ok, state}
    rescue
      error ->
        _ = execute(state.conn, "ROLLBACK")
        {:reply, {:error, Exception.message(error)}, state}
    end
  end

  @impl true
  def terminate(_reason, state) do
    Exqlite.Sqlite3.close(state.conn)
  end

  @doc false
  def encode_batch(entries) do
    count = length(entries)
    header = <<0x02, 0, 0::little-16, count::little-32>>

    timestamps =
      Enum.map(entries, fn entry ->
        <<TimelessLogs.Timestamp.to_microseconds(entry.timestamp)::signed-little-64>>
      end)

    with {:ok, severities} <- text_column(entries, &severity/1),
         {:ok, messages} <- text_column(entries, & &1.message),
         {:ok, metadata} <- text_column(entries, &canonical_metadata/1) do
      {:ok, IO.iodata_to_binary([header, timestamps, severities, messages, metadata])}
    end
  rescue
    error -> {:error, "cannot encode public rich logs batch: #{Exception.message(error)}"}
  end

  @doc false
  def canonical_entry(entry) do
    with severity when is_binary(severity) <- severity(entry),
         metadata when is_binary(metadata) <- canonical_metadata(entry) do
      {:ok,
       %{
         timestamp: TimelessLogs.Timestamp.to_microseconds(entry.timestamp),
         severity: severity,
         message: entry.message,
         metadata: metadata
       }}
    else
      {:error, _} = error -> error
    end
  end

  defp text_column(entries, mapper) do
    Enum.reduce_while(entries, {:ok, []}, fn entry, {:ok, acc} ->
      case mapper.(entry) do
        value when is_binary(value) ->
          {:cont, {:ok, [[<<byte_size(value)::little-32>>, value] | acc]}}

        {:error, _} = error ->
          {:halt, error}

        value ->
          {:halt, {:error, "batch text value is not UTF-8 text: #{inspect(value)}"}}
      end
    end)
    |> case do
      {:ok, values} -> {:ok, Enum.reverse(values)}
      {:error, _} = error -> error
    end
  end

  defp severity(entry) do
    case entry.level do
      level when is_atom(level) -> Atom.to_string(level)
      level when is_binary(level) -> String.downcase(level)
      level -> {:error, "invalid log severity #{inspect(level)}"}
    end
  end

  defp canonical_metadata(entry) do
    entry.metadata
    |> Kernel.||(%{})
    |> json_value()
    |> :json.encode()
    |> IO.iodata_to_binary()
  rescue
    error -> {:error, "metadata is not canonical JSON: #{Exception.message(error)}"}
  end

  # OTP's JSON encoder treats arbitrary atoms as strings. Translate Elixir's
  # nil to the JSON null sentinel explicitly and reject non-JSON terms rather
  # than silently inspecting them during a fidelity-sensitive migration.
  defp json_value(nil), do: :null
  defp json_value(value) when is_boolean(value), do: value
  defp json_value(value) when is_binary(value) or is_number(value), do: value
  defp json_value(value) when is_atom(value), do: Atom.to_string(value)
  defp json_value(value) when is_list(value), do: Enum.map(value, &json_value/1)

  defp json_value(value) when is_map(value) and not is_struct(value) do
    Map.new(value, fn {key, nested} -> {to_string(key), json_value(nested)} end)
  end

  defp json_value(value), do: raise(ArgumentError, "unsupported JSON term #{inspect(value)}")

  defp initialize_database(conn, capabilities, retention_seconds) do
    statements = [
      "PRAGMA page_size = 16384",
      "PRAGMA journal_mode = WAL",
      "PRAGMA synchronous = NORMAL",
      "PRAGMA auto_vacuum = INCREMENTAL",
      "PRAGMA busy_timeout = 5000",
      logs_create(retention_seconds),
      """
      CREATE TABLE IF NOT EXISTS _timeless_schema_migrations(
        signal TEXT NOT NULL,
        version INTEGER NOT NULL CHECK(version > 0),
        applied_at_unix INTEGER NOT NULL,
        server_version TEXT NOT NULL,
        extension_version TEXT NOT NULL,
        extension_data_abi INTEGER NOT NULL,
        PRIMARY KEY(signal,version)
      ) STRICT
      """
    ]

    with :ok <- execute_all(conn, statements) do
      version = Application.spec(:timeless_logs, :vsn) |> to_string()

      case execute(
             conn,
             "INSERT OR IGNORE INTO _timeless_schema_migrations VALUES ('logs',1,unixepoch(),?1,?2,?3)",
             [version, capabilities["extension_version"], capabilities["data_abi"]]
           ) do
        {:ok, _} -> :ok
        {:error, _} = error -> error
      end
    end
  end

  defp logs_create(nil) do
    "CREATE VIRTUAL TABLE IF NOT EXISTS logs USING timeless_logs(index_keys='service,path,status,host',timestamp_unit='us')"
  end

  defp logs_create(seconds) do
    "CREATE VIRTUAL TABLE IF NOT EXISTS logs USING timeless_logs(index_keys='service,path,status,host',timestamp_unit='us',retention='#{seconds}s')"
  end

  defp validate_retention(nil), do: :ok
  defp validate_retention(seconds) when is_integer(seconds) and seconds > 0, do: :ok
  defp validate_retention(value), do: {:error, "invalid logs retention seconds #{inspect(value)}"}

  defp execute_all(conn, statements) do
    Enum.reduce_while(statements, :ok, fn sql, :ok ->
      case execute(conn, sql) do
        {:ok, _} -> {:cont, :ok}
        {:error, _} = error -> {:halt, error}
      end
    end)
  end

  defp load_extension(conn, path) do
    with :ok <- Exqlite.Sqlite3.enable_load_extension(conn, true),
         {:ok, _} <- execute(conn, "SELECT load_extension(?1)", [path]),
         :ok <- Exqlite.Sqlite3.enable_load_extension(conn, false) do
      :ok
    end
  end

  defp capabilities(conn) do
    with {:ok, [[json]]} <- execute(conn, "SELECT timeless_capabilities()") do
      {:ok, :json.decode(json)}
    else
      other -> {:error, "extension capability handshake failed: #{inspect(other)}"}
    end
  end

  defp require_capability(capabilities) do
    batches = get_in(capabilities, ["signals", "logs", "batches"]) || []

    if capabilities["data_abi"] == 1 and "rich-v1" in batches do
      :ok
    else
      {:error, "extension lacks logs rich-v1/data ABI 1 capability"}
    end
  end

  defp execute(conn, sql, params \\ []) do
    TimelessLogs.DB.execute(conn, sql, params)
  end

  defp failpoint!(configured, configured),
    do: raise("injected migration failure at #{configured}")

  defp failpoint!(_configured, _point), do: :ok

  defp extension_path! do
    System.get_env("TIMELESS_EXT_PATH") ||
      Application.get_env(:timeless_logs, :extension_path) ||
      raise "TIMELESS_EXT_PATH or :timeless_logs, :extension_path is required for libSQL migration"
  end
end
