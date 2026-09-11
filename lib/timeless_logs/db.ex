defmodule TimelessLogs.DB do
  @moduledoc """
  SQLite connection manager with a single writer and pooled readers.

  Uses WAL mode for concurrent reads during writes. The writer is serialized
  through a GenServer to respect SQLite's single-writer constraint.
  """

  use GenServer

  defstruct [:writer, :readers, :reader_counter, :data_dir, :db_path, :name]

  @max_retries 8

  def start_link(opts) do
    name = Keyword.fetch!(opts, :name)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @doc "Execute a write query (INSERT, UPDATE, DELETE) through the serialized writer."
  def write(db, sql, params \\ []) do
    GenServer.call(db, {:write, sql, params}, :infinity)
  end

  @doc "Execute multiple write queries in a single transaction."
  def write_transaction(db, fun) when is_function(fun, 1) do
    GenServer.call(db, {:write_transaction, fun}, :infinity)
  end

  @doc "Execute a read query using a reader connection from the pool."
  def read(db, sql, params \\ []) do
    case :persistent_term.get(reader_pool_key(db), nil) do
      {readers, counter} when tuple_size(readers) > 0 ->
        index = rem(:atomics.add_get(counter, 1, 1) - 1, tuple_size(readers))
        readers |> elem(index) |> TimelessLogs.DB.Reader.read(sql, params)

      _ ->
        # Startup/restart fallback. In normal operation callers bypass the
        # writer GenServer and fan out directly across reader workers.
        GenServer.call(db, {:read, sql, params}, :infinity)
    end
  end

  @doc "Get the database path."
  def db_path(db) do
    GenServer.call(db, :db_path)
  end

  @doc "Create a consistent backup of this database using VACUUM INTO."
  def backup(db, target_path) do
    GenServer.call(db, {:backup, target_path}, :infinity)
  end

  # --- Server ---

  @impl true
  def init(opts) do
    data_dir = Keyword.fetch!(opts, :data_dir)
    name = Keyword.fetch!(opts, :name)
    File.mkdir_p!(data_dir)

    db_path = Path.join(data_dir, "logs_index.db")

    writer = open_with_retry(db_path, @max_retries)
    configure_connection(writer)
    run_migrations(writer)

    if Keyword.get(opts, :clean, false) do
      clean_all_tables(writer)
    end

    default_readers =
      case System.get_env("CI") do
        nil -> System.schedulers_online()
        _ -> 1
      end

    reader_count = Keyword.get(opts, :reader_pool_size, default_readers)

    readers =
      for _ <- 1..reader_count do
        {:ok, reader} = TimelessLogs.DB.Reader.start_link(db_path)
        reader
      end

    reader_counter = :atomics.new(1, signed: false)
    :atomics.put(reader_counter, 1, 0)
    :persistent_term.put(reader_pool_key(name), {List.to_tuple(readers), reader_counter})

    state = %__MODULE__{
      writer: writer,
      readers: readers,
      reader_counter: reader_counter,
      data_dir: data_dir,
      db_path: db_path,
      name: name
    }

    {:ok, state}
  end

  @impl true
  def handle_call({:write, sql, params}, _from, state) do
    result = execute(state.writer, sql, params)
    {:reply, result, state}
  end

  def handle_call({:write_transaction, fun}, _from, state) do
    try do
      {:ok, _} = execute(state.writer, "BEGIN IMMEDIATE", [])
      result = fun.(state.writer)
      {:ok, _} = execute(state.writer, "COMMIT", [])
      {:reply, {:ok, result}, state}
    rescue
      e ->
        safe_rollback(state.writer)
        {:reply, {:error, e}, state}
    catch
      kind, reason ->
        safe_rollback(state.writer)
        {:reply, {:error, {kind, reason}}, state}
    end
  end

  def handle_call({:read, sql, params}, _from, state) do
    index = rem(:atomics.add_get(state.reader_counter, 1, 1) - 1, length(state.readers))
    reader = Enum.at(state.readers, index)
    result = TimelessLogs.DB.Reader.read(reader, sql, params)
    {:reply, result, state}
  end

  def handle_call(:db_path, _from, state) do
    {:reply, state.db_path, state}
  end

  def handle_call({:backup, target_path}, _from, state) do
    result = execute(state.writer, "VACUUM INTO ?1", [target_path])
    {:reply, result, state}
  end

  @impl true
  def terminate(_reason, state) do
    :persistent_term.erase(reader_pool_key(state.name))
    Exqlite.Sqlite3.close(state.writer)

    Enum.each(state.readers, fn reader ->
      if Process.alive?(reader), do: GenServer.stop(reader, :normal, :infinity)
    end)
  end

  # --- Internals ---

  defp open_with_retry(path, retries) do
    case Exqlite.Sqlite3.open(path) do
      {:ok, conn} ->
        conn

      {:error, _reason} when retries > 0 ->
        Process.sleep(retry_backoff(@max_retries - retries))
        open_with_retry(path, retries - 1)

      {:error, reason} ->
        raise "failed to open SQLite database #{path}: #{inspect(reason)}"
    end
  end

  defp configure_connection(conn) do
    pragmas = [
      "PRAGMA page_size = 16384",
      "PRAGMA journal_mode = WAL",
      "PRAGMA synchronous = NORMAL",
      "PRAGMA cache_size = -128000",
      "PRAGMA auto_vacuum = INCREMENTAL",
      "PRAGMA mmap_size = #{mmap_size()}",
      "PRAGMA wal_autocheckpoint = 10000",
      "PRAGMA temp_store = MEMORY",
      "PRAGMA busy_timeout = 5000"
    ]

    Enum.each(pragmas, &execute(conn, &1, []))
  end

  @doc false
  def open_and_configure_reader(db_path, attempts \\ 5) do
    conn = open_with_retry(db_path, @max_retries)

    try do
      configure_reader(conn)
      conn
    rescue
      e ->
        Exqlite.Sqlite3.close(conn)

        if attempts > 1 do
          Process.sleep(200 * (6 - attempts))
          open_and_configure_reader(db_path, attempts - 1)
        else
          reraise e, __STACKTRACE__
        end
    end
  end

  defp configure_reader(conn) do
    pragmas = [
      "PRAGMA mmap_size = #{mmap_size()}",
      "PRAGMA cache_size = -8000",
      "PRAGMA temp_store = MEMORY",
      "PRAGMA busy_timeout = 5000"
    ]

    Enum.each(pragmas, &execute(conn, &1, []))
  end

  # 2GB mmap on real systems, disabled on CI/overlay filesystems
  defp mmap_size do
    case System.get_env("CI") do
      nil -> 2_147_483_648
      _ -> 0
    end
  end

  defp run_migrations(conn) do
    TimelessLogs.DB.Migrations.run(conn)
  end

  defp clean_all_tables(conn) do
    for table <- ["blocks", "term_index", "compression_stats", "block_data"] do
      execute(conn, "DELETE FROM #{table}", [])
    end
  end

  @doc false
  def execute(conn, sql, params) do
    execute_with_retry(conn, sql, params, @max_retries)
  end

  @doc "Prepare once, execute many times with different params. Use inside write_transaction."
  def execute_batch(conn, sql, params_list) when is_list(params_list) do
    case Exqlite.Sqlite3.prepare(conn, sql) do
      {:ok, stmt} ->
        try do
          Enum.each(params_list, fn params ->
            :ok = Exqlite.Sqlite3.bind(stmt, params)
            :done = Exqlite.Sqlite3.step(conn, stmt)
            :ok = Exqlite.Sqlite3.reset(stmt)
          end)

          :ok
        after
          Exqlite.Sqlite3.release(conn, stmt)
        end

      {:error, reason} ->
        raise "SQLite execute_batch failed: #{inspect(reason)} (sql: #{sql})"
    end
  end

  defp execute_with_retry(conn, sql, params, retries) do
    case Exqlite.Sqlite3.prepare(conn, sql) do
      {:ok, stmt} ->
        try do
          if params != [] do
            :ok = Exqlite.Sqlite3.bind(stmt, params)
          end

          {:ok, fetch_all(conn, stmt, [])}
        after
          Exqlite.Sqlite3.release(conn, stmt)
        end

      {:error, _reason} when retries > 0 ->
        Process.sleep(retry_backoff(@max_retries - retries))
        execute_with_retry(conn, sql, params, retries - 1)

      {:error, reason} ->
        raise "SQLite execute failed after retries: #{inspect(reason)} (sql: #{sql})"
    end
  end

  defp fetch_all(conn, stmt, acc) do
    case Exqlite.Sqlite3.step(conn, stmt) do
      {:row, row} -> fetch_all(conn, stmt, [row | acc])
      :done -> Enum.reverse(acc)
      {:error, reason} -> raise "SQLite step failed: #{inspect(reason)}"
    end
  end

  # Exponential backoff: 100, 200, 400, 800, 1600, 3200, 6400, 12800ms
  defp retry_backoff(attempt), do: 100 * Integer.pow(2, attempt)

  defp safe_rollback(conn) do
    execute(conn, "ROLLBACK", [])
  rescue
    _ -> :ok
  end

  defp reader_pool_key(db), do: {__MODULE__, :reader_pool, db}
end

defmodule TimelessLogs.DB.Reader do
  @moduledoc false

  use GenServer

  def start_link(db_path), do: GenServer.start_link(__MODULE__, db_path)

  def read(reader, sql, params) do
    GenServer.call(reader, {:read, sql, params}, :infinity)
  end

  @impl true
  def init(db_path) do
    {:ok, TimelessLogs.DB.open_and_configure_reader(db_path)}
  end

  @impl true
  def handle_call({:read, sql, params}, _from, conn) do
    {:reply, TimelessLogs.DB.execute(conn, sql, params), conn}
  end

  @impl true
  def terminate(_reason, conn) do
    Exqlite.Sqlite3.close(conn)
  end
end
