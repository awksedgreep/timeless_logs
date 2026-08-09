defmodule TimelessLogs.LibsqlEngine do
  @moduledoc """
  In-process storage engine over the timeless-libsql `timeless_logs`
  virtual table — Session 1 of the port plan
  (`notes/libsql_engine_port_plan_2026-08-09.md`).

  Opt-in via `config :timeless_logs, engine: :libsql`. The writer owns
  `<data_dir>/logs.db`: ingest encodes public rich-v1 batches (the
  migration candidate's validated encoder) and control commands ride the
  vtab's shadow-name channel — the same public surface the external Rust
  owner uses, so embedded and external share one on-disk format.

  Session 1 scope is the write path (ingest/flush/optimize) plus raw SQL
  access; the query surface routes here in Session 2.
  """

  use GenServer

  require Logger

  alias TimelessLogs.LibsqlCandidate

  @table "logs"

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts, name: __MODULE__)

  @doc "Ingest normalized entries (`%{timestamp:, level:, message:, metadata:}`)."
  def ingest([]), do: :ok
  def ingest(entries), do: GenServer.call(__MODULE__, {:ingest, entries}, :infinity)

  @doc "Persist buffered entries into blocks now."
  def flush, do: command("flush")

  @doc "Run a bounded background optimize pass."
  def optimize, do: command("optimize")

  @doc false
  def sql(sql, params \\ []), do: GenServer.call(__MODULE__, {:sql, sql, params}, :infinity)

  defp command(cmd),
    do:
      GenServer.call(
        __MODULE__,
        {:sql, "INSERT INTO #{@table}(#{@table}) VALUES (?1)", [cmd]},
        :infinity
      )

  @impl true
  def init(opts) do
    # Trap exits so terminate/2 (final flush + close) runs on ordinary
    # supervisor shutdown — the metrics engine's shutdown-durability
    # lesson, applied from day one.
    Process.flag(:trap_exit, true)
    data_dir = Keyword.get(opts, :data_dir, TimelessLogs.Config.data_dir())
    reject_unmigrated_legacy_store!(data_dir)
    File.mkdir_p!(data_dir)
    path = Path.join(data_dir, "logs.db")

    retention_seconds =
      Keyword.get(opts, :retention_seconds, TimelessLogs.Config.retention_max_age())

    with {:ok, conn, capabilities} <-
           LibsqlCandidate.open_connection(path, Keyword.get(opts, :extension_path)),
         :ok <- LibsqlCandidate.initialize_database(conn, capabilities, retention_seconds) do
      Logger.info(
        "timeless_logs libSQL engine: extension #{capabilities["extension_version"]} " <>
          "(data ABI #{capabilities["data_abi"]}) on #{path}"
      )

      flush_timer = schedule_flush()
      {:ok, %{conn: conn, path: path, flush_timer: flush_timer}}
    else
      {:error, reason} -> {:stop, reason}
    end
  end

  @impl true
  def handle_call({:ingest, entries}, _from, state) do
    result =
      with {:ok, blob} <- LibsqlCandidate.encode_batch(entries),
           {:ok, _} <-
             LibsqlCandidate.execute(
               state.conn,
               "INSERT INTO #{@table}(#{@table}) VALUES (?1)",
               [{:blob, blob}]
             ) do
        :ok
      end

    {:reply, result, state}
  end

  def handle_call({:sql, sql, params}, _from, state) do
    {:reply, LibsqlCandidate.execute(state.conn, sql, params), state}
  end

  @impl true
  def handle_info(:flush, state) do
    _ = LibsqlCandidate.execute(state.conn, "INSERT INTO #{@table}(#{@table}) VALUES ('flush')")
    {:noreply, %{state | flush_timer: schedule_flush()}}
  end

  def handle_info(_msg, state), do: {:noreply, state}

  @impl true
  def terminate(_reason, state) do
    _ = LibsqlCandidate.execute(state.conn, "INSERT INTO #{@table}(#{@table}) VALUES ('flush')")
    Exqlite.Sqlite3.close(state.conn)
  end

  defp schedule_flush do
    Process.send_after(self(), :flush, TimelessLogs.Config.flush_interval())
  end

  # A data_dir carrying the legacy block-store layout must be converted
  # with TimelessLogs.ReleaseMigration before the libSQL engine will
  # touch it — never silently ignore existing data (the metrics
  # reject_unmigrated_rust_store! precedent).
  defp reject_unmigrated_legacy_store!(data_dir) do
    legacy? =
      File.exists?(Path.join(data_dir, "logs_index.db")) or
        File.dir?(Path.join(data_dir, "blocks"))

    migrated? = File.exists?(Path.join(data_dir, "logs.db"))

    if legacy? and not migrated? do
      raise "timeless_logs engine: :libsql refuses to start against the unmigrated " <>
              "legacy block store in #{data_dir} — run the TimelessLogs.ReleaseMigration " <>
              "conversion first, or configure engine: :elixir"
    end

    :ok
  end
end
