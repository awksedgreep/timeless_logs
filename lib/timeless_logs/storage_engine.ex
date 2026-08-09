defmodule TimelessLogs.StorageEngine do
  @moduledoc false
  # Engine dispatch seam (the timeless_metrics storage_engine.ex pattern,
  # app-scoped because timeless_logs is a singleton store). The supervisor
  # records the running engine in :persistent_term at startup; the facade
  # routes through here so the deprecated Elixir block engine and the
  # libSQL engine stay swappable behind one public API.
  #
  # Session 1 routes the WRITE path only; reads grow here in Session 2 of
  # notes/libsql_engine_port_plan_2026-08-09.md.

  def engine, do: :persistent_term.get({TimelessLogs, :engine}, :elixir)

  @doc false
  def put_engine(engine) when engine in [:elixir, :libsql],
    do: :persistent_term.put({TimelessLogs, :engine}, engine)

  def ingest(entries) do
    case engine() do
      :libsql ->
        with :ok <- TimelessLogs.LibsqlEngine.ingest(entries) do
          # The Elixir engine broadcasts from inside Buffer; the libSQL
          # path publishes here so subscribers see one stream either way.
          Enum.each(entries, &broadcast_to_subscribers/1)
          :ok
        end

      _ ->
        TimelessLogs.Buffer.log_many(entries)
    end
  end

  @doc false
  # Single-entry ingest for the Logger handler.
  #
  # The handler used to call Buffer.log/1 directly, which meant that under
  # engine: :libsql it cast to shard processes libsql_children/0 never starts.
  # GenServer.cast to an unregistered name returns :ok, so every log line was
  # silently discarded and no subscriber was ever notified.
  #
  # The elixir branch stays on Buffer.log/1 rather than ingest/1's log_many/1:
  # log_many/1 can block the caller on backpressure, and a Logger handler runs
  # in the calling process. Preserving log/1 here keeps that path unchanged.
  def ingest_one(entry) do
    case engine() do
      :libsql -> libsql_ingest_one(entry)
      _ -> TimelessLogs.Buffer.log(entry)
    end
  rescue
    _ -> :ok
  catch
    # `:logger` removes a handler that raises or exits, permanently. The engine
    # is a GenServer, so it can be starting, restarting or already stopped when
    # an event arrives — a shutdown notice reaching a dead engine exits with
    # :noproc and would take log capture down with it.
    #
    # Dropping the odd line while the engine is unavailable is bad. Losing the
    # handler is unrecoverable, so this stays total on purpose.
    :exit, _ -> :ok
    _, _ -> :ok
  end

  # The engine logs from inside its own process (startup banner, retention,
  # recovery). Those events reach this handler while running *as* the engine,
  # so persisting them would be a GenServer.call to self: it exits with
  # :calling_self, and `:logger` responds by removing the handler altogether.
  # One lost line is survivable; a removed handler means no logs at all.
  #
  # Subscribers still see the entry, so live tail stays complete.
  defp libsql_ingest_one(entry) do
    if self() == Process.whereis(TimelessLogs.LibsqlEngine) do
      broadcast_to_subscribers(entry)
      :ok
    else
      with :ok <- TimelessLogs.LibsqlEngine.ingest([entry]) do
        broadcast_to_subscribers(entry)
        :ok
      end
    end
  end

  defp broadcast_to_subscribers(entry) do
    entry_struct = TimelessLogs.Entry.from_map(entry)

    Registry.dispatch(TimelessLogs.Registry, :log_entries, fn subscribers ->
      for {pid, opts} <- subscribers do
        if opts == [] or TimelessLogs.Filter.matches?(entry, opts) do
          send(pid, {:timeless_logs, :entry, entry_struct})
        end
      end
    end)
  end

  def flush do
    case engine() do
      :libsql -> TimelessLogs.LibsqlEngine.flush()
      _ -> TimelessLogs.Buffer.flush()
    end
  end

  def query(filters) do
    case engine() do
      :libsql -> TimelessLogs.LibsqlEngine.query(filters)
      _ -> TimelessLogs.Index.query(filters)
    end
  end

  def count(filters) do
    case engine() do
      :libsql -> TimelessLogs.LibsqlEngine.count(filters)
      _ -> TimelessLogs.Index.count(filters)
    end
  end

  @stream_page 1_000

  # libSQL streaming is offset-paged over the engine's query (ascending,
  # page size #{@stream_page}); the Elixir engine's block-based stream
  # stays in the facade path. Like the legacy stream, a concurrent write
  # can shift late pages — both engines share that caveat.
  def stream(filters) do
    case engine() do
      :libsql ->
        search = Keyword.drop(filters, [:limit, :offset, :order])

        Stream.resource(
          fn -> 0 end,
          fn offset ->
            case TimelessLogs.LibsqlEngine.query(
                   Keyword.merge(search, limit: @stream_page, offset: offset, order: :asc)
                 ) do
              {:ok, %TimelessLogs.Result{entries: []}} -> {:halt, offset}
              {:ok, %TimelessLogs.Result{entries: entries}} -> {entries, offset + length(entries)}
              {:error, _} -> {:halt, offset}
            end
          end,
          fn _ -> :ok end
        )

      _ ->
        TimelessLogs.legacy_stream(filters)
    end
  end

  def merge_now do
    case engine() do
      :libsql ->
        case TimelessLogs.LibsqlEngine.optimize() do
          {:ok, _} -> :ok
          other -> other
        end

      _ ->
        TimelessLogs.Compactor.merge_now()
    end
  end

  def stats do
    case engine() do
      :libsql -> TimelessLogs.LibsqlEngine.stats()
      _ -> TimelessLogs.Index.stats()
    end
  end
end
