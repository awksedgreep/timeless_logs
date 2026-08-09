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
