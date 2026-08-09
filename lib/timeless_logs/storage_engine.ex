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
      :libsql -> TimelessLogs.LibsqlEngine.ingest(entries)
      _ -> TimelessLogs.Buffer.log_many(entries)
    end
  end

  def flush do
    case engine() do
      :libsql -> TimelessLogs.LibsqlEngine.flush()
      _ -> TimelessLogs.Buffer.flush()
    end
  end
end
