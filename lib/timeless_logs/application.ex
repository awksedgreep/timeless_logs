defmodule TimelessLogs.Application do
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    opts = [strategy: :one_for_one, name: TimelessLogs.Supervisor]
    Supervisor.start_link(configured_children(), opts)
  end

  @doc false
  def configured_children(owner \\ Application.get_env(:timeless_logs, :owner, :embedded))

  def configured_children(:external), do: []

  def configured_children(:embedded) do
    case TimelessLogs.Config.engine() do
      :libsql -> libsql_children()
      _ -> elixir_children()
    end
  end

  # Opt-in libSQL engine (port plan Session 1): one in-process writer over
  # the timeless-libsql vtab. Subscriptions keep their Registry; the
  # legacy buffer/index/compactor/hot-tail pipeline does not start.
  defp libsql_children do
    TimelessLogs.StorageEngine.put_engine(:libsql)

    [
      {Registry, keys: :duplicate, name: TimelessLogs.Registry},
      {TimelessLogs.LibsqlEngine, []}
    ]
  end

  defp elixir_children do
    TimelessLogs.StorageEngine.put_engine(:elixir)
    storage = TimelessLogs.Config.storage()
    data_dir = TimelessLogs.Config.data_dir()

    if storage == :disk do
      blocks_dir = Path.join(data_dir, "blocks")
      File.mkdir_p!(blocks_dir)
    end

    TimelessLogs.IngestPressure.install(TimelessLogs.BufferShard.count())

    [
      {Registry, keys: :duplicate, name: TimelessLogs.Registry},
      {TimelessLogs.DB, name: TimelessLogs.DB, data_dir: data_dir, clean: storage == :memory},
      {TimelessLogs.Index, data_dir: data_dir, storage: storage, db: TimelessLogs.DB},
      {Task.Supervisor, name: TimelessLogs.FlushSupervisor},
      {TimelessLogs.Compactor, data_dir: data_dir, storage: storage},
      {TimelessLogs.Retention, []}
    ] ++ hot_tail_child() ++ buffer_shards(data_dir) ++ http_child()
  end

  def configured_children(owner) do
    raise ArgumentError,
          "invalid :timeless_logs :owner #{inspect(owner)}; expected :embedded or :external"
  end

  defp hot_tail_child do
    if TimelessLogs.Config.hot_tail?(), do: [{TimelessLogs.HotTail, []}], else: []
  end

  defp http_child do
    case Application.get_env(:timeless_logs, :http, false) do
      false -> []
      true -> [{TimelessLogs.HTTP, []}]
      opts when is_list(opts) -> [{TimelessLogs.HTTP, opts}]
    end
  end

  defp buffer_shards(data_dir) do
    for shard <- 0..(TimelessLogs.BufferShard.count() - 1) do
      Supervisor.child_spec(
        {TimelessLogs.Buffer,
         data_dir: data_dir, shard: shard, name: TimelessLogs.BufferShard.name(shard)},
        id: {:timeless_logs_buffer, shard}
      )
    end
  end
end
