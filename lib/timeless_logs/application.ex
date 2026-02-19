defmodule TimelessLogs.Application do
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    storage = TimelessLogs.Config.storage()
    data_dir = TimelessLogs.Config.data_dir()

    if storage == :disk do
      blocks_dir = Path.join(data_dir, "blocks")
      File.mkdir_p!(blocks_dir)
    end

    children =
      [
        {Registry, keys: :duplicate, name: TimelessLogs.Registry},
        {TimelessLogs.Index, data_dir: data_dir, storage: storage},
        {Task.Supervisor, name: TimelessLogs.FlushSupervisor},
        {TimelessLogs.Buffer, data_dir: data_dir},
        {TimelessLogs.Compactor, data_dir: data_dir, storage: storage},
        {TimelessLogs.Retention, []}
      ] ++ http_child()

    opts = [strategy: :one_for_one, name: TimelessLogs.Supervisor]
    Supervisor.start_link(children, opts)
  end

  defp http_child do
    case Application.get_env(:timeless_logs, :http, false) do
      false -> []
      true -> [{TimelessLogs.HTTP, []}]
      opts when is_list(opts) -> [{TimelessLogs.HTTP, opts}]
    end
  end
end
