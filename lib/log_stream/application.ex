defmodule LogStream.Application do
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    storage = LogStream.Config.storage()
    data_dir = LogStream.Config.data_dir()

    if storage == :disk do
      blocks_dir = Path.join(data_dir, "blocks")
      File.mkdir_p!(blocks_dir)
    end

    children = [
      {Registry, keys: :duplicate, name: LogStream.Registry},
      {LogStream.Index, data_dir: data_dir, storage: storage},
      {LogStream.Buffer, data_dir: data_dir},
      {LogStream.Compactor, data_dir: data_dir, storage: storage},
      {LogStream.Retention, []}
    ]

    opts = [strategy: :one_for_one, name: LogStream.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
