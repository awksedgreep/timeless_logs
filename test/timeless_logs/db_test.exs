defmodule TimelessLogs.DBTest do
  use ExUnit.Case, async: false

  test "reads bypass the writer GenServer and use the reader pool concurrently" do
    unique = System.unique_integer([:positive])
    data_dir = "test/tmp/db_reader_pool_#{unique}"
    name = TimelessLogs.DBTest.ReaderPool
    File.rm_rf!(data_dir)

    {:ok, db} =
      TimelessLogs.DB.start_link(
        name: name,
        data_dir: data_dir,
        clean: true,
        reader_pool_size: 2
      )

    on_exit(fn ->
      if Process.alive?(db), do: GenServer.stop(db)
      File.rm_rf!(data_dir)
    end)

    :sys.suspend(db)

    try do
      task = Task.async(fn -> TimelessLogs.DB.read(name, "SELECT 42") end)
      assert Task.await(task, 1_000) == {:ok, [[42]]}
    after
      :sys.resume(db)
    end
  end
end
