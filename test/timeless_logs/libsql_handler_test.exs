defmodule TimelessLogs.LibsqlHandlerTest do
  @moduledoc """
  The Logger handler on `engine: :libsql`.

  `libsql_facade_test.exs` covers the engine through `TimelessLogs.ingest/1`.
  Production does not capture logs that way — the `:logger` handler does, and
  it used to call `Buffer.log/1` directly, casting to shard processes that
  `libsql_children/0` never starts. `GenServer.cast` to an unregistered name
  returns `:ok`, so entries vanished and no subscriber was notified.

  Every test here goes through `TimelessLogs.Handler.log/2` rather than the
  facade, because that one function call is where the coverage stopped and the
  bug lived.
  """

  use ExUnit.Case, async: false

  require Logger

  @data_dir "test/tmp/libsql_handler"
  @extension System.get_env("TIMELESS_EXT_PATH") ||
               Path.expand("../../../timeless-libsql/target/release/libtimeless_ext.so", __DIR__)

  setup do
    Application.stop(:timeless_logs)
    File.rm_rf!(@data_dir)

    previous = %{
      engine: Application.get_env(:timeless_logs, :engine),
      data_dir: Application.get_env(:timeless_logs, :data_dir),
      extension_path: Application.get_env(:timeless_logs, :extension_path)
    }

    Application.put_env(:timeless_logs, :engine, :libsql)
    Application.put_env(:timeless_logs, :data_dir, @data_dir)
    Application.put_env(:timeless_logs, :extension_path, @extension)
    {:ok, _} = Application.ensure_all_started(:timeless_logs)

    on_exit(fn ->
      Application.stop(:timeless_logs)

      for {key, value} <- previous do
        case value do
          nil -> Application.delete_env(:timeless_logs, key)
          _ -> Application.put_env(:timeless_logs, key, value)
        end
      end

      File.rm_rf!(@data_dir)
      {:ok, _} = Application.ensure_all_started(:timeless_logs)
    end)

    :ok
  end

  defp handler_log(level, message, meta \\ %{}) do
    event = %{
      level: level,
      msg: {:string, message},
      meta: Map.put_new(meta, :time, System.os_time(:microsecond))
    }

    TimelessLogs.Handler.log(event, %{})
  end

  test "handler-written entries are persisted and queryable" do
    :ok = handler_log(:error, "handler boom", %{service: "api"})
    :ok = handler_log(:info, "handler hello", %{service: "api"})
    :ok = TimelessLogs.flush()

    assert {:ok, %TimelessLogs.Result{entries: [entry], total: 1}} =
             TimelessLogs.query(level: :error)

    assert entry.message == "handler boom"

    assert {:ok, %TimelessLogs.Stats{total_entries: 2}} = TimelessLogs.stats()
  end

  test "handler-written entries reach subscribers" do
    assert :ok = TimelessLogsDashboardContract.subscribe()

    :ok = handler_log(:error, "tail me")

    assert_receive {:timeless_logs, :entry, %TimelessLogs.Entry{message: "tail me"}}, 1_000
  end

  test "subscriber filters still apply to handler-written entries" do
    {:ok, _} = TimelessLogs.subscribe(level: :error)

    :ok = handler_log(:info, "not for you")
    :ok = handler_log(:error, "for you")

    assert_receive {:timeless_logs, :entry, %TimelessLogs.Entry{message: "for you"}}, 1_000
    refute_receive {:timeless_logs, :entry, %TimelessLogs.Entry{message: "not for you"}}, 100
  end

  test "nothing is silently dropped: every handler write lands" do
    for i <- 1..25, do: :ok = handler_log(:info, "entry #{i}")
    :ok = TimelessLogs.flush()

    assert {:ok, %TimelessLogs.Stats{total_entries: 25}} = TimelessLogs.stats()
  end

  describe "the handler survives :logger" do
    # The tests above call Handler.log/2 directly, which cannot observe
    # :logger removing a failing handler. These go through Logger itself.
    #
    # The engine logs from inside its own process, and persisting those events
    # would be a GenServer.call to self. That exits with :calling_self, and
    # :logger removes the handler outright — leaving the store with no input at
    # all, which is worse than the bug this file was written for.

    test "is still attached after the engine logs from its own process" do
      assert attached?(), "handler was not attached at test start"

      # Force the engine to emit from inside itself.
      :ok = TimelessLogs.merge_now()
      Logger.info("after engine work")
      :ok = TimelessLogs.flush()

      assert attached?(), ":logger removed the handler — ingestion is dead"
    end

    test "entries logged through Logger are persisted" do
      Logger.error("through logger")
      :ok = TimelessLogs.flush()

      assert {:ok, %TimelessLogs.Result{entries: entries}} = TimelessLogs.query(level: :error)
      assert Enum.any?(entries, &(&1.message =~ "through logger"))
    end
  end

  defp attached? do
    :logger.get_handler_config()
    |> Enum.any?(fn %{id: id} -> id == TimelessLogs.Handler.handler_id() end)
  end
end

defmodule TimelessLogsDashboardContract do
  @moduledoc false
  # Mirrors what a consumer expecting the documented `:ok | {:error, term()}`
  # contract does. TimelessLogs.subscribe/1 answers {:ok, pid} from
  # Registry.register/3, and a consumer matching only `:ok` crashed on the
  # success path — the logs dashboard did exactly this.
  def subscribe do
    case TimelessLogs.subscribe() do
      {:ok, _pid} -> :ok
      {:error, {:already_registered, _pid}} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end
end
