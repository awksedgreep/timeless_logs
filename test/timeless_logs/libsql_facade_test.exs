defmodule TimelessLogs.LibsqlFacadeTest do
  # The whole public facade running on engine: :libsql — the port's
  # engine-contract test. Serial: restarts the OTP app with swapped env.
  use ExUnit.Case, async: false

  @data_dir "test/tmp/libsql_facade"
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

  test "the public facade works end to end on the libSQL engine" do
    assert {:ok, _} = TimelessLogs.subscribe(level: :error)

    base = System.os_time(:microsecond)

    :ok =
      TimelessLogs.ingest([
        %{timestamp: base, level: :info, message: "hello world", metadata: %{"service" => "api"}},
        %{timestamp: base + 1, level: :error, message: "boom", metadata: %{"service" => "api"}},
        %{timestamp: base + 2, level: :info, message: "bye", metadata: %{"service" => "web"}}
      ])

    # Subscribers see only their filtered stream, as %Entry{} structs.
    assert_receive {:timeless_logs, :entry, %TimelessLogs.Entry{level: :error, message: "boom"}}
    refute_receive {:timeless_logs, :entry, %TimelessLogs.Entry{message: "hello world"}}, 50

    :ok = TimelessLogs.unsubscribe()

    assert :ok = TimelessLogs.flush()

    # query / count through the facade.
    assert {:ok, %TimelessLogs.Result{entries: [error_entry], total: 1}} =
             TimelessLogs.query(level: :error)

    assert error_entry.metadata["service"] == "api"
    assert {:ok, 2} = TimelessLogs.count(metadata: %{"service" => "api"})

    # stream + the stream-derived field aggregations.
    assert TimelessLogs.stream(metadata: %{"service" => "api"}) |> Enum.count() == 2

    assert {:ok, values} = TimelessLogs.field_values("service")
    assert %{"value" => "api", "hits" => 2} in values
    assert %{"value" => "web", "hits" => 1} in values

    assert {:ok, names} = TimelessLogs.field_names()
    assert Enum.any?(names, &(&1["value"] == "_msg" and &1["hits"] == 3))

    # stats, maintenance, backup.
    assert {:ok, %TimelessLogs.Stats{total_entries: 3}} = TimelessLogs.stats()
    assert :ok = TimelessLogs.merge_now()

    backup_dir = Path.join(@data_dir, "backup")
    assert {:ok, %{files: ["logs.db"], total_bytes: bytes}} = TimelessLogs.backup(backup_dir)
    assert bytes > 0
    assert File.exists?(Path.join(backup_dir, "logs.db"))
  end
end
