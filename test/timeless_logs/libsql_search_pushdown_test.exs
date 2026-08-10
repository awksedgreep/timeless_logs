defmodule TimelessLogs.LibsqlSearchPushdownTest do
  @moduledoc """
  Message and metadata search on the libSQL engine.

  `:message` used to also match any metadata value. That made it impossible to
  push into the storage engine, which matches the message, so every search
  decoded the whole store and filtered in Elixir — measured at roughly 0.8s per
  200k entries, several seconds against a real store, on every submit.

  `:message` is now message-only and pushes down; metadata is searched with
  `:metadata`, which pushes down through the indexed key columns. These tests
  pin both halves of that split, since the whole point is that each predicate
  reaches the engine.
  """

  use ExUnit.Case, async: false

  @data_dir "test/tmp/libsql_search_pushdown"
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

    base = System.os_time(:microsecond)

    :ok =
      TimelessLogs.ingest([
        %{
          timestamp: base,
          level: :info,
          message: "checkout completed",
          metadata: %{"service" => "api", "host" => "web-1"}
        },
        %{
          timestamp: base + 1,
          level: :error,
          message: "checkout failed",
          metadata: %{"service" => "api", "host" => "web-2"}
        },
        %{
          timestamp: base + 2,
          level: :info,
          message: "unrelated work",
          metadata: %{"service" => "worker", "host" => "web-1"}
        }
      ])

    :ok = TimelessLogs.flush()

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

  defp messages(filters) do
    {:ok, %TimelessLogs.Result{entries: entries}} = TimelessLogs.query(filters)
    entries |> Enum.map(& &1.message) |> Enum.sort()
  end

  describe "message search" do
    test "matches the message" do
      assert messages(message: "checkout") == ["checkout completed", "checkout failed"]
    end

    test "is case-insensitive" do
      assert messages(message: "CHECKOUT") == ["checkout completed", "checkout failed"]
    end

    test "does not match metadata values" do
      # "worker" appears only as a metadata value, never in a message. It used
      # to match, which is what made the predicate unpushable.
      assert messages(message: "worker") == []
    end

    test "combines with level" do
      assert messages(message: "checkout", level: :error) == ["checkout failed"]
    end
  end

  describe "metadata search" do
    test "an indexed key matches exactly" do
      assert messages(metadata: %{"service" => "worker"}) == ["unrelated work"]
    end

    test "another indexed key narrows differently" do
      assert messages(metadata: %{"host" => "web-1"}) == ["checkout completed", "unrelated work"]
    end

    test "several keys must all match" do
      assert messages(metadata: %{"service" => "api", "host" => "web-2"}) == ["checkout failed"]
    end

    test "a value that matches nothing returns nothing" do
      assert messages(metadata: %{"service" => "nope"}) == []
    end

    test "an unindexed key still filters, just without pushdown" do
      :ok =
        TimelessLogs.ingest([
          %{
            timestamp: System.os_time(:microsecond),
            level: :info,
            message: "tagged entry",
            metadata: %{"request_id" => "abc123"}
          }
        ])

      :ok = TimelessLogs.flush()

      assert messages(metadata: %{"request_id" => "abc123"}) == ["tagged entry"]
    end
  end

  describe "the two searches compose" do
    test "message and metadata together" do
      assert messages(message: "checkout", metadata: %{"host" => "web-2"}) == ["checkout failed"]
    end

    test "a message hit outside the metadata match is excluded" do
      assert messages(message: "checkout", metadata: %{"service" => "worker"}) == []
    end
  end
end
