defmodule TimelessLogs.LegacyRelocatedStoreTest do
  @moduledoc """
  Migrating a legacy store whose address changed, or which lost a block.

  `blocks.file_path` is recorded absolute, so a store that moves — restored
  backup, remounted container volume, renamed host path — carries rows pointing
  at where it used to live. The legacy engine rehomes those by basename on
  startup; the migration reader has to agree with it, or a perfectly healthy
  store that merely changed address cannot be converted at all. Because the
  conversion runs during `init/1`, disagreeing does not degrade the migration,
  it stops the application from booting.

  The same applies to a block the index still lists but that is no longer on
  disk. The legacy engine prunes the row and carries on; aborting instead would
  report a stale row as store corruption and tell the operator to restart, which
  can never make the file reappear.

  What must NOT happen is silently converting fewer entries than the store holds,
  so these assert the exact surviving count rather than merely that it booted.
  """

  use ExUnit.Case, async: false

  alias TimelessLogs.LegacyReader

  @data_dir "test/tmp/legacy_relocated"

  setup do
    File.rm_rf!(@data_dir)
    File.mkdir_p!(@data_dir)
    on_exit(fn -> File.rm_rf!(@data_dir) end)
    :ok
  end

  # A legacy SQLite index whose rows point at `recorded_dir`, while the blocks
  # themselves sit under `root/blocks`. When the two differ, the store has moved.
  defp build_store(root, recorded_dir, opts \\ []) do
    blocks_dir = Path.join(root, "blocks")
    File.mkdir_p!(blocks_dir)

    {:ok, conn} = Exqlite.Sqlite3.open(Path.join(root, "logs_index.db"))

    ddl = [
      """
      CREATE TABLE blocks (
        block_id INTEGER PRIMARY KEY, file_path TEXT, byte_size INTEGER NOT NULL,
        entry_count INTEGER NOT NULL, ts_min INTEGER NOT NULL, ts_max INTEGER NOT NULL,
        format TEXT NOT NULL DEFAULT 'zstd', created_at INTEGER NOT NULL)
      """,
      "CREATE TABLE _metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL) WITHOUT ROWID",
      "INSERT INTO _metadata (key, value) VALUES ('schema_version', '2')"
    ]

    for sql <- ddl, do: :ok = Exqlite.Sqlite3.execute(conn, sql)

    missing = Keyword.get(opts, :missing, [])
    base = System.os_time(:nanosecond)

    for id <- 1..3 do
      name = String.pad_leading(Integer.to_string(id), 12, "0") <> ".raw"
      payload = :erlang.term_to_binary([{:entry, id}])

      unless id in missing do
        File.write!(Path.join(blocks_dir, name), payload)
      end

      {:ok, stmt} =
        Exqlite.Sqlite3.prepare(
          conn,
          "INSERT INTO blocks VALUES (?1,?2,?3,?4,?5,?6,?7,?8)"
        )

      :ok =
        Exqlite.Sqlite3.bind(stmt, [
          id,
          Path.join(recorded_dir, name),
          byte_size(payload),
          1,
          base + id,
          base + id + 1,
          "raw",
          System.system_time(:second)
        ])

      :done = Exqlite.Sqlite3.step(conn, stmt)
      :ok = Exqlite.Sqlite3.release(conn, stmt)
    end

    Exqlite.Sqlite3.close(conn)
    root
  end

  defp open!(root) do
    {:ok, reader} = LegacyReader.open(root, generation: :sqlite)
    on_exit(fn -> :ok end)
    reader
  end

  test "a store still sitting at its recorded address is unaffected" do
    root = Path.join(@data_dir, "in_place") |> Path.expand()
    build_store(root, Path.join(root, "blocks"))

    reader = open!(root)
    assert {:ok, %{blocks: 3, records: 3}} = LegacyReader.inventory(reader)
    LegacyReader.close(reader)
  end

  test "a relocated store resolves its blocks instead of reporting a path escape" do
    root = Path.join(@data_dir, "moved") |> Path.expand()
    build_store(root, "/observability/logs/blocks")

    reader = open!(root)

    # Before rehoming this returned zero blocks, because every row resolved
    # outside the new root and was rejected.
    assert {:ok, %{blocks: 3, records: 3}} = LegacyReader.inventory(reader)
    LegacyReader.close(reader)
  end

  test "a block the index lists but disk no longer has is excluded from the inventory" do
    root = Path.join(@data_dir, "gap") |> Path.expand()
    build_store(root, "/observability/logs/blocks", missing: [2])

    reader = open!(root)

    # 2 of 3 blocks survive, so the expected count must drop with them —
    # otherwise the migration's count check reads the gap as corruption.
    assert {:ok, %{blocks: 2, records: 2}} = LegacyReader.inventory(reader)
    LegacyReader.close(reader)
  end

  test "traversal in a recorded path is still confined to the store" do
    root = Path.join(@data_dir, "escape") |> Path.expand()
    build_store(root, "/etc/../../../../etc")

    reader = open!(root)

    # Rehoming takes the basename, so these resolve under root/blocks rather
    # than where the row pointed. Counting 3 is itself the proof: nothing named
    # `000000000001.raw` exists under /etc, so had the traversal been honoured
    # the blocks would have resolved to missing files and been skipped.
    assert {:ok, %{blocks: 3}} = LegacyReader.inventory(reader)

    LegacyReader.close(reader)
  end
end
