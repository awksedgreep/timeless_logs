defmodule TimelessLogs.DB.Migrations do
  @moduledoc false

  @max_retries 8

  def run(conn) do
    create_metadata_table(conn)
    version = get_version(conn)
    run_from(conn, version)
  end

  defp create_metadata_table(conn) do
    execute(conn, """
    CREATE TABLE IF NOT EXISTS _metadata (
      key   TEXT PRIMARY KEY,
      value TEXT NOT NULL
    ) WITHOUT ROWID
    """)
  end

  defp get_version(conn) do
    get_version_with_retry(conn, @max_retries)
  end

  defp get_version_with_retry(conn, retries) do
    case Exqlite.Sqlite3.prepare(conn, "SELECT value FROM _metadata WHERE key = 'schema_version'") do
      {:ok, stmt} ->
        result = Exqlite.Sqlite3.step(conn, stmt)
        Exqlite.Sqlite3.release(conn, stmt)

        case result do
          {:row, [version]} -> String.to_integer(version)
          :done -> 0
        end

      {:error, _reason} when retries > 0 ->
        Process.sleep(retry_backoff(@max_retries - retries))
        get_version_with_retry(conn, retries - 1)

      {:error, reason} ->
        raise "SQLite get_version failed after retries: #{inspect(reason)}"
    end
  end

  defp set_version(conn, version) do
    execute(conn, "INSERT OR REPLACE INTO _metadata (key, value) VALUES ('schema_version', ?1)", [
      to_string(version)
    ])
  end

  defp run_from(conn, version) when version < 1 do
    execute(conn, "BEGIN")

    execute(conn, """
    CREATE TABLE IF NOT EXISTS blocks (
      block_id     INTEGER PRIMARY KEY,
      file_path    TEXT,
      byte_size    INTEGER NOT NULL,
      entry_count  INTEGER NOT NULL,
      ts_min       INTEGER NOT NULL,
      ts_max       INTEGER NOT NULL,
      format       TEXT NOT NULL DEFAULT 'zstd',
      created_at   INTEGER NOT NULL
    )
    """)

    execute(conn, "CREATE INDEX IF NOT EXISTS idx_blocks_ts_min ON blocks(ts_min)")
    execute(conn, "CREATE INDEX IF NOT EXISTS idx_blocks_ts_max ON blocks(ts_max)")
    execute(conn, "CREATE INDEX IF NOT EXISTS idx_blocks_format ON blocks(format)")
    execute(conn, "CREATE INDEX IF NOT EXISTS idx_blocks_created_at ON blocks(created_at)")

    execute(conn, """
    CREATE TABLE IF NOT EXISTS term_index (
      term      TEXT NOT NULL,
      block_id  INTEGER NOT NULL,
      PRIMARY KEY (term, block_id)
    ) WITHOUT ROWID
    """)

    execute(conn, "CREATE INDEX IF NOT EXISTS idx_term_index_block_id ON term_index(block_id)")

    execute(conn, """
    CREATE TABLE IF NOT EXISTS compression_stats (
      key             TEXT PRIMARY KEY,
      raw_in          INTEGER NOT NULL DEFAULT 0,
      compressed_out  INTEGER NOT NULL DEFAULT 0,
      count           INTEGER NOT NULL DEFAULT 0
    ) WITHOUT ROWID
    """)

    execute(conn, """
    CREATE TABLE IF NOT EXISTS block_data (
      block_id  INTEGER PRIMARY KEY,
      data      BLOB NOT NULL
    )
    """)

    set_version(conn, 1)
    execute(conn, "COMMIT")

    run_from(conn, 1)
  end

  defp run_from(_conn, 1), do: :ok

  defp execute(conn, sql, params \\ []) do
    execute_with_retry(conn, sql, params, @max_retries)
  end

  defp execute_with_retry(conn, sql, params, retries) do
    case Exqlite.Sqlite3.prepare(conn, sql) do
      {:ok, stmt} ->
        if params != [] do
          :ok = Exqlite.Sqlite3.bind(stmt, params)
        end

        :done = Exqlite.Sqlite3.step(conn, stmt)
        Exqlite.Sqlite3.release(conn, stmt)

      {:error, _reason} when retries > 0 ->
        Process.sleep(retry_backoff(@max_retries - retries))
        execute_with_retry(conn, sql, params, retries - 1)

      {:error, reason} ->
        raise "SQLite migration failed after retries: #{inspect(reason)} (sql: #{sql})"
    end
  end

  defp retry_backoff(attempt), do: 100 * Integer.pow(2, attempt)
end
