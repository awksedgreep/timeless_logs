defmodule Mix.Tasks.TimelessLogs.SearchBenchmark do
  @moduledoc "Benchmark search speed across query patterns on a week of indexed Phoenix logs"
  use Mix.Task

  @shortdoc "Benchmark log search speed with indexed data"

  @impl true
  def run(_args) do
    Mix.Task.run("app.start")

    data_dir = "search_bench_#{System.unique_integer([:positive])}"
    blocks_dir = Path.join(data_dir, "blocks")
    File.mkdir_p!(blocks_dir)

    IO.puts("=== TimelessLogs Search Benchmark ===\n")
    IO.puts("Generating 1 week of Phoenix logs...")

    {entries, _raw_size} = generate_week_of_logs()
    entry_count = length(entries)
    IO.puts("Generated #{fmt_number(entry_count)} log entries\n")

    IO.puts("Ingesting and indexing...")
    {ingest_us, {block_count, index_size}} = :timer.tc(fn -> ingest_all(entries, data_dir) end)
    IO.puts("Ingested #{block_count} blocks in #{fmt_ms(ingest_us)}")
    IO.puts("SQLite index size: #{fmt_bytes(index_size)}\n")

    # Now benchmark various query patterns
    IO.puts("Running search benchmarks (5 iterations each)...\n")

    queries = [
      {"level=error (indexed, ~2% of logs)", [level: :error, limit: 100]},
      {"level=error, limit 10 (paginated)", [level: :error, limit: 10]},
      {"level=error + metadata service=api (indexed intersection)",
       [level: :error, metadata: %{module: "Phoenix.Logger"}, limit: 100]},
      {"message substring 'timeout' (scan, no index)", [message: "timeout", limit: 100]},
      {"message 'Healthcheck' (common, scan)", [message: "Healthcheck", limit: 100]},
      {"last 1 hour (time range, ~0.6% of blocks)",
       [since: System.system_time(:second) - 3600, limit: 100]},
      {"last 1 hour + level=error (time + index)",
       [since: System.system_time(:second) - 3600, level: :error, limit: 100]},
      {"last 24 hours (time range, ~14% of blocks)",
       [since: System.system_time(:second) - 86400, limit: 100]},
      {"needle in haystack: specific request_id",
       [metadata: %{request_id: pick_request_id(entries)}, limit: 10]},
      {"all logs, page 1 (no filters, worst case)", [limit: 100]},
      {"all logs, page 50 (deep pagination)", [limit: 100, offset: 4900]}
    ]

    results =
      Enum.map(queries, fn {label, filters} ->
        times =
          for _ <- 1..5 do
            {us, {:ok, result}} = :timer.tc(fn -> TimelessLogs.Index.query(filters) end)
            {us, result}
          end

        latencies = Enum.map(times, fn {us, _} -> us end)
        {_, sample_result} = hd(times)
        median = Enum.sort(latencies) |> Enum.at(2)
        min_l = Enum.min(latencies)
        max_l = Enum.max(latencies)

        {label, median, min_l, max_l, sample_result.total}
      end)

    IO.puts(
      String.pad_trailing("Query", 55) <>
        String.pad_leading("Median", 10) <>
        String.pad_leading("Min", 10) <>
        String.pad_leading("Max", 10) <>
        String.pad_leading("Matches", 10)
    )

    IO.puts(String.duplicate("-", 95))

    for {label, median, min_l, max_l, total} <- results do
      IO.puts(
        String.pad_trailing(label, 55) <>
          String.pad_leading(fmt_ms(median), 10) <>
          String.pad_leading(fmt_ms(min_l), 10) <>
          String.pad_leading(fmt_ms(max_l), 10) <>
          String.pad_leading(fmt_number(total), 10)
      )
    end

    IO.puts("")

    # Summary stats
    total_blocks = block_count
    total_disk = dir_size(blocks_dir) + index_size

    IO.puts("=== Storage Summary ===")
    IO.puts("Blocks on disk:   #{fmt_bytes(dir_size(blocks_dir))}")
    IO.puts("SQLite index:     #{fmt_bytes(index_size)}")
    IO.puts("Total disk:       #{fmt_bytes(total_disk)}")

    IO.puts(
      "Index overhead:   #{:erlang.float_to_binary(index_size / max(total_disk, 1) * 100, decimals: 1)}%"
    )

    IO.puts("Blocks:           #{total_blocks}")
    IO.puts("Entries:          #{fmt_number(entry_count)}")

    File.rm_rf!(data_dir)
  end

  defp ingest_all(entries, data_dir) do
    db_path = Path.join(data_dir, "index.db")
    {:ok, db} = Exqlite.Sqlite3.open(db_path)

    Exqlite.Sqlite3.execute(db, "PRAGMA journal_mode=WAL")
    Exqlite.Sqlite3.execute(db, "PRAGMA synchronous=NORMAL")

    Exqlite.Sqlite3.execute(db, """
    CREATE TABLE IF NOT EXISTS blocks (
      block_id INTEGER PRIMARY KEY,
      file_path TEXT,
      byte_size INTEGER NOT NULL,
      entry_count INTEGER NOT NULL,
      ts_min INTEGER NOT NULL,
      ts_max INTEGER NOT NULL,
      data BLOB,
      format TEXT NOT NULL DEFAULT 'zstd',
      created_at INTEGER NOT NULL DEFAULT (unixepoch())
    )
    """)

    Exqlite.Sqlite3.execute(db, """
    CREATE TABLE IF NOT EXISTS block_terms (
      term TEXT NOT NULL,
      block_id INTEGER NOT NULL REFERENCES blocks(block_id),
      PRIMARY KEY (term, block_id)
    ) WITHOUT ROWID
    """)

    Exqlite.Sqlite3.execute(db, """
    CREATE INDEX IF NOT EXISTS idx_blocks_ts ON blocks(ts_min, ts_max)
    """)

    block_count =
      entries
      |> Enum.chunk_every(1000)
      |> Enum.reduce(0, fn chunk, count ->
        case TimelessLogs.Writer.write_block(chunk, data_dir) do
          {:ok, meta} ->
            index_block_direct(db, meta, chunk)
            count + 1

          _ ->
            count
        end
      end)

    Exqlite.Sqlite3.close(db)

    # Point the running Index GenServer at this data
    # We need to restart the app with this data_dir
    Application.stop(:timeless_logs)
    Application.put_env(:timeless_logs, :data_dir, data_dir)
    Application.ensure_all_started(:timeless_logs)

    index_size = File.stat!(db_path).size
    {block_count, index_size}
  end

  defp index_block_direct(db, meta, entries) do
    Exqlite.Sqlite3.execute(db, "BEGIN")

    {:ok, block_stmt} =
      Exqlite.Sqlite3.prepare(db, """
      INSERT INTO blocks (block_id, file_path, byte_size, entry_count, ts_min, ts_max, format)
      VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
      """)

    format_str = Atom.to_string(Map.get(meta, :format, :raw))

    Exqlite.Sqlite3.bind(block_stmt, [
      meta.block_id,
      meta.file_path,
      meta.byte_size,
      meta.entry_count,
      meta.ts_min,
      meta.ts_max,
      format_str
    ])

    Exqlite.Sqlite3.step(db, block_stmt)
    Exqlite.Sqlite3.release(db, block_stmt)

    terms =
      entries
      |> Enum.flat_map(fn entry ->
        level_term = "level:#{entry.level}"
        metadata_terms = Enum.map(entry.metadata, fn {k, v} -> "#{k}:#{v}" end)
        [level_term | metadata_terms]
      end)
      |> Enum.uniq()

    # Batch insert terms (400 per statement, 800 params)
    terms
    |> Enum.chunk_every(400)
    |> Enum.each(fn batch ->
      n = length(batch)

      placeholders =
        Enum.map_join(1..n, ", ", fn i -> "(?#{i * 2 - 1}, ?#{i * 2})" end)

      sql = "INSERT OR IGNORE INTO block_terms (term, block_id) VALUES #{placeholders}"
      {:ok, stmt} = Exqlite.Sqlite3.prepare(db, sql)
      params = Enum.flat_map(batch, fn term -> [term, meta.block_id] end)
      Exqlite.Sqlite3.bind(stmt, params)
      Exqlite.Sqlite3.step(db, stmt)
      Exqlite.Sqlite3.release(db, stmt)
    end)

    Exqlite.Sqlite3.execute(db, "COMMIT")
  end

  defp pick_request_id(entries) do
    # Find a request_id that exists in the data
    entries
    |> Enum.find_value(fn entry ->
      Map.get(entry.metadata, "request_id")
    end)
    |> then(fn
      nil -> "nonexistent"
      id -> id
    end)
  end

  defp dir_size(path) do
    Path.wildcard(Path.join(path, "*"))
    |> Enum.reduce(0, fn file, acc ->
      case File.stat(file) do
        {:ok, %{size: size}} -> acc + size
        _ -> acc
      end
    end)
  end

  # --- Log generation (same as benchmark task) ---

  defp generate_week_of_logs do
    minutes_in_week = 7 * 24 * 60
    base_ts = System.system_time(:second) - minutes_in_week * 60

    entries =
      Enum.flat_map(0..(minutes_in_week - 1), fn minute ->
        ts = base_ts + minute * 60
        reqs = 25 + :rand.uniform(15)
        request_logs(ts, reqs) ++ background_logs(ts, minute)
      end)

    raw_size = byte_size(:erlang.term_to_binary(entries))
    {entries, raw_size}
  end

  defp request_logs(base_ts, count) do
    Enum.flat_map(1..count, fn _i ->
      ts = base_ts + :rand.uniform(59)
      req_id = random_hex(16)
      method = Enum.random(~w(GET GET GET GET POST PUT DELETE PATCH))
      path = random_path()
      status = random_status()
      duration = random_duration()
      user_id = :rand.uniform(10_000)

      entries = [
        %{
          timestamp: ts,
          level: :info,
          message: "#{method} #{path}",
          metadata: %{
            "request_id" => req_id,
            "module" => "Phoenix.Endpoint",
            "user_id" => "#{user_id}"
          }
        },
        %{
          timestamp: ts,
          level: :debug,
          message: ecto_query(path),
          metadata: %{
            "request_id" => req_id,
            "module" => "Ecto.Adapters.SQL",
            "source" => random_table(),
            "query_time" => "#{:rand.uniform(50)}ms"
          }
        },
        %{
          timestamp: ts,
          level: :info,
          message: "Sent #{status} in #{duration}ms",
          metadata: %{
            "request_id" => req_id,
            "module" => "Phoenix.Logger",
            "status" => "#{status}",
            "duration" => "#{duration}"
          }
        }
      ]

      if :rand.uniform(3) == 1 do
        extra = %{
          timestamp: ts,
          level: :debug,
          message: ecto_query(path),
          metadata: %{
            "request_id" => req_id,
            "module" => "Ecto.Adapters.SQL",
            "source" => random_table(),
            "query_time" => "#{:rand.uniform(20)}ms"
          }
        }

        [extra | entries]
      else
        entries
      end
      |> maybe_add_error(ts, req_id, status)
    end)
  end

  defp maybe_add_error(entries, ts, req_id, status) when status >= 500 do
    error = %{
      timestamp: ts,
      level: :error,
      message:
        "Internal server error: #{Enum.random(["timeout", "connection_refused", "nxdomain", "pool_timeout", "deadlock_detected"])}",
      metadata: %{
        "request_id" => req_id,
        "module" => "Phoenix.Logger",
        "crash_reason" =>
          Enum.random([
            "%DBConnection.ConnectionError{message: \"connection not available\"}",
            "%Postgrex.Error{postgres: %{code: :deadlock_detected}}",
            "%RuntimeError{message: \"unexpected nil\"}"
          ])
      }
    }

    [error | entries]
  end

  defp maybe_add_error(entries, _ts, _req_id, _status), do: entries

  defp background_logs(ts, minute) do
    logs = [
      %{
        timestamp: ts + 45,
        level: :debug,
        message: "Healthcheck OK",
        metadata: %{
          "module" => "MyApp.Health",
          "memory_mb" => "#{256 + :rand.uniform(512)}",
          "process_count" => "#{200 + :rand.uniform(100)}"
        }
      }
    ]

    logs =
      if rem(minute, 5) == 0 do
        [
          %{
            timestamp: ts + 30,
            level: :info,
            message:
              "Running scheduled job: #{Enum.random(~w(cleanup_sessions refresh_cache sync_data send_digests update_stats))}",
            metadata: %{"module" => "MyApp.Scheduler", "job_id" => random_hex(8)}
          }
          | logs
        ]
      else
        logs
      end

    if :rand.uniform(30) == 1 do
      [
        %{
          timestamp: ts + :rand.uniform(59),
          level: :warning,
          message:
            Enum.random([
              "Connection pool checkout timeout after 5000ms",
              "Slow query detected (>100ms)",
              "Rate limit approaching for API key",
              "Certificate expiring in 7 days",
              "Memory usage above 80% threshold"
            ]),
          metadata: %{
            "module" =>
              Enum.random(~w(DBConnection Ecto.Adapters.SQL MyApp.RateLimiter MyApp.Monitor))
          }
        }
        | logs
      ]
    else
      logs
    end
  end

  defp random_path do
    Enum.random([
      "/api/v1/users",
      "/api/v1/users/#{:rand.uniform(10000)}",
      "/api/v1/posts",
      "/api/v1/posts/#{:rand.uniform(50000)}",
      "/api/v1/posts/#{:rand.uniform(50000)}/comments",
      "/api/v1/sessions",
      "/api/v1/notifications",
      "/api/v1/search?q=#{Enum.random(~w(elixir phoenix search))}",
      "/dashboard",
      "/dashboard/metrics",
      "/health",
      "/live/updates",
      "/uploads/#{random_hex(8)}"
    ])
  end

  defp random_status do
    case :rand.uniform(100) do
      n when n <= 70 -> 200
      n when n <= 80 -> 201
      n when n <= 85 -> 204
      n when n <= 88 -> 301
      n when n <= 92 -> 304
      n when n <= 95 -> 400
      n when n <= 97 -> 404
      n when n <= 98 -> 422
      n when n <= 99 -> 500
      _ -> 503
    end
  end

  defp random_duration do
    case :rand.uniform(100) do
      n when n <= 60 -> 1 + :rand.uniform(20)
      n when n <= 85 -> 20 + :rand.uniform(80)
      n when n <= 95 -> 100 + :rand.uniform(400)
      _ -> 500 + :rand.uniform(2000)
    end
  end

  defp ecto_query(path) do
    table =
      cond do
        String.contains?(path, "users") -> "users"
        String.contains?(path, "posts") -> "posts"
        String.contains?(path, "comments") -> "comments"
        String.contains?(path, "sessions") -> "sessions"
        true -> "records"
      end

    a = String.first(table)

    Enum.random([
      ~s|SELECT #{a}0."id", #{a}0."name" FROM "#{table}" AS #{a}0 WHERE (#{a}0."id" = $1) [#{:rand.uniform(10000)}]|,
      ~s|SELECT #{a}0."id" FROM "#{table}" AS #{a}0 WHERE (#{a}0."active" = $1) LIMIT $2 [true, 20]|,
      ~s|INSERT INTO "#{table}" ("name","inserted_at") VALUES ($1,$2) RETURNING "id"|,
      ~s|UPDATE "#{table}" SET "updated_at" = $1 WHERE "id" = $2|
    ])
  end

  defp random_table, do: Enum.random(~w(users posts comments sessions notifications tags))
  defp random_hex(n), do: :crypto.strong_rand_bytes(n) |> Base.encode16(case: :lower)

  defp fmt_bytes(bytes) when bytes < 1024, do: "#{bytes} B"

  defp fmt_bytes(bytes) when bytes < 1024 * 1024,
    do: "#{:erlang.float_to_binary(bytes / 1024, decimals: 1)} KB"

  defp fmt_bytes(bytes), do: "#{:erlang.float_to_binary(bytes / 1024 / 1024, decimals: 1)} MB"

  defp fmt_number(n) when n >= 1_000_000,
    do: "#{:erlang.float_to_binary(n / 1_000_000, decimals: 1)}M"

  defp fmt_number(n) when n >= 1_000, do: "#{:erlang.float_to_binary(n / 1_000, decimals: 1)}K"
  defp fmt_number(n), do: "#{n}"

  defp fmt_ms(us) when us < 1_000, do: "#{us}us"
  defp fmt_ms(us) when us < 1_000_000, do: "#{:erlang.float_to_binary(us / 1_000, decimals: 1)}ms"
  defp fmt_ms(us), do: "#{:erlang.float_to_binary(us / 1_000_000, decimals: 2)}s"
end
