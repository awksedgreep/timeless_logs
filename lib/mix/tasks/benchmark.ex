defmodule Mix.Tasks.TimelessLogs.Benchmark do
  @moduledoc "Benchmark compression rates with realistic Phoenix log data"
  use Mix.Task

  @shortdoc "Benchmark log compression with simulated Phoenix traffic"

  @impl true
  def run(_args) do
    Mix.Task.run("app.start")

    data_dir = "benchmark_data_#{System.unique_integer([:positive])}"
    blocks_dir = Path.join(data_dir, "blocks")
    File.mkdir_p!(blocks_dir)

    IO.puts("Generating 1 week of Phoenix logs...")
    IO.puts("Simulating ~30 req/min avg with typical patterns\n")

    {entries, raw_size} = generate_week_of_logs()
    entry_count = length(entries)

    IO.puts("Generated #{fmt_number(entry_count)} log entries")
    IO.puts("Raw size (term_to_binary): #{fmt_bytes(raw_size)}\n")

    IO.puts("Compressing in blocks of 1000 entries...\n")

    {elapsed_us, {total_compressed, block_count}} =
      :timer.tc(fn -> compress_all(entries, data_dir) end)

    elapsed_s = elapsed_us / 1_000_000
    ratio = raw_size / max(total_compressed, 1)
    entries_per_sec = entry_count / elapsed_s

    IO.puts("=== Results ===")
    IO.puts("Entries:          #{fmt_number(entry_count)}")
    IO.puts("Blocks:           #{block_count}")
    IO.puts("Raw size:         #{fmt_bytes(raw_size)}")
    IO.puts("Compressed size:  #{fmt_bytes(total_compressed)}")
    IO.puts("Ratio:            #{:erlang.float_to_binary(ratio, decimals: 1)}x")

    IO.puts(
      "Savings:          #{:erlang.float_to_binary((1 - total_compressed / raw_size) * 100, decimals: 1)}%"
    )

    IO.puts("Time:             #{:erlang.float_to_binary(elapsed_s, decimals: 2)}s")
    IO.puts("Throughput:       #{fmt_number(round(entries_per_sec))} entries/sec")
    IO.puts("")

    # Compare compression levels using 1000-entry blocks
    chunks = Enum.chunk_every(entries, 1000)
    binaries = Enum.map(chunks, &:erlang.term_to_binary/1)

    IO.puts("=== zstd: Compression level comparison (1000-entry blocks) ===")
    IO.puts("")

    for level <- [1, 3, 5, 9, 12, 15, 19] do
      {elapsed_us, compressed_sizes} =
        :timer.tc(fn ->
          Enum.map(binaries, fn bin -> byte_size(:ezstd.compress(bin, level)) end)
        end)

      total_compressed = Enum.sum(compressed_sizes)
      ratio = raw_size / max(total_compressed, 1)
      elapsed_ms = elapsed_us / 1_000
      entries_per_sec = entry_count / (elapsed_us / 1_000_000)

      IO.puts(
        "  Level #{String.pad_leading("#{level}", 2)}: " <>
          "#{String.pad_leading(fmt_bytes(total_compressed), 10)} " <>
          "(#{String.pad_leading(:erlang.float_to_binary(ratio, decimals: 1), 5)}x)  " <>
          "#{String.pad_leading(:erlang.float_to_binary(elapsed_ms, decimals: 0), 6)} ms  " <>
          "#{fmt_number(round(entries_per_sec))} entries/sec"
      )
    end

    IO.puts("")
    IO.puts("=== OpenZL (columnar): Compression level comparison (1000-entry blocks) ===")
    IO.puts("")

    for level <- [1, 3, 5, 9, 12, 15, 19] do
      {elapsed_us, compressed_sizes} =
        :timer.tc(fn ->
          Enum.map(chunks, fn chunk ->
            {:ok, compressed} = TimelessLogs.Writer.columnar_serialize(chunk, level: level)
            byte_size(compressed)
          end)
        end)

      total_compressed = Enum.sum(compressed_sizes)
      ratio = raw_size / max(total_compressed, 1)
      elapsed_ms = elapsed_us / 1_000
      entries_per_sec = entry_count / (elapsed_us / 1_000_000)

      IO.puts(
        "  Level #{String.pad_leading("#{level}", 2)}: " <>
          "#{String.pad_leading(fmt_bytes(total_compressed), 10)} " <>
          "(#{String.pad_leading(:erlang.float_to_binary(ratio, decimals: 1), 5)}x)  " <>
          "#{String.pad_leading(:erlang.float_to_binary(elapsed_ms, decimals: 0), 6)} ms  " <>
          "#{fmt_number(round(entries_per_sec))} entries/sec"
      )
    end

    IO.puts("")

    # Head-to-head at default levels
    zstd_level = TimelessLogs.Config.zstd_compression_level()
    ozl_level = TimelessLogs.Config.openzl_compression_level()
    IO.puts("=== Head-to-head at default levels (zstd=#{zstd_level}, openzl=#{ozl_level}) ===")
    IO.puts("")

    # zstd
    {zstd_us, zstd_sizes} =
      :timer.tc(fn ->
        Enum.map(binaries, fn bin -> byte_size(:ezstd.compress(bin, zstd_level)) end)
      end)

    zstd_total = Enum.sum(zstd_sizes)

    # OpenZL (columnar)
    {ozl_us, ozl_sizes} =
      :timer.tc(fn ->
        Enum.map(chunks, fn chunk ->
          {:ok, compressed} = TimelessLogs.Writer.columnar_serialize(chunk, level: ozl_level)
          byte_size(compressed)
        end)
      end)

    ozl_total = Enum.sum(ozl_sizes)

    # Verify roundtrip
    ozl_roundtrip_ok =
      Enum.all?(chunks, fn chunk ->
        {:ok, compressed} = TimelessLogs.Writer.columnar_serialize(chunk, level: ozl_level)
        {:ok, decoded} = TimelessLogs.Writer.columnar_deserialize(compressed)
        decoded == chunk
      end)

    IO.puts(
      "  zstd:   #{String.pad_leading(fmt_bytes(zstd_total), 10)} " <>
        "(#{:erlang.float_to_binary(raw_size / max(zstd_total, 1), decimals: 1)}x)  " <>
        "#{:erlang.float_to_binary(zstd_us / 1_000, decimals: 0)} ms"
    )

    IO.puts(
      "  OpenZL: #{String.pad_leading(fmt_bytes(ozl_total), 10)} " <>
        "(#{:erlang.float_to_binary(raw_size / max(ozl_total, 1), decimals: 1)}x)  " <>
        "#{:erlang.float_to_binary(ozl_us / 1_000, decimals: 0)} ms"
    )

    IO.puts("")

    size_diff = (ozl_total - zstd_total) / max(zstd_total, 1) * 100
    speed_diff = (ozl_us - zstd_us) / max(zstd_us, 1) * 100

    IO.puts("  OpenZL vs zstd:")

    IO.puts(
      "    Size:  #{:erlang.float_to_binary(abs(size_diff), decimals: 1)}% " <>
        "#{if size_diff < 0, do: "smaller", else: "larger"}"
    )

    IO.puts(
      "    Speed: #{:erlang.float_to_binary(abs(speed_diff), decimals: 1)}% " <>
        "#{if speed_diff < 0, do: "faster", else: "slower"}"
    )

    IO.puts("    Roundtrip: #{if ozl_roundtrip_ok, do: "PASS", else: "FAIL"}")
    IO.puts("")

    # --- Decompression benchmark ---
    # Pre-compress all blocks in both formats
    zstd_blobs =
      Enum.map(binaries, fn bin -> :ezstd.compress(bin, zstd_level) end)

    ozl_blobs =
      Enum.map(chunks, fn chunk ->
        {:ok, compressed} = TimelessLogs.Writer.columnar_serialize(chunk, level: ozl_level)
        compressed
      end)

    IO.puts(
      "=== Decompression speed (#{length(zstd_blobs)} blocks, #{fmt_number(entry_count)} entries) ==="
    )

    IO.puts("")

    # zstd decompress
    {zstd_dec_us, _} =
      :timer.tc(fn ->
        Enum.each(zstd_blobs, fn blob ->
          {:ok, _entries} = TimelessLogs.Writer.decompress_block(blob, :zstd)
        end)
      end)

    # openzl decompress
    {ozl_dec_us, _} =
      :timer.tc(fn ->
        Enum.each(ozl_blobs, fn blob ->
          {:ok, _entries} = TimelessLogs.Writer.decompress_block(blob, :openzl)
        end)
      end)

    zstd_dec_eps = entry_count / (zstd_dec_us / 1_000_000)
    ozl_dec_eps = entry_count / (ozl_dec_us / 1_000_000)
    dec_speed_diff = (ozl_dec_us - zstd_dec_us) / max(zstd_dec_us, 1) * 100

    IO.puts(
      "  zstd:   #{String.pad_leading(:erlang.float_to_binary(zstd_dec_us / 1_000, decimals: 0), 6)} ms  " <>
        "#{fmt_number(round(zstd_dec_eps))} entries/sec"
    )

    IO.puts(
      "  OpenZL: #{String.pad_leading(:erlang.float_to_binary(ozl_dec_us / 1_000, decimals: 0), 6)} ms  " <>
        "#{fmt_number(round(ozl_dec_eps))} entries/sec"
    )

    IO.puts(
      "  OpenZL vs zstd: #{:erlang.float_to_binary(abs(dec_speed_diff), decimals: 1)}% " <>
        "#{if dec_speed_diff < 0, do: "faster", else: "slower"}"
    )

    IO.puts("")

    # --- Simulated query: decompress + filter + sort (what Index.query does) ---
    IO.puts("=== Simulated query: decompress → filter → sort ===")
    IO.puts("    (filter: level == :info, scan all #{length(zstd_blobs)} blocks)")
    IO.puts("")

    # zstd query
    {zstd_q_us, zstd_q_count} =
      :timer.tc(fn ->
        zstd_blobs
        |> Task.async_stream(
          fn blob ->
            {:ok, entries} = TimelessLogs.Writer.decompress_block(blob, :zstd)
            Enum.filter(entries, &(&1.level == :info))
          end,
          max_concurrency: System.schedulers_online(),
          ordered: false
        )
        |> Enum.flat_map(fn {:ok, entries} -> entries end)
        |> Enum.sort_by(& &1.timestamp, :desc)
        |> length()
      end)

    # openzl query
    {ozl_q_us, ozl_q_count} =
      :timer.tc(fn ->
        ozl_blobs
        |> Task.async_stream(
          fn blob ->
            {:ok, entries} = TimelessLogs.Writer.decompress_block(blob, :openzl)
            Enum.filter(entries, &(&1.level == :info))
          end,
          max_concurrency: System.schedulers_online(),
          ordered: false
        )
        |> Enum.flat_map(fn {:ok, entries} -> entries end)
        |> Enum.sort_by(& &1.timestamp, :desc)
        |> length()
      end)

    q_speed_diff = (ozl_q_us - zstd_q_us) / max(zstd_q_us, 1) * 100

    IO.puts(
      "  zstd:   #{String.pad_leading(:erlang.float_to_binary(zstd_q_us / 1_000, decimals: 0), 6)} ms  " <>
        "(#{fmt_number(zstd_q_count)} matching entries)"
    )

    IO.puts(
      "  OpenZL: #{String.pad_leading(:erlang.float_to_binary(ozl_q_us / 1_000, decimals: 0), 6)} ms  " <>
        "(#{fmt_number(ozl_q_count)} matching entries)"
    )

    IO.puts(
      "  OpenZL vs zstd: #{:erlang.float_to_binary(abs(q_speed_diff), decimals: 1)}% " <>
        "#{if q_speed_diff < 0, do: "faster", else: "slower"}"
    )

    IO.puts("")

    # --- Ingestion benchmark: raw write + index (what happens on every flush) ---
    IO.puts("=== Ingestion: raw write + index (simulates Buffer flush path) ===")
    IO.puts("")

    # Disk ingestion
    ingest_dir = "benchmark_ingest_#{System.unique_integer([:positive])}"
    ingest_blocks_dir = Path.join(ingest_dir, "blocks")
    File.mkdir_p!(ingest_blocks_dir)

    {disk_ingest_us, _} =
      :timer.tc(fn ->
        Enum.each(chunks, fn chunk ->
          {:ok, _meta} = TimelessLogs.Writer.write_block(chunk, ingest_dir, :raw)
        end)
      end)

    disk_ingest_eps = entry_count / (disk_ingest_us / 1_000_000)

    IO.puts(
      "  Raw to disk:   #{String.pad_leading(:erlang.float_to_binary(disk_ingest_us / 1_000, decimals: 0), 6)} ms  " <>
        "#{fmt_number(round(disk_ingest_eps))} entries/sec"
    )

    File.rm_rf!(ingest_dir)

    # Memory ingestion
    {mem_ingest_us, _} =
      :timer.tc(fn ->
        Enum.each(chunks, fn chunk ->
          {:ok, _meta} = TimelessLogs.Writer.write_block(chunk, :memory, :raw)
        end)
      end)

    mem_ingest_eps = entry_count / (mem_ingest_us / 1_000_000)

    IO.puts(
      "  Raw to memory: #{String.pad_leading(:erlang.float_to_binary(mem_ingest_us / 1_000, decimals: 0), 6)} ms  " <>
        "#{fmt_number(round(mem_ingest_eps))} entries/sec"
    )

    IO.puts("")

    # --- Compaction throughput: raw → zstd vs raw → openzl (parallel, like real compactor) ---
    IO.puts("=== Compaction throughput (parallel, #{System.schedulers_online()} cores) ===")
    IO.puts("")

    concurrency = System.schedulers_online()

    {zstd_compact_us, _} =
      :timer.tc(fn ->
        chunks
        |> Task.async_stream(
          fn chunk ->
            {:ok, _meta} = TimelessLogs.Writer.write_block(chunk, :memory, :zstd)
          end,
          max_concurrency: concurrency,
          ordered: false
        )
        |> Stream.run()
      end)

    zstd_compact_eps = entry_count / (zstd_compact_us / 1_000_000)

    {ozl_compact_us, _} =
      :timer.tc(fn ->
        chunks
        |> Task.async_stream(
          fn chunk ->
            {:ok, _meta} = TimelessLogs.Writer.write_block(chunk, :memory, :openzl)
          end,
          max_concurrency: concurrency,
          ordered: false
        )
        |> Stream.run()
      end)

    ozl_compact_eps = entry_count / (ozl_compact_us / 1_000_000)
    compact_diff = (ozl_compact_us - zstd_compact_us) / max(zstd_compact_us, 1) * 100

    IO.puts(
      "  zstd:   #{String.pad_leading(:erlang.float_to_binary(zstd_compact_us / 1_000, decimals: 0), 6)} ms  " <>
        "#{fmt_number(round(zstd_compact_eps))} entries/sec"
    )

    IO.puts(
      "  OpenZL: #{String.pad_leading(:erlang.float_to_binary(ozl_compact_us / 1_000, decimals: 0), 6)} ms  " <>
        "#{fmt_number(round(ozl_compact_eps))} entries/sec"
    )

    IO.puts(
      "  OpenZL vs zstd: #{:erlang.float_to_binary(abs(compact_diff), decimals: 1)}% " <>
        "#{if compact_diff < 0, do: "faster", else: "slower"}"
    )

    IO.puts("")

    File.rm_rf!(data_dir)
  end

  defp generate_week_of_logs do
    # ~30 req/min * 60 * 24 * 7 = 302,400 requests
    # ~3.5 log entries per request avg = ~1M entries
    # Plus background jobs, errors, system events

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
    Enum.flat_map(1..count, fn i ->
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

      # Sometimes add a second query
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
      |> maybe_add_error(ts, req_id, status, i)
    end)
  end

  defp maybe_add_error(entries, ts, req_id, status, _i) when status >= 500 do
    error = %{
      timestamp: ts,
      level: :error,
      message:
        "Internal server error: #{Enum.random(["timeout", "connection_refused", "nxdomain", "pool_timeout", "deadlock_detected"])}",
      metadata: %{
        "request_id" => req_id,
        "module" => "Phoenix.Logger",
        "crash_reason" => random_crash()
      }
    }

    [error | entries]
  end

  defp maybe_add_error(entries, _ts, _req_id, _status, _i), do: entries

  defp background_logs(ts, minute) do
    logs = []

    # Periodic job every 5 minutes
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

    # Heartbeat every minute
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
      | logs
    ]

    # Occasional warnings
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
      "/api/v1/search?q=#{random_word()}",
      "/dashboard",
      "/dashboard/metrics",
      "/dashboard/users",
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
        String.contains?(path, "notifications") -> "notifications"
        true -> "records"
      end

    a = String.first(table)

    Enum.random([
      ~s|SELECT #{a}0."id", #{a}0."name", #{a}0."inserted_at" FROM "#{table}" AS #{a}0 WHERE (#{a}0."id" = $1) [#{:rand.uniform(10000)}]|,
      ~s|SELECT #{a}0."id" FROM "#{table}" AS #{a}0 WHERE (#{a}0."active" = $1) ORDER BY #{a}0."inserted_at" DESC LIMIT $2 [true, 20]|,
      ~s|INSERT INTO "#{table}" ("name","inserted_at","updated_at") VALUES ($1,$2,$3) RETURNING "id"|,
      ~s|UPDATE "#{table}" SET "updated_at" = $1 WHERE "id" = $2|
    ])
  end

  defp random_table, do: Enum.random(~w(users posts comments sessions notifications tags))
  defp random_word, do: Enum.random(~w(elixir phoenix search query test user admin deploy error))
  defp random_hex(n), do: :crypto.strong_rand_bytes(n) |> Base.encode16(case: :lower)

  defp random_crash do
    Enum.random([
      "%DBConnection.ConnectionError{message: \"connection not available\"}",
      "%Postgrex.Error{postgres: %{code: :deadlock_detected}}",
      "%Jason.DecodeError{position: 0}",
      "%RuntimeError{message: \"unexpected nil\"}",
      "%FunctionClauseError{module: MyApp.UserController}"
    ])
  end

  defp compress_all(entries, data_dir) do
    entries
    |> Enum.chunk_every(1000)
    |> Enum.reduce({0, 0}, fn chunk, {total_size, block_count} ->
      case TimelessLogs.Writer.write_block(chunk, data_dir, :zstd) do
        {:ok, meta} -> {total_size + meta.byte_size, block_count + 1}
        _ -> {total_size, block_count}
      end
    end)
  end

  defp fmt_bytes(bytes) when bytes < 1024, do: "#{bytes} B"

  defp fmt_bytes(bytes) when bytes < 1024 * 1024,
    do: "#{:erlang.float_to_binary(bytes / 1024, decimals: 1)} KB"

  defp fmt_bytes(bytes), do: "#{:erlang.float_to_binary(bytes / 1024 / 1024, decimals: 1)} MB"

  defp fmt_number(n) when n >= 1_000_000 do
    "#{:erlang.float_to_binary(n / 1_000_000, decimals: 1)}M"
  end

  defp fmt_number(n) when n >= 1_000 do
    "#{:erlang.float_to_binary(n / 1_000, decimals: 1)}K"
  end

  defp fmt_number(n), do: "#{n}"
end
