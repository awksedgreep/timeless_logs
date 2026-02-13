defmodule Mix.Tasks.LogStream.Benchmark do
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
    IO.puts("=== Compression level comparison (1000-entry blocks) ===")
    IO.puts("")

    chunks = Enum.chunk_every(entries, 1000)
    binaries = Enum.map(chunks, &:erlang.term_to_binary/1)

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
      case LogStream.Writer.write_block(chunk, data_dir, :zstd) do
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
