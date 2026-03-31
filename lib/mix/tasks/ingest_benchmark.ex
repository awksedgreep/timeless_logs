defmodule Mix.Tasks.TimelessLogs.IngestBenchmark do
  @moduledoc "Benchmark ingestion throughput through the full pipeline"
  use Mix.Task

  @shortdoc "Benchmark log ingestion throughput (Buffer → Writer → Index)"

  @shard_scenarios [1, 2, 4, 8]

  @impl true
  def run(_args) do
    data_dir = "ingest_bench_#{System.unique_integer([:positive])}"
    blocks_dir = Path.join(data_dir, "blocks")
    File.mkdir_p!(blocks_dir)

    configure_app(data_dir)
    Mix.Task.run("app.start")

    IO.puts("=== TimelessLogs Ingestion Benchmark ===\n")

    # Pre-generate entries to exclude generation time
    entry_count = 500_000
    IO.puts("Pre-generating #{fmt_number(entry_count)} entries...")
    entries = generate_entries(entry_count)
    IO.puts("Done.\n")

    # Phase 1: Writer-only throughput (serialization + disk I/O)
    IO.puts("--- Phase 1: Writer-only (no indexing) ---")
    writer_dir = Path.join(data_dir, "writer_bench")
    File.mkdir_p!(Path.join(writer_dir, "blocks"))

    {writer_us, writer_blocks} =
      :timer.tc(fn ->
        entries
        |> Enum.chunk_every(1000)
        |> Enum.reduce(0, fn chunk, count ->
          case TimelessLogs.Writer.write_block(chunk, writer_dir, :raw) do
            {:ok, _} -> count + 1
            _ -> count
          end
        end)
      end)

    writer_eps = entry_count / (writer_us / 1_000_000)
    IO.puts("  #{writer_blocks} blocks in #{fmt_ms(writer_us)}")
    IO.puts("  Throughput: #{fmt_number(round(writer_eps))} entries/sec\n")

    # Phase 2: Writer + Index (sequential, sync indexing)
    IO.puts("--- Phase 2: Writer + Index (sync) ---")
    idx_dir = Path.join(data_dir, "idx_bench")
    File.mkdir_p!(Path.join(idx_dir, "blocks"))
    Application.stop(:timeless_logs)
    Application.put_env(:timeless_logs, :data_dir, idx_dir)
    Application.ensure_all_started(:timeless_logs)

    {idx_us, idx_blocks} =
      :timer.tc(fn ->
        entries
        |> Enum.chunk_every(1000)
        |> Enum.reduce(0, fn chunk, count ->
          case TimelessLogs.Writer.write_block(chunk, idx_dir, :raw) do
            {:ok, meta} ->
              terms = TimelessLogs.Index.extract_terms(chunk)
              TimelessLogs.Index.index_block(meta, chunk, terms)
              count + 1

            _ ->
              count
          end
        end)
      end)

    idx_eps = entry_count / (idx_us / 1_000_000)
    IO.puts("  #{idx_blocks} blocks in #{fmt_ms(idx_us)}")
    IO.puts("  Throughput: #{fmt_number(round(idx_eps))} entries/sec")
    overhead = (idx_us - writer_us) / 1000
    IO.puts("  Index overhead: #{:erlang.float_to_binary(overhead, decimals: 1)}ms total\n")

    # Phase 3: Full pipeline (Buffer.log → flush → Writer → async Index)
    IO.puts("--- Phase 3: Full pipeline (Buffer.log → Writer → Index) ---")
    pipe_dir = Path.join(data_dir, "pipe_bench")
    restart_logs(pipe_dir)

    {pipe_us, _} =
      :timer.tc(fn ->
        for entry <- entries do
          TimelessLogs.Buffer.log(entry)
        end

        # Flush remaining buffer
        TimelessLogs.Buffer.flush()
        # Drain Index mailbox and publish cache
        TimelessLogs.Index.sync()
      end)

    pipe_eps = entry_count / (pipe_us / 1_000_000)
    {:ok, stats} = TimelessLogs.Index.stats()

    IO.puts("  #{stats.total_blocks} blocks, #{fmt_number(stats.total_entries)} entries indexed")
    IO.puts("  Wall time: #{fmt_ms(pipe_us)}")
    IO.puts("  Throughput: #{fmt_number(round(pipe_eps))} entries/sec\n")

    IO.puts("--- Phase 3b: Batch API (TimelessLogs.ingest/1 → Writer → Index) ---")
    batch_dir = Path.join(data_dir, "batch_bench")
    restart_logs(batch_dir)

    {batch_us, _} =
      :timer.tc(fn ->
        entries
        |> Enum.chunk_every(1000)
        |> Enum.each(&TimelessLogs.ingest/1)

        TimelessLogs.Buffer.flush()
        TimelessLogs.Index.sync()
      end)

    batch_eps = entry_count / (batch_us / 1_000_000)
    {:ok, batch_stats} = TimelessLogs.Index.stats()

    IO.puts(
      "  #{batch_stats.total_blocks} blocks, #{fmt_number(batch_stats.total_entries)} entries indexed"
    )

    IO.puts("  Wall time: #{fmt_ms(batch_us)}")
    IO.puts("  Throughput: #{fmt_number(round(batch_eps))} entries/sec\n")

    IO.puts("--- Phase 3c: Batch API by shard count ---")

    shard_results =
      Enum.map(@shard_scenarios, fn shard_count ->
        shard_dir = Path.join(data_dir, "batch_shards_#{shard_count}")
        restart_logs(shard_dir, ingest_shard_count: shard_count)

        {us, _} =
          :timer.tc(fn ->
            entries
            |> Enum.chunk_every(1000)
            |> Enum.each(&TimelessLogs.ingest/1)

            TimelessLogs.Buffer.flush()
            TimelessLogs.Index.sync()
          end)

        eps = entry_count / (us / 1_000_000)
        {:ok, shard_stats} = TimelessLogs.Index.stats()

        {shard_count, us, eps, shard_stats.total_entries}
      end)

    Enum.each(shard_results, fn {shard_count, us, eps, total_entries} ->
      IO.puts(
        "  shards=#{String.pad_leading(Integer.to_string(shard_count), 2)}  " <>
          "#{fmt_ms(us)}  #{fmt_number(round(eps))} entries/sec  " <>
          "#{fmt_number(total_entries)} indexed"
      )
    end)

    IO.puts("")

    # Phase 4: Full Logger path (Logger.info → Handler → Buffer → Writer → Index)
    # with stdout/console disabled
    IO.puts("--- Phase 4: Logger path, no stdout ---")
    logger_dir = Path.join(data_dir, "logger_bench")
    restart_logs(logger_dir)

    # Remove the default console handler so only TimelessLogs receives logs
    :logger.remove_handler(:default)

    # Pre-generate log arguments (messages + metadata) to exclude string building from timing
    log_args =
      Enum.map(1..entry_count, fn _i ->
        msg = "Request #{method()} #{path()} completed in #{:rand.uniform(500)}ms"

        meta = [
          request_id: random_hex(16),
          module: Enum.random(~w(Phoenix.Logger Ecto.Adapters.SQL MyApp.UserController)),
          status: Enum.random([200, 200, 200, 201, 301, 400, 404, 500]),
          user_id: :rand.uniform(10_000)
        ]

        {msg, meta}
      end)

    require Logger

    {logger_us, _} =
      :timer.tc(fn ->
        for {msg, meta} <- log_args do
          Logger.info(msg, meta)
        end

        TimelessLogs.Buffer.flush()
        TimelessLogs.Index.sync()
      end)

    logger_eps = entry_count / (logger_us / 1_000_000)
    {:ok, logger_stats} = TimelessLogs.Index.stats()

    IO.puts(
      "  #{logger_stats.total_blocks} blocks, #{fmt_number(logger_stats.total_entries)} entries indexed"
    )

    IO.puts("  Wall time: #{fmt_ms(logger_us)}")
    IO.puts("  Throughput: #{fmt_number(round(logger_eps))} entries/sec\n")

    # Phase 5: Same but with stdout re-enabled for comparison
    IO.puts("--- Phase 5: Logger path, WITH stdout (for comparison) ---")
    stdout_dir = Path.join(data_dir, "stdout_bench")
    restart_logs(stdout_dir)

    # Re-add default console handler
    :logger.add_handler(:default, :logger_std_h, %{
      level: :info,
      formatter: {:logger_formatter, %{template: [:message, "\n"]}}
    })

    # Redirect stdout to /dev/null so we measure the IO cost without filling the terminal
    {:ok, devnull} = File.open("/dev/null", [:write])
    old_gl = Process.group_leader()
    Process.group_leader(self(), devnull)

    {stdout_us, _} =
      :timer.tc(fn ->
        for {msg, meta} <- log_args do
          Logger.info(msg, meta)
        end

        TimelessLogs.Buffer.flush()
        TimelessLogs.Index.sync()
      end)

    Process.group_leader(self(), old_gl)
    File.close(devnull)
    :logger.remove_handler(:default)

    stdout_eps = entry_count / (stdout_us / 1_000_000)

    IO.puts("  Wall time: #{fmt_ms(stdout_us)}")
    IO.puts("  Throughput: #{fmt_number(round(stdout_eps))} entries/sec")
    overhead_pct = (stdout_us - logger_us) / max(logger_us, 1) * 100

    IO.puts("  stdout overhead: #{:erlang.float_to_binary(overhead_pct, decimals: 1)}% slower\n")

    # Summary
    IO.puts("=== Summary ===")
    IO.puts("  Writer only:              #{fmt_number(round(writer_eps))} entries/sec")
    IO.puts("  Writer + Index (sync):    #{fmt_number(round(idx_eps))} entries/sec")
    IO.puts("  Buffer pipeline:          #{fmt_number(round(pipe_eps))} entries/sec")
    IO.puts("  Batch ingest API:         #{fmt_number(round(batch_eps))} entries/sec")
    IO.puts("  Logger (no stdout):       #{fmt_number(round(logger_eps))} entries/sec")
    IO.puts("  Logger (with stdout):     #{fmt_number(round(stdout_eps))} entries/sec")

    Application.stop(:timeless_logs)
    File.rm_rf!(data_dir)
  end

  defp configure_app(data_dir, overrides \\ []) do
    File.mkdir_p!(Path.join(data_dir, "blocks"))
    Application.put_env(:timeless_logs, :data_dir, data_dir)
    Application.put_env(:timeless_logs, :storage, :disk)
    Application.put_env(:timeless_logs, :compaction_interval, 600_000)

    Enum.each(overrides, fn {key, value} ->
      Application.put_env(:timeless_logs, key, value)
    end)
  end

  defp restart_logs(data_dir, overrides \\ []) do
    Application.stop(:timeless_logs)
    configure_app(data_dir, overrides)
    Application.ensure_all_started(:timeless_logs)
  end

  defp generate_entries(count) do
    base_ts = System.system_time(:second) - 86400

    Enum.map(1..count, fn i ->
      ts = base_ts + div(i, 10)
      level = Enum.random([:info, :info, :info, :debug, :debug, :warning, :error])

      %{
        timestamp: ts,
        level: level,
        message: "Request #{method()} #{path()} completed in #{:rand.uniform(500)}ms",
        metadata: %{
          "request_id" => random_hex(16),
          "module" => Enum.random(~w(Phoenix.Logger Ecto.Adapters.SQL MyApp.UserController)),
          "status" => "#{Enum.random([200, 200, 200, 201, 301, 400, 404, 500])}",
          "user_id" => "#{:rand.uniform(10_000)}"
        }
      }
    end)
  end

  defp method, do: Enum.random(~w(GET GET GET POST PUT DELETE))
  defp path, do: Enum.random(~w(/api/users /api/posts /api/comments /dashboard /health))
  defp random_hex(n), do: :crypto.strong_rand_bytes(n) |> Base.encode16(case: :lower)

  defp fmt_number(n) when n >= 1_000_000,
    do: "#{:erlang.float_to_binary(n / 1_000_000, decimals: 1)}M"

  defp fmt_number(n) when n >= 1_000, do: "#{:erlang.float_to_binary(n / 1_000, decimals: 1)}K"
  defp fmt_number(n), do: "#{n}"

  defp fmt_ms(us) when us < 1_000, do: "#{us}us"
  defp fmt_ms(us) when us < 1_000_000, do: "#{:erlang.float_to_binary(us / 1_000, decimals: 1)}ms"
  defp fmt_ms(us), do: "#{:erlang.float_to_binary(us / 1_000_000, decimals: 2)}s"
end
