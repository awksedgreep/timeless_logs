defmodule Mix.Tasks.TimelessLogs.IndexBenchmark do
  @moduledoc "Benchmark pure ETS index operations in isolation (no disk I/O)"
  use Mix.Task

  @shortdoc "Benchmark index ETS operations with in-memory storage"

  @block_count 2_000
  @entries_per_block 100
  @iterations 5
  @destructive_iterations 3

  @levels [:info, :info, :info, :debug, :debug, :warning, :error]
  @modules ~w(Phoenix.Logger Ecto.Adapters.SQL MyApp.Scheduler MyApp.Health MyApp.Monitor)

  @impl true
  def run(_args) do
    Application.put_env(:timeless_logs, :storage, :memory)
    Application.put_env(:timeless_logs, :compaction_interval, 600_000)
    Mix.Task.run("app.start")

    total_entries = @block_count * @entries_per_block

    IO.puts("=== TimelessLogs Index Benchmark ===\n")
    IO.puts("Seeding #{fmt_number(@block_count)} blocks (#{fmt_number(total_entries)} log entries, ~200K term index entries)...")

    {setup_us, _} = :timer.tc(fn -> seed_blocks() end)
    IO.puts("Setup: #{fmt_ms(setup_us)}\n")

    {:ok, stats} = TimelessLogs.Index.stats()
    term_count = :ets.info(:timeless_logs_term_index, :size)

    IO.puts("Total blocks: #{fmt_number(stats.total_blocks)}")
    IO.puts("Total entries: #{fmt_number(stats.total_entries)}")
    IO.puts("Total term index entries: #{fmt_number(term_count)}\n")

    six_hours_ago = System.system_time(:second) - 6 * 3600

    benchmarks = [
      {"stats()", fn -> TimelessLogs.Index.stats() end},
      {"raw_block_ids()", fn -> TimelessLogs.Index.raw_block_ids() end},
      {"matching_block_ids (no filter)", fn -> TimelessLogs.Index.matching_block_ids(limit: 1000) end},
      {"matching_block_ids (term filter)",
       fn -> TimelessLogs.Index.matching_block_ids(level: :error, limit: 1000) end},
      {"matching_block_ids (time range)",
       fn -> TimelessLogs.Index.matching_block_ids(since: six_hours_ago, limit: 1000) end},
      {"matching_block_ids (term + time)",
       fn ->
         TimelessLogs.Index.matching_block_ids(
           level: :error,
           since: six_hours_ago,
           limit: 1000
         )
       end}
    ]

    IO.puts("--- Index Operations (#{@iterations} iterations) ---")

    IO.puts(
      String.pad_trailing("Operation", 38) <>
        String.pad_leading("Median", 10) <>
        String.pad_leading("Min", 10) <>
        String.pad_leading("Max", 10)
    )

    IO.puts(String.duplicate("-", 68))

    for {label, fun} <- benchmarks do
      {median, min_l, max_l} = bench(fun)

      IO.puts(
        String.pad_trailing(label, 38) <>
          String.pad_leading(fmt_ms(median), 10) <>
          String.pad_leading(fmt_ms(min_l), 10) <>
          String.pad_leading(fmt_ms(max_l), 10)
      )
    end

    # Destructive operations — restart app + re-seed before each iteration
    IO.puts("\n--- Destructive Operations (#{@destructive_iterations} iterations, re-seed each) ---")

    IO.puts(
      String.pad_trailing("Operation", 38) <>
        String.pad_leading("Median", 10) <>
        String.pad_leading("Min", 10) <>
        String.pad_leading("Max", 10)
    )

    IO.puts(String.duplicate("-", 68))

    destructive_ops = [
      {"delete_blocks_over_size",
       fn ->
         {:ok, s} = TimelessLogs.Index.stats()
         TimelessLogs.Index.delete_blocks_over_size(div(s.total_bytes, 2))
       end},
      {"delete_by_term_limit",
       fn ->
         half_terms = div(:ets.info(:timeless_logs_term_index, :size), 2)
         TimelessLogs.Index.delete_oldest_blocks_until_term_limit(half_terms)
       end}
    ]

    for {label, fun} <- destructive_ops do
      times =
        for _ <- 1..@destructive_iterations do
          reseed()
          {us, _} = :timer.tc(fun)
          us
        end

      sorted = Enum.sort(times)
      median = Enum.at(sorted, div(@destructive_iterations, 2))
      min_l = hd(sorted)
      max_l = List.last(sorted)

      IO.puts(
        String.pad_trailing(label, 38) <>
          String.pad_leading(fmt_ms(median), 10) <>
          String.pad_leading(fmt_ms(min_l), 10) <>
          String.pad_leading(fmt_ms(max_l), 10)
      )
    end

    IO.puts("\n=== Summary ===")
    IO.puts("Total term index entries: #{fmt_number(term_count)}")
    IO.puts("Total blocks: #{fmt_number(stats.total_blocks)}")
    IO.puts("Total entries: #{fmt_number(stats.total_entries)}")

    Application.stop(:timeless_logs)
  end

  defp reseed do
    Application.stop(:timeless_logs)
    Application.ensure_all_started(:timeless_logs)
    seed_blocks()
  end

  defp seed_blocks do
    base_ts = System.system_time(:second) - 86_400

    for i <- 1..@block_count do
      chunk = generate_chunk(base_ts, i)

      case TimelessLogs.Writer.write_block(chunk, :memory, :raw) do
        {:ok, meta} ->
          terms = TimelessLogs.Index.extract_terms(chunk)
          TimelessLogs.Index.index_block(meta, chunk, terms)

        _ ->
          :ok
      end
    end
  end

  defp generate_chunk(base_ts, block_idx) do
    for j <- 1..@entries_per_block do
      ts = base_ts + block_idx * @entries_per_block + j
      level = Enum.at(@levels, rem(j, length(@levels)))
      mod = Enum.at(@modules, rem(block_idx + j, length(@modules)))

      %{
        timestamp: ts,
        level: level,
        message: "Request processed in #{:rand.uniform(500)}ms",
        metadata: %{
          "module" => mod,
          "request_id" => random_hex(16),
          "status" => "#{Enum.random([200, 201, 400, 404, 500])}"
        }
      }
    end
  end

  defp bench(fun) do
    times = for _ <- 1..@iterations, do: elem(:timer.tc(fun), 0)
    sorted = Enum.sort(times)
    {Enum.at(sorted, div(@iterations, 2)), hd(sorted), List.last(sorted)}
  end

  defp random_hex(n), do: :crypto.strong_rand_bytes(n) |> Base.encode16(case: :lower)

  defp fmt_number(n) when n >= 1_000_000,
    do: "#{:erlang.float_to_binary(n / 1_000_000, decimals: 1)}M"

  defp fmt_number(n) when n >= 1_000, do: "#{:erlang.float_to_binary(n / 1_000, decimals: 1)}K"
  defp fmt_number(n), do: "#{n}"

  defp fmt_ms(us) when us < 1_000, do: "#{us}us"
  defp fmt_ms(us) when us < 1_000_000, do: "#{:erlang.float_to_binary(us / 1_000, decimals: 1)}ms"
  defp fmt_ms(us), do: "#{:erlang.float_to_binary(us / 1_000_000, decimals: 2)}s"
end
