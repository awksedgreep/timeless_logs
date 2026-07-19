# Container HTTP workload for timeless_logs — auto-ramp NDJSON ingest + LogsQL queries.
#
# Pure HTTP client (Finch). Run from a project that has Finch available, e.g.:
#
#   cd ../timeless_metrics && mix run --no-start ../timeless_logs/bench/container_http_workload.exs \
#     --url http://127.0.0.1:19428 --writers 16 --batch 500
#
# Ramps by halving the per-writer POST interval each step until saturation
# (write p99 > 100ms, error rate > 5%, or throughput < 60% of target).
# Query workers run a fixed LogsQL mix alongside writes the whole time.

defmodule LogsHttpWorkload do
  @p99_ceiling_us 100_000
  @error_rate_ceil 0.05
  @throughput_floor 0.60
  @min_interval_ms 2

  @services ~w(api web worker billing auth search notifications ingest scheduler admin)
  @hosts Enum.map(1..50, fn i -> "host-#{i}" end)
  @tables ~w(users posts comments sessions notifications tags)
  @paths [
    "/api/v1/users",
    "/api/v1/posts",
    "/api/v1/sessions",
    "/api/v1/notifications",
    "/dashboard",
    "/dashboard/metrics",
    "/health",
    "/live/updates"
  ]
  @warn_msgs [
    "Connection pool checkout timeout after 5000ms",
    "Slow query detected (>100ms)",
    "Rate limit approaching for API key",
    "Memory usage above 80% threshold"
  ]
  @err_msgs [
    "Internal server error: timeout",
    "Internal server error: connection_refused",
    "Internal server error: pool_timeout",
    "Internal server error: deadlock_detected"
  ]

  @query_templates [
    {"errors_5m", "_time:5m level:error | limit 100"},
    {"service_1h", "_time:1h level:info service:api | limit 100"},
    {"substr_15m", ~s(_time:15m "timeout" | limit 50)},
    {"count_1h", "_time:1h level:error | stats count(*)"},
    {"tail_5m", "_time:5m | limit 100"}
  ]

  def run do
    {:ok, _} = Application.ensure_all_started(:finch)
    Process.flag(:trap_exit, true)

    {opts, _, _} =
      OptionParser.parse(System.argv(),
        switches: [
          url: :string,
          writers: :integer,
          batch: :integer,
          step_seconds: :integer,
          start_interval: :float,
          query_workers: :integer,
          warmup: :integer
        ]
      )

    url = opts[:url] || "http://127.0.0.1:19428"
    writers = opts[:writers] || 16
    batch = opts[:batch] || 500
    step_dur = opts[:step_seconds] || 15
    start_interval = opts[:start_interval] || 1000.0
    query_workers = opts[:query_workers] || 10
    warmup_s = opts[:warmup] || 5

    IO.puts("")
    IO.puts("  " <> String.duplicate("=", 64))
    IO.puts("  TimelessLogs Container HTTP Workload — Auto-Ramp")
    IO.puts("  " <> String.duplicate("=", 64))
    IO.puts("  Target:      #{url}")
    IO.puts("  Writers:     #{writers} x #{batch} entries/POST (NDJSON /insert/jsonline)")
    IO.puts("  Ramp:        interval ÷2 from #{trunc(start_interval)}ms until saturation")
    IO.puts("  Step dur:    #{step_dur}s (#{warmup_s}s warmup)")

    IO.puts(
      "  Queries:     #{query_workers} workers, LogsQL mix: " <>
        Enum.map_join(@query_templates, ", ", fn {n, _} -> n end)
    )

    IO.puts("  " <> String.duplicate("=", 64))

    Finch.start_link(
      name: BenchFinch,
      pools: %{default: [size: writers + query_workers + 10, count: 1]}
    )

    verify(url)
    tails = pregenerate_tails(20_000)

    # Warmup
    warm_body = build_body(tails, batch)

    Enum.each(1..warmup_s, fn _ ->
      Enum.each(1..writers, fn _ -> post_lines(url, warm_body) end)
      Process.sleep(1000)
    end)

    steps = ramp(url, tails, writers, batch, step_dur, start_interval, query_workers, [])

    print_results(steps)
    print_health(url)
  end

  defp ramp(url, tails, writers, batch, step_dur, interval_ms, query_workers, acc) do
    target_eps = writers * batch * 1000 / interval_ms

    IO.write(
      "  Step #{length(acc) + 1}: #{fmt_ms(interval_ms)} (~#{fmt_num(target_eps)} entries/s) ... "
    )

    write_ets = :ets.new(:w, [:duplicate_bag, :public, {:write_concurrency, true}])
    query_ets = :ets.new(:q, [:duplicate_bag, :public, {:write_concurrency, true}])
    # 1=reqs 2=werrs 3=queries 4=qerrs
    ctr = :counters.new(4, [:atomics])

    stop = :atomics.new(1, [])

    writer_pids =
      Enum.map(1..writers, fn i ->
        spawn_link(fn ->
          Process.sleep(trunc(interval_ms * (i - 1) / writers))
          writer_loop(url, tails, batch, interval_ms, write_ets, ctr, stop)
        end)
      end)

    query_pids =
      Enum.map(1..query_workers, fn i ->
        spawn_link(fn ->
          Process.sleep(i * 50)
          query_loop(url, query_ets, ctr, stop)
        end)
      end)

    Process.sleep(step_dur * 1000)
    :atomics.put(stop, 1, 1)
    Process.sleep(300)
    Enum.each(writer_pids ++ query_pids, &Process.exit(&1, :kill))

    w_lat = :ets.tab2list(write_ets) |> Enum.map(&elem(&1, 1)) |> Enum.sort()
    q_lat = :ets.tab2list(query_ets) |> Enum.map(&elem(&1, 1)) |> Enum.sort()
    :ets.delete(write_ets)
    :ets.delete(query_ets)

    reqs = :counters.get(ctr, 1)
    werrs = :counters.get(ctr, 2)
    queries = :counters.get(ctr, 3)
    qerrs = :counters.get(ctr, 4)

    actual_eps = reqs * batch / step_dur
    err_rate = if reqs + werrs > 0, do: werrs / (reqs + werrs), else: 0.0
    p99 = pct(w_lat, 0.99)

    step = %{
      interval: interval_ms,
      target_eps: target_eps,
      actual_eps: actual_eps,
      reqs_s: reqs / step_dur,
      werrs: werrs,
      qps: queries / step_dur,
      qerrs: qerrs,
      w_p50: pct(w_lat, 0.50),
      w_p99: p99,
      w_p999: pct(w_lat, 0.999),
      q_p50: pct(q_lat, 0.50),
      q_p99: pct(q_lat, 0.99),
      q_p999: pct(q_lat, 0.999)
    }

    IO.puts("#{fmt_num(actual_eps)} entries/s, w_p99 #{fmt_us(p99)}, #{trunc(step.qps)} qps")

    saturated =
      p99 > @p99_ceiling_us or err_rate > @error_rate_ceil or
        actual_eps < target_eps * @throughput_floor or interval_ms / 2 < @min_interval_ms

    if saturated do
      reason =
        cond do
          p99 > @p99_ceiling_us ->
            "write p99 #{fmt_us(p99)} > 100ms"

          err_rate > @error_rate_ceil ->
            "error rate #{Float.round(err_rate * 100, 1)}%"

          actual_eps < target_eps * @throughput_floor ->
            "throughput #{fmt_num(actual_eps)} < 60% of target"

          true ->
            "min interval reached"
        end

      IO.puts("  >> Saturated: #{reason}")
      Enum.reverse([step | acc])
    else
      ramp(url, tails, writers, batch, step_dur, interval_ms / 2, query_workers, [step | acc])
    end
  end

  defp writer_loop(url, tails, batch, interval_ms, ets, ctr, stop) do
    if :atomics.get(stop, 1) == 1 do
      :ok
    else
      body = build_body(tails, batch)
      t0 = System.monotonic_time(:microsecond)
      ok = post_lines(url, body)
      elapsed = System.monotonic_time(:microsecond) - t0

      if ok do
        :ets.insert(ets, {:l, elapsed})
        :counters.add(ctr, 1, 1)
      else
        :counters.add(ctr, 2, 1)
      end

      sleep = max(trunc(interval_ms) - div(elapsed, 1000), 0)
      if sleep > 0, do: Process.sleep(sleep)
      writer_loop(url, tails, batch, interval_ms, ets, ctr, stop)
    end
  end

  defp query_loop(url, ets, ctr, stop) do
    if :atomics.get(stop, 1) == 1 do
      :ok
    else
      {_name, q} = Enum.random(@query_templates)
      t0 = System.monotonic_time(:microsecond)

      req =
        Finch.build(
          :post,
          url <> "/select/logsql/query",
          [{"content-type", "application/x-www-form-urlencoded"}],
          URI.encode_query(%{"query" => q})
        )

      case Finch.request(req, BenchFinch, receive_timeout: 30_000) do
        {:ok, %{status: s}} when s in 200..299 ->
          :ets.insert(ets, {:l, System.monotonic_time(:microsecond) - t0})
          :counters.add(ctr, 3, 1)

        _ ->
          :counters.add(ctr, 4, 1)
      end

      Process.sleep(50)
      query_loop(url, ets, ctr, stop)
    end
  end

  defp post_lines(url, body) do
    req =
      Finch.build(
        :post,
        url <> "/insert/jsonline",
        [{"content-type", "application/x-ndjson"}],
        body
      )

    case Finch.request(req, BenchFinch, receive_timeout: 30_000) do
      {:ok, %{status: s}} when s in 200..299 -> true
      _ -> false
    end
  end

  # Pre-generate JSON line tails (everything after the timestamp) to keep
  # client CPU out of the measurement. Each POST stamps fresh _time values.
  defp pregenerate_tails(n) do
    tails =
      Enum.map(1..n, fn _ ->
        svc = Enum.random(@services)
        host = Enum.random(@hosts)
        req_id = Base.encode16(:crypto.strong_rand_bytes(8), case: :lower)

        {level, msg, extra} =
          case :rand.uniform(100) do
            r when r <= 40 ->
              {"debug",
               ~s(SELECT * FROM \\"#{Enum.random(@tables)}\\" WHERE id = $1 [#{:rand.uniform(10_000)}]),
               ~s(,"source":"#{Enum.random(@tables)}","query_time":"#{:rand.uniform(50)}ms")}

            r when r <= 85 ->
              {"info",
               "#{Enum.random(~w(GET GET GET POST PUT DELETE))} #{Enum.random(@paths)} -> #{Enum.random([200, 200, 200, 201, 204, 304])} in #{:rand.uniform(200)}ms",
               ~s(,"status":"200","duration":"#{:rand.uniform(200)}")}

            r when r <= 95 ->
              {"warning", Enum.random(@warn_msgs), ""}

            _ ->
              {"error", Enum.random(@err_msgs),
               ~s(,"reason":"#{Enum.random(~w(timeout pool_timeout deadlock nxdomain))}")}
          end

        ~s(,"_msg":"#{msg}","level":"#{level}","service":"#{svc}","host":"#{host}","request_id":"#{req_id}"#{extra}})
      end)

    List.to_tuple(tails)
  end

  defp build_body(tails, batch) do
    ts = System.system_time(:second)
    n = tuple_size(tails)

    Enum.map(1..batch, fn _ ->
      [~s({"_time":), Integer.to_string(ts), elem(tails, :rand.uniform(n) - 1), "\n"]
    end)
  end

  defp verify(url) do
    req = Finch.build(:get, url <> "/health")

    case Finch.request(req, BenchFinch, receive_timeout: 5_000) do
      {:ok, %{status: 200, body: body}} -> IO.puts("  Target OK: #{body}")
      other -> raise "target #{url} not healthy: #{inspect(other)}"
    end
  end

  defp print_health(url) do
    req = Finch.build(:get, url <> "/health")

    case Finch.request(req, BenchFinch, receive_timeout: 60_000) do
      {:ok, %{status: 200, body: body}} -> IO.puts("\n  Final /health: #{body}")
      other -> IO.puts("\n  Final /health failed: #{inspect(other)}")
    end
  end

  defp print_results(steps) do
    IO.puts("\n  Write Latency")
    IO.puts("  " <> String.duplicate("-", 78))

    IO.puts(
      "  " <>
        pad("Interval", 10) <>
        pad("Req/s", 8) <>
        pad("Entries/s", 12) <>
        pad("p50", 10) <> pad("p99", 10) <> pad("p999", 10) <> pad("errs", 6)
    )

    Enum.each(steps, fn s ->
      IO.puts(
        "  " <>
          pad(fmt_ms(s.interval), 10) <>
          pad("#{trunc(s.reqs_s)}", 8) <>
          pad(fmt_num(s.actual_eps), 12) <>
          pad(fmt_us(s.w_p50), 10) <>
          pad(fmt_us(s.w_p99), 10) <> pad(fmt_us(s.w_p999), 10) <> pad("#{s.werrs}", 6)
      )
    end)

    IO.puts("\n  Query Latency Under Write Load")
    IO.puts("  " <> String.duplicate("-", 78))

    IO.puts(
      "  " <>
        pad("W Entries/s", 12) <>
        pad("Q/s", 8) <>
        pad("p50", 10) <>
        pad("p99", 10) <> pad("p999", 10) <> pad("errs", 6)
    )

    Enum.each(steps, fn s ->
      IO.puts(
        "  " <>
          pad(fmt_num(s.actual_eps), 12) <>
          pad("#{Float.round(s.qps, 1)}", 8) <>
          pad(fmt_us(s.q_p50), 10) <>
          pad(fmt_us(s.q_p99), 10) <>
          pad(fmt_us(s.q_p999), 10) <> pad("#{s.qerrs}", 6)
      )
    end)

    peak = Enum.max_by(steps, & &1.actual_eps)

    IO.puts(
      "\n  Peak ingest: #{fmt_num(peak.actual_eps)} entries/s (write p99 #{fmt_us(peak.w_p99)})"
    )
  end

  defp pct([], _), do: 0

  defp pct(sorted, p) do
    idx = min(trunc(length(sorted) * p), length(sorted) - 1)
    Enum.at(sorted, idx)
  end

  defp pad(s, n), do: String.pad_trailing(s, n)

  defp fmt_ms(ms) when ms >= 1000, do: "#{Float.round(ms / 1000, 1)}s"
  defp fmt_ms(ms), do: "#{trunc(ms)}ms"

  defp fmt_us(us) when us >= 1_000_000, do: "#{Float.round(us / 1_000_000, 2)}s"
  defp fmt_us(us) when us >= 1_000, do: "#{Float.round(us / 1_000, 2)}ms"
  defp fmt_us(us), do: "#{trunc(us)}us"

  defp fmt_num(n) when n >= 1_000_000, do: "#{Float.round(n / 1_000_000, 1)}M"
  defp fmt_num(n) when n >= 1_000, do: "#{Float.round(n / 1_000, 1)}K"
  defp fmt_num(n), do: "#{trunc(n)}"
end

LogsHttpWorkload.run()
