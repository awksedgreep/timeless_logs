defmodule TimelessLogsTest do
  use ExUnit.Case, async: false

  require Logger

  @data_dir "test/tmp/log_stream"

  setup do
    # Stop app so we can reconfigure with test data dir
    Application.stop(:timeless_logs)
    File.rm_rf!(@data_dir)
    Application.put_env(:timeless_logs, :data_dir, @data_dir)
    Application.put_env(:timeless_logs, :flush_interval, 60_000)
    Application.put_env(:timeless_logs, :max_buffer_size, 10_000)
    Application.ensure_all_started(:timeless_logs)

    on_exit(fn ->
      Application.stop(:timeless_logs)
      File.rm_rf!(@data_dir)
    end)

    :ok
  end

  describe "full pipeline" do
    test "logs are compressed, indexed, and queryable" do
      Logger.info("test message one", domain: [:test])
      Logger.error("something broke", request_id: "req-123")
      Logger.warning("disk almost full", service: "storage")

      TimelessLogs.flush()

      {:ok, %TimelessLogs.Result{entries: errors, total: 1}} =
        TimelessLogs.query(level: :error)

      assert length(errors) == 1
      assert hd(errors).message =~ "something broke"
      assert %TimelessLogs.Entry{} = hd(errors)

      {:ok, %TimelessLogs.Result{entries: all, total: 3}} =
        TimelessLogs.query([])

      assert length(all) == 3
    end

    test "metadata filtering works" do
      Logger.error("timeout", service: "api", request_id: "abc")
      Logger.error("timeout", service: "web", request_id: "def")

      TimelessLogs.flush()

      {:ok, %TimelessLogs.Result{entries: results}} =
        TimelessLogs.query(level: :error, metadata: %{service: "api"})

      assert length(results) == 1
      assert hd(results).metadata["request_id"] == "abc"
    end

    test "metadata filtering works for host" do
      Logger.info("host-specific log", host: "prod-web-cache-01", service: "api")
      Logger.info("other host log", host: "web-02", service: "api")

      TimelessLogs.flush()

      {:ok, %TimelessLogs.Result{entries: results, total: 1}} =
        TimelessLogs.query(metadata: %{"host" => "prod-web-cache-01"})

      assert length(results) == 1
      assert hd(results).metadata["host"] == "prod-web-cache-01"
    end

    test "extract_terms keeps stable low-cardinality metadata and skips identifier-like values" do
      entries = [
        %{
          timestamp: System.os_time(:microsecond),
          level: :info,
          message: "request completed",
          metadata: %{
            service: "api",
            path: "/users",
            request_id: "9f6e7c19f0a24f91a3c2d4b5e6f70809",
            duration_ms: 123,
            webhook_id: "evt_01hxyz8j6m9r4s2t7u0v"
          }
        }
      ]

      terms = TimelessLogs.Index.extract_terms(entries)

      assert "level:info" in terms
      assert "service:api" in terms
      assert "path:/users" in terms
      refute Enum.any?(terms, &String.starts_with?(&1, "request_id:"))
      refute Enum.any?(terms, &String.starts_with?(&1, "duration_ms:"))
      refute Enum.any?(terms, &String.starts_with?(&1, "webhook_id:"))
    end

    test "message substring search" do
      Logger.info("user logged in successfully")
      Logger.info("user logged out")
      Logger.info("database connection established")

      TimelessLogs.flush()

      {:ok, %TimelessLogs.Result{entries: results, total: 2}} =
        TimelessLogs.query(message: "logged")

      assert length(results) == 2
    end

    test "pagination can skip exact totals" do
      for i <- 1..20 do
        Logger.info("paged log #{i}", page_test: true)
      end

      TimelessLogs.flush()

      {:ok, %TimelessLogs.Result{entries: page1, has_more: has_more, limit: 5}} =
        TimelessLogs.query(limit: 5, count_total: false)

      assert length(page1) == 5
      assert has_more

      {:ok, %TimelessLogs.Result{entries: page4, has_more: has_more_last, offset: 15}} =
        TimelessLogs.query(limit: 5, offset: 15, count_total: false)

      assert length(page4) == 5
      refute has_more_last
    end

    test "blocks are initially written as raw" do
      for i <- 1..10 do
        Logger.info("log entry number #{i}", iteration: "#{i}")
      end

      TimelessLogs.flush()

      blocks_dir = Path.join(@data_dir, "blocks")
      raw_files = Path.wildcard(Path.join(blocks_dir, "*.raw"))
      assert length(raw_files) >= 1

      # Raw blocks use term_to_binary without zstd compression
      file = hd(raw_files)
      data = File.read!(file)
      assert {:ok, entries} = TimelessLogs.Writer.decompress_block(data, :raw)
      assert length(entries) >= 1
    end

    test "time range filtering" do
      now = System.system_time(:second)

      Logger.info("recent log")
      TimelessLogs.flush()

      {:ok, %TimelessLogs.Result{entries: results}} =
        TimelessLogs.query(since: now - 10)

      assert length(results) >= 1

      {:ok, %TimelessLogs.Result{entries: []}} =
        TimelessLogs.query(since: now + 3600)
    end

    test "pagination with limit and offset" do
      for i <- 1..20 do
        Logger.info("entry #{String.pad_leading(Integer.to_string(i), 2, "0")}")
      end

      TimelessLogs.flush()

      {:ok, %TimelessLogs.Result{entries: page1, total: 20, limit: 5, offset: 0}} =
        TimelessLogs.query(limit: 5)

      assert length(page1) == 5

      {:ok, %TimelessLogs.Result{entries: page2, total: 20, offset: 5}} =
        TimelessLogs.query(limit: 5, offset: 5)

      assert length(page2) == 5
      assert hd(page1).timestamp != hd(page2).timestamp || hd(page1).message != hd(page2).message
    end

    test "nested map metadata does not crash extract_terms" do
      # Simulate VictoriaLogs-style ingest with nested meta (e.g. syslog)
      entry = %{
        timestamp: System.os_time(:microsecond),
        level: :info,
        message: "syslog message",
        metadata: %{
          service: "syslog",
          app: "syslog",
          meta: %{"source_ip" => "10.0.0.1", "facility" => 16, "hostname" => "router1"}
        }
      }

      TimelessLogs.Buffer.log(entry)
      TimelessLogs.flush()

      # Should be queryable by metadata (service filter)
      {:ok, %TimelessLogs.Result{entries: results}} =
        TimelessLogs.query(metadata: %{service: "syslog"})

      assert length(results) >= 1
      assert hd(results).message =~ "syslog"
    end

    test "ingest/1 accepts batched entries" do
      now = System.system_time(:second)

      entries = [
        %{timestamp: now, level: :info, message: "one", metadata: %{"service" => "api"}},
        %{timestamp: now + 1, level: :error, message: "two", metadata: %{"service" => "api"}}
      ]

      assert :ok = TimelessLogs.ingest(entries)
      TimelessLogs.flush()

      assert {:ok, %TimelessLogs.Result{total: 2}} = TimelessLogs.query([])
      assert {:ok, %TimelessLogs.Result{total: 1}} = TimelessLogs.query(level: :error)
    end

    test "ordering asc and desc" do
      Logger.info("first")
      Process.sleep(1100)
      Logger.info("second")
      TimelessLogs.flush()

      {:ok, %TimelessLogs.Result{entries: desc}} =
        TimelessLogs.query(order: :desc)

      {:ok, %TimelessLogs.Result{entries: asc}} =
        TimelessLogs.query(order: :asc)

      assert hd(desc).timestamp >= hd(asc).timestamp
    end

    test "count_total false preserves page slices for multi-block disk queries" do
      for block <- 1..6 do
        for item <- 1..4 do
          Logger.info("disk page block=#{block} item=#{item}", service: "disk-page")
        end

        TimelessLogs.flush()
      end

      {:ok, %TimelessLogs.Result{entries: all_entries, total: 24}} =
        TimelessLogs.query(limit: 24, order: :desc, metadata: %{service: "disk-page"})

      expected_messages = Enum.map(all_entries, & &1.message)
      page_size = 5

      paged_messages =
        0..4
        |> Enum.flat_map(fn page_index ->
          offset = page_index * page_size

          {:ok,
           %TimelessLogs.Result{
             entries: entries,
             has_more: has_more,
             total: reported_total,
             offset: ^offset,
             limit: ^page_size
           }} =
            TimelessLogs.query(
              limit: page_size,
              offset: offset,
              order: :desc,
              count_total: false,
              metadata: %{service: "disk-page"}
            )

          expected_count = min(page_size, max(length(expected_messages) - offset, 0))

          assert length(entries) == expected_count

          assert Enum.map(entries, & &1.message) ==
                   Enum.slice(expected_messages, offset, expected_count)

          assert has_more == offset + expected_count < length(expected_messages)

          expected_total =
            if has_more do
              offset + expected_count + 1
            else
              length(expected_messages)
            end

          assert reported_total == expected_total

          Enum.map(entries, & &1.message)
        end)

      assert paged_messages == expected_messages
      assert Enum.uniq(paged_messages) == paged_messages
    end

    test "count_total true and false return identical entries for the same page" do
      base = System.system_time(:second) - 1_000

      for block <- 1..4 do
        entries =
          for item <- 1..3 do
            seq = (block - 1) * 3 + item

            %{
              timestamp: base + seq,
              level: :info,
              message: "slice check block=#{block} item=#{item}",
              metadata: %{"service" => "slice-check"}
            }
          end

        assert :ok = TimelessLogs.ingest(entries)
        TimelessLogs.flush()
      end

      pagination = [limit: 4, offset: 4, order: :desc, metadata: %{service: "slice-check"}]

      {:ok, %TimelessLogs.Result{entries: exact_entries, total: exact_total, has_more: false}} =
        TimelessLogs.query(pagination)

      {:ok,
       %TimelessLogs.Result{entries: fast_entries, total: fast_total, has_more: fast_has_more}} =
        TimelessLogs.query(Keyword.put(pagination, :count_total, false))

      assert Enum.map(fast_entries, & &1.message) == Enum.map(exact_entries, & &1.message)
      assert fast_has_more
      assert exact_total == 12
      assert fast_total == 9
    end

    test "count_total false matches exact pages when disk blocks have uneven read cost" do
      large_payload = String.duplicate("x", 200_000)
      base = System.system_time(:second) - 2_000

      slow_entries =
        for item <- 1..3 do
          %{
            timestamp: base + item,
            level: :info,
            message: "slow-old block item=#{item} #{large_payload}",
            metadata: %{"service" => "uneven-page"}
          }
        end

      assert :ok = TimelessLogs.ingest(slow_entries)
      TimelessLogs.flush()

      for block <- 2..4 do
        entries =
          for item <- 1..3 do
            seq = (block - 1) * 3 + item

            %{
              timestamp: base + seq,
              level: :info,
              message: "fast block=#{block} item=#{item}",
              metadata: %{"service" => "uneven-page"}
            }
          end

        assert :ok = TimelessLogs.ingest(entries)
        TimelessLogs.flush()
      end

      for order <- [:asc, :desc] do
        for offset <- [0, 3, 6] do
          pagination = [
            limit: 3,
            offset: offset,
            order: order,
            metadata: %{service: "uneven-page"}
          ]

          {:ok, %TimelessLogs.Result{entries: exact_entries, total: 12}} =
            TimelessLogs.query(pagination)

          {:ok,
           %TimelessLogs.Result{
             entries: fast_entries,
             has_more: fast_has_more,
             total: fast_total
           }} =
            TimelessLogs.query(Keyword.put(pagination, :count_total, false))

          assert Enum.map(fast_entries, & &1.message) == Enum.map(exact_entries, & &1.message)
          assert fast_has_more == offset + 3 < 12

          expected_total =
            if fast_has_more do
              offset + 4
            else
              12
            end

          assert fast_total == expected_total
        end
      end
    end

    test "time filters preserve exact page slices for count_total false" do
      base = System.system_time(:second) - 5_000

      entries =
        for idx <- 1..18 do
          %{
            timestamp: base + idx,
            level: :info,
            message: "time-slice-#{idx}",
            metadata: %{"service" => "time-slice"}
          }
        end

      entries
      |> Enum.chunk_every(3)
      |> Enum.each(fn chunk ->
        assert :ok = TimelessLogs.ingest(chunk)
        TimelessLogs.flush()
      end)

      filters = [
        since: base + 5,
        until: base + 14,
        metadata: %{service: "time-slice"},
        order: :asc
      ]

      {:ok, %TimelessLogs.Result{entries: all_entries, total: 10}} =
        TimelessLogs.query([limit: 10] ++ filters)

      expected_messages = Enum.map(all_entries, & &1.message)

      for offset <- [0, 4, 8] do
        {:ok, %TimelessLogs.Result{entries: exact_entries, total: 10}} =
          TimelessLogs.query([limit: 4, offset: offset] ++ filters)

        {:ok,
         %TimelessLogs.Result{
           entries: fast_entries,
           total: fast_total,
           has_more: fast_has_more
         }} =
          TimelessLogs.query([limit: 4, offset: offset, count_total: false] ++ filters)

        expected_count = min(4, max(length(expected_messages) - offset, 0))

        assert Enum.map(exact_entries, & &1.message) ==
                 Enum.slice(expected_messages, offset, expected_count)

        assert Enum.map(fast_entries, & &1.message) == Enum.map(exact_entries, & &1.message)
        assert fast_has_more == offset + expected_count < 10
        assert fast_total == if(fast_has_more, do: offset + expected_count + 1, else: 10)
      end
    end

    test "equal timestamps still produce stable page equivalence" do
      timestamp = System.system_time(:second) - 9_000

      entries =
        for idx <- 1..12 do
          %{
            timestamp: timestamp,
            level: :info,
            message: "same-time-#{String.pad_leading(Integer.to_string(idx), 2, "0")}",
            metadata: %{"service" => "same-time"}
          }
        end

      entries
      |> Enum.chunk_every(3)
      |> Enum.each(fn chunk ->
        assert :ok = TimelessLogs.ingest(chunk)
        TimelessLogs.flush()
      end)

      pagination = [limit: 4, offset: 4, order: :desc, metadata: %{service: "same-time"}]

      {:ok, %TimelessLogs.Result{entries: exact_entries, total: 12}} =
        TimelessLogs.query(pagination)

      {:ok, %TimelessLogs.Result{entries: fast_entries, total: fast_total, has_more: true}} =
        TimelessLogs.query(Keyword.put(pagination, :count_total, false))

      assert Enum.map(fast_entries, & &1.message) == Enum.map(exact_entries, & &1.message)
      assert fast_total == 9
    end
  end
end
