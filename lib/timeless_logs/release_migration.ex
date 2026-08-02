defmodule TimelessLogs.ReleaseMigration do
  @moduledoc false

  alias TimelessLogs.{DB, LegacyReader, LibsqlCandidate}

  @signal "logs"
  @journal_version 1
  @page_size 8_192
  @digest_modulus Integer.pow(2, 256)
  @minimum_headroom 64 * 1_024 * 1_024

  @doc "Stage or resume an immutable legacy-log conversion. Cutover is Session 3."
  def stage(data_dir, opts \\ []) do
    data_dir = Path.expand(data_dir)
    candidate_dir = Path.join([data_dir, ".timeless-migration", @signal])
    candidate_db = Path.join(candidate_dir, "logs.db")
    started = System.monotonic_time(:nanosecond)
    start_observation(candidate_db)

    with {:ok, reader} <- LegacyReader.open(data_dir, opts),
         {:ok, inventory} <- LegacyReader.inventory(reader),
         {:ok, manifest} <- source_manifest(data_dir, LegacyReader.manifest_paths(reader)),
         :ok <- preflight_disk(candidate_dir, manifest.bytes, opts),
         :ok <- File.mkdir_p(candidate_dir),
         {:ok, candidate} <-
           LibsqlCandidate.start_link(
             path: candidate_db,
             extension_path: Keyword.get(opts, :extension_path),
             retention_seconds:
               Keyword.get(opts, :retention_seconds, TimelessLogs.Config.retention_max_age())
           ) do
      try do
        with {:ok, journal} <- initialize_or_resume(candidate, manifest, inventory),
             {:ok, copied} <- copy_pages(reader, candidate, journal, opts),
             {:ok, maintenance} <- finish_public_maintenance(candidate),
             :ok <- LibsqlCandidate.phase(candidate, "validating"),
             :ok <- verify_manifest(data_dir, LegacyReader.manifest_paths(reader), manifest),
             :ok <- GenServer.stop(candidate),
             {:ok, validation} <-
               cold_validate(
                 candidate_db,
                 copied.records_completed,
                 copied.identity_digest,
                 opts
               ),
             {:ok, report} <-
               finish_report(
                 candidate_db,
                 manifest,
                 copied,
                 validation,
                 maintenance,
                 started,
                 opts
               ) do
          {:ok, report}
        end
      rescue
        error -> {:error, Exception.format(:error, error, __STACKTRACE__)}
      catch
        {:migration_failpoint, point} ->
          {:error, "injected migration failure at #{point}; committed work is resumable"}
      after
        LegacyReader.close(reader)
        if Process.alive?(candidate), do: GenServer.stop(candidate)
      end
    end
  end

  def candidate_path(data_dir) do
    Path.join([Path.expand(data_dir), ".timeless-migration", @signal, "logs.db"])
  end

  @doc false
  def legacy_manifest(data_dir, opts \\ []) do
    data_dir = Path.expand(data_dir)

    with {:ok, reader} <- LegacyReader.open(data_dir, opts) do
      try do
        source_manifest(data_dir, LegacyReader.manifest_paths(reader))
      after
        LegacyReader.close(reader)
      end
    end
  end

  @doc false
  def validate_checkpoint(path, opts \\ []) do
    with {:ok, conn} <- Exqlite.Sqlite3.open(path, mode: :readonly) do
      result =
        case DB.execute(
               conn,
               "SELECT records_completed,identity_digest FROM _timeless_migration WHERE singleton=1",
               []
             ) do
          {:ok, [[completed, digest]]} -> {:ok, completed, digest}
          other -> {:error, "invalid logs checkpoint journal: #{inspect(other)}"}
        end

      Exqlite.Sqlite3.close(conn)

      with {:ok, completed, digest} <- result,
           {:ok, _} <- cold_validate(path, completed, digest, opts) do
        :ok
      end
    end
  end

  defp initialize_or_resume(candidate, manifest, inventory) do
    for sql <- [
          """
          CREATE TABLE IF NOT EXISTS _timeless_migration (
            singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
            version INTEGER NOT NULL,
            signal TEXT NOT NULL,
            phase TEXT NOT NULL,
            source_manifest_json TEXT NOT NULL,
            source_manifest_digest TEXT NOT NULL,
            generation TEXT NOT NULL,
            cursor_json TEXT,
            records_completed INTEGER NOT NULL,
            records_total INTEGER NOT NULL,
            identity_digest TEXT NOT NULL,
            checkpoints INTEGER NOT NULL,
            retries INTEGER NOT NULL,
            started_at_ns INTEGER NOT NULL,
            updated_at_ns INTEGER NOT NULL
          ) STRICT
          """,
          """
          CREATE TABLE IF NOT EXISTS _timeless_migration_events (
            sequence INTEGER PRIMARY KEY AUTOINCREMENT,
            phase TEXT NOT NULL,
            cursor_json TEXT,
            records_completed INTEGER NOT NULL,
            at_ns INTEGER NOT NULL
          ) STRICT
          """
        ] do
      {:ok, _} = LibsqlCandidate.sql(candidate, sql)
    end

    {:ok, rows} =
      LibsqlCandidate.sql(
        candidate,
        """
        SELECT version,signal,phase,source_manifest_digest,generation,cursor_json,
               records_completed,records_total,identity_digest,checkpoints,retries
        FROM _timeless_migration WHERE singleton=1
        """
      )

    generation = Atom.to_string(inventory_generation(manifest))

    case rows do
      [] ->
        now = System.system_time(:nanosecond)

        {:ok, _} =
          LibsqlCandidate.sql(
            candidate,
            """
            INSERT INTO _timeless_migration
              (singleton,version,signal,phase,source_manifest_json,
               source_manifest_digest,generation,cursor_json,records_completed,
               records_total,identity_digest,checkpoints,retries,started_at_ns,updated_at_ns)
            VALUES (1,?1,?2,'copying',?3,?4,?5,NULL,0,?6,?7,0,0,?8,?8)
            """,
            [
              @journal_version,
              @signal,
              manifest.json,
              manifest.digest,
              generation,
              inventory.records,
              zero_digest(),
              now
            ]
          )

        read_journal(candidate)

      [
        [
          version,
          signal,
          phase,
          digest,
          stored_generation,
          cursor_json,
          completed,
          total,
          identity_digest,
          checkpoints,
          retries
        ]
      ] ->
        cond do
          version != @journal_version ->
            {:error,
             "incompatible logs migration journal version #{version}; expected #{@journal_version}"}

          signal != @signal ->
            {:error, "candidate journal belongs to #{signal}, not #{@signal}"}

          digest != manifest.digest ->
            {:error, "legacy logs source changed since migration began"}

          stored_generation != generation or total != inventory.records ->
            {:error, "legacy logs inventory changed since migration began"}

          true ->
            {:ok, _} =
              LibsqlCandidate.sql(
                candidate,
                "UPDATE _timeless_migration SET retries=retries+1 WHERE singleton=1"
              )

            {:ok,
             %{
               phase: phase,
               cursor: decode_cursor(cursor_json),
               records_completed: completed,
               records_total: total,
               identity_digest: identity_digest,
               checkpoints: checkpoints,
               retries: retries + 1,
               source_scan_ns: 0,
               public_write_ns: 0
             }}
        end
    end
  end

  defp read_journal(candidate) do
    {:ok, [[phase, cursor_json, completed, total, identity_digest, checkpoints, retries]]} =
      LibsqlCandidate.sql(
        candidate,
        """
        SELECT phase,cursor_json,records_completed,records_total,
               identity_digest,checkpoints,retries
        FROM _timeless_migration WHERE singleton=1
        """
      )

    {:ok,
     %{
       phase: phase,
       cursor: decode_cursor(cursor_json),
       records_completed: completed,
       records_total: total,
       identity_digest: identity_digest,
       checkpoints: checkpoints,
       retries: retries,
       source_scan_ns: 0,
       public_write_ns: 0
     }}
  end

  defp copy_pages(
         _reader,
         _candidate,
         %{records_completed: total, records_total: total} = state,
         _opts
       ),
       do: {:ok, state}

  defp copy_pages(reader, candidate, state, opts) do
    scan_started = System.monotonic_time(:nanosecond)

    case LegacyReader.page(reader, state.cursor, @page_size) do
      {:ok, entries, cursor, has_more?} ->
        scan_ns = System.monotonic_time(:nanosecond) - scan_started

        with {:ok, digest} <- digest_entries(entries, state.identity_digest) do
          next = %{
            state
            | phase: "copying",
              cursor: cursor,
              records_completed: state.records_completed + length(entries),
              identity_digest: digest,
              source_scan_ns: state.source_scan_ns + scan_ns
          }

          checkpoint = state.checkpoints + 1
          failpoint = selected_failpoint(opts[:failpoint], checkpoint)
          if failpoint == :before_batch, do: throw({:migration_failpoint, :before_batch})

          journal = %{
            phase: next.phase,
            cursor_json: encode_cursor(next.cursor),
            records_completed: next.records_completed,
            identity_digest: next.identity_digest,
            updated_at_ns: System.system_time(:nanosecond)
          }

          write_started = System.monotonic_time(:nanosecond)

          with :ok <-
                 LibsqlCandidate.checkpoint(candidate, entries, journal,
                   failpoint: failpoint,
                   final_page: not has_more?
                 ) do
            committed = %{
              next
              | checkpoints: checkpoint,
                public_write_ns:
                  next.public_write_ns + System.monotonic_time(:nanosecond) - write_started
            }

            observe_hwm(LibsqlCandidate.path(candidate))

            if failpoint == :after_checkpoint,
              do: throw({:migration_failpoint, :after_checkpoint})

            if has_more?,
              do: copy_pages(reader, candidate, committed, opts),
              else: {:ok, committed}
          end
        end

      {:error, reason} ->
        {:error, "failed reading legacy logs: #{inspect(reason)}"}
    end
  end

  defp finish_public_maintenance(candidate) do
    flush_started = System.monotonic_time(:nanosecond)

    with {:ok, _} <- LibsqlCandidate.command(candidate, "flush") do
      flush_ns = System.monotonic_time(:nanosecond) - flush_started
      optimize_started = System.monotonic_time(:nanosecond)

      with {:ok, _} <- LibsqlCandidate.command(candidate, "optimize") do
        optimize_ns = System.monotonic_time(:nanosecond) - optimize_started
        checkpoint_started = System.monotonic_time(:nanosecond)

        with {:ok, _} <- LibsqlCandidate.sql(candidate, "PRAGMA wal_checkpoint(TRUNCATE)") do
          {:ok,
           %{
             flush_ns: flush_ns,
             optimize_ns: optimize_ns,
             checkpoint_ns: System.monotonic_time(:nanosecond) - checkpoint_started
           }}
        end
      end
    end
  end

  defp cold_validate(path, expected_count, expected_digest, opts) do
    with {:ok, conn, _capabilities} <-
           LibsqlCandidate.open_readonly_connection(path, Keyword.get(opts, :extension_path)) do
      try do
        with {:ok, [["ok"]]} <- DB.execute(conn, "PRAGMA integrity_check", []),
             {:ok, [[native_count]]} <- DB.execute(conn, "SELECT COUNT(*) FROM logs", []),
             {:ok, actual} <- target_digest(conn) do
          if native_count == expected_count and actual.records == expected_count and
               actual.digest == expected_digest and actual.timestamp_ordered? do
            {:ok, actual}
          else
            {:error,
             "cold logs parity mismatch: expected records=#{expected_count} digest=#{expected_digest}; " <>
               "actual count=#{native_count}/#{actual.records} digest=#{actual.digest} " <>
               "timestamp_ordered=#{actual.timestamp_ordered?}"}
          end
        else
          other -> {:error, "cold logs validation failed: #{inspect(other)}"}
        end
      after
        Exqlite.Sqlite3.close(conn)
      end
    end
  end

  defp target_digest(conn) do
    sql = "SELECT ts,level,message,metadata FROM logs ORDER BY ts ASC"

    with {:ok, statement} <- Exqlite.Sqlite3.prepare(conn, sql) do
      try do
        target_rows(conn, statement, %{
          records: 0,
          digest: zero_digest(),
          ts_min: nil,
          ts_max: nil,
          previous_ts: nil,
          timestamp_ordered?: true
        })
      after
        Exqlite.Sqlite3.release(conn, statement)
      end
    end
  end

  defp target_rows(conn, statement, state) do
    case Exqlite.Sqlite3.step(conn, statement) do
      {:row, [ts, severity, message, metadata]} ->
        identity = entry_identity(ts, severity, message, metadata)

        next = %{
          state
          | records: state.records + 1,
            digest: digest_add(state.digest, identity),
            ts_min: if(state.ts_min == nil, do: ts, else: min(state.ts_min, ts)),
            ts_max: if(state.ts_max == nil, do: ts, else: max(state.ts_max, ts)),
            previous_ts: ts,
            timestamp_ordered?:
              state.timestamp_ordered? and (state.previous_ts == nil or ts >= state.previous_ts)
        }

        target_rows(conn, statement, next)

      :done ->
        {:ok, state}

      {:error, reason} ->
        {:error, "stream target logs: #{inspect(reason)}"}
    end
  end

  defp finish_report(path, manifest, state, validation, maintenance, started, opts) do
    observe_hwm(path)
    observed = :persistent_term.get({__MODULE__, :observed})

    with {:ok, conn, _} <-
           LibsqlCandidate.open_connection(path, Keyword.get(opts, :extension_path)) do
      try do
        now = System.system_time(:nanosecond)
        {:ok, _} = DB.execute(conn, "BEGIN IMMEDIATE", [])

        {:ok, _} =
          DB.execute(
            conn,
            "UPDATE _timeless_migration SET phase='verified',updated_at_ns=?1 WHERE singleton=1",
            [now]
          )

        {:ok, _} =
          DB.execute(
            conn,
            "INSERT INTO _timeless_migration_events(phase,cursor_json,records_completed,at_ns) SELECT 'verified',cursor_json,records_completed,?1 FROM _timeless_migration WHERE singleton=1",
            [now]
          )

        {:ok, _} = DB.execute(conn, "COMMIT", [])

        {:ok, [[0, _frames, _checkpointed]]} =
          DB.execute(conn, "PRAGMA wal_checkpoint(TRUNCATE)", [])

        {:ok, [[pages, page_size, freelist]]} =
          DB.execute(
            conn,
            "SELECT (SELECT page_count FROM pragma_page_count),(SELECT page_size FROM pragma_page_size),(SELECT freelist_count FROM pragma_freelist_count)",
            []
          )

        elapsed_ns = max(System.monotonic_time(:nanosecond) - started, 1)

        {:ok,
         %{
           signal: @signal,
           phase: :verified,
           candidate: path,
           source_manifest_digest: manifest.digest,
           source_bytes: manifest.bytes,
           records: validation.records,
           ts_min: validation.ts_min,
           ts_max: validation.ts_max,
           checkpoints: state.checkpoints,
           retries: state.retries,
           identity_digest: validation.digest,
           source_scan_ns: state.source_scan_ns,
           public_write_ns: state.public_write_ns,
           flush_ns: maintenance.flush_ns,
           optimize_ns: maintenance.optimize_ns,
           checkpoint_ns: maintenance.checkpoint_ns,
           durable_records_per_second: validation.records * 1_000_000_000 / elapsed_ns,
           elapsed_ns: elapsed_ns,
           candidate_bytes: file_size(path),
           wal_bytes: file_size(path <> "-wal"),
           physical_bytes:
             file_size(path) + file_size(path <> "-wal") + file_size(path <> "-shm"),
           migration_rss_baseline_bytes: observed.rss_baseline,
           migration_rss_hwm_bytes: observed.rss_hwm,
           migration_rss_delta_bytes: max(observed.rss_hwm - observed.rss_baseline, 0),
           candidate_peak_physical_bytes: observed.candidate_peak_bytes,
           sqlite_logical_bytes: pages * page_size,
           sqlite_freelist_bytes: freelist * page_size,
           process_hwm_bytes: process_hwm_bytes()
         }}
      after
        Exqlite.Sqlite3.close(conn)
      end
    end
  end

  defp digest_entries(entries, digest) do
    Enum.reduce_while(entries, {:ok, digest}, fn entry, {:ok, acc} ->
      case LibsqlCandidate.canonical_entry(entry) do
        {:ok, canonical} ->
          {:cont,
           {:ok,
            digest_add(
              acc,
              entry_identity(
                canonical.timestamp,
                canonical.severity,
                canonical.message,
                canonical.metadata
              )
            )}}

        {:error, _} = error ->
          {:halt, error}
      end
    end)
  end

  defp entry_identity(ts, severity, message, metadata) do
    [
      <<ts::signed-big-64>>,
      sized(severity),
      sized(message),
      sized(metadata)
    ]
  end

  defp source_manifest(root, paths) do
    files =
      paths
      |> Enum.flat_map(&regular_files/1)
      |> Enum.uniq()
      |> Enum.sort()
      |> Enum.map(fn path ->
        stat = File.stat!(path, time: :posix)

        %{
          path: Path.relative_to(path, root),
          size: stat.size,
          mtime: stat.mtime,
          sha256: sha256_file(path)
        }
      end)

    generation =
      if Enum.any?(files, &(&1.path == "logs_index.db")), do: :sqlite, else: :snapshot_log

    json = canonical_json(%{version: 1, signal: @signal, generation: generation, files: files})

    {:ok,
     %{
       files: files,
       json: json,
       digest: sha256(json),
       bytes: Enum.sum_by(files, & &1.size),
       generation: generation
     }}
  rescue
    error -> {:error, "failed to inventory legacy logs source: #{Exception.message(error)}"}
  end

  defp inventory_generation(manifest), do: manifest.generation

  defp verify_manifest(root, paths, expected) do
    case source_manifest(root, paths) do
      {:ok, %{digest: digest}} when digest == expected.digest -> :ok
      {:ok, _} -> {:error, "legacy logs source changed during migration"}
      {:error, _} = error -> error
    end
  end

  defp regular_files(path) do
    cond do
      File.regular?(path) -> [path]
      File.dir?(path) -> path |> File.ls!() |> Enum.flat_map(&regular_files(Path.join(path, &1)))
      true -> raise "legacy source contains non-regular path #{path}"
    end
  end

  defp preflight_disk(candidate_dir, source_bytes, opts) do
    parent = Path.dirname(candidate_dir)
    File.mkdir_p!(parent)
    base = max(source_bytes * 2, @minimum_headroom)
    required = base + div(base, 4)
    available = Keyword.get_lazy(opts, :available_bytes, fn -> available_bytes(parent) end)

    if available >= required do
      :ok
    else
      {:error,
       "insufficient disk for logs migration: require #{required} bytes including WAL and 25% safety margin; #{available} bytes available"}
    end
  end

  defp available_bytes(path) do
    case System.cmd("df", ["-Pk", path], stderr_to_stdout: true) do
      {output, 0} ->
        output
        |> String.split("\n", trim: true)
        |> List.last()
        |> String.split(~r/\s+/, trim: true)
        |> Enum.at(3)
        |> String.to_integer()
        |> Kernel.*(1_024)

      {output, status} ->
        raise "cannot determine free space (df exit #{status}): #{String.trim(output)}"
    end
  end

  defp selected_failpoint({point, checkpoint}, checkpoint), do: point
  defp selected_failpoint(point, _checkpoint) when is_atom(point), do: point
  defp selected_failpoint(_, _checkpoint), do: nil

  defp encode_cursor(nil), do: nil

  defp encode_cursor({generation, block_id, ordinal}) do
    canonical_json(%{generation: generation, block_id: block_id, ordinal: ordinal})
  end

  defp decode_cursor(nil), do: nil

  defp decode_cursor(json) do
    value = :json.decode(json)
    {value["generation"], value["block_id"], value["ordinal"]}
  end

  defp digest_add(digest, identity) do
    current = digest |> Base.decode16!(case: :mixed) |> :binary.decode_unsigned()

    addition =
      identity
      |> IO.iodata_to_binary()
      |> then(&:crypto.hash(:sha256, &1))
      |> :binary.decode_unsigned()

    encoded = rem(current + addition, @digest_modulus) |> :binary.encode_unsigned()
    padded = :binary.copy(<<0>>, 32 - byte_size(encoded)) <> encoded
    Base.encode16(padded, case: :lower)
  end

  defp sized(value) when is_binary(value), do: [<<byte_size(value)::unsigned-big-32>>, value]
  defp zero_digest, do: String.duplicate("0", 64)
  defp canonical_json(value), do: value |> :json.encode() |> IO.iodata_to_binary()
  defp sha256(data), do: :crypto.hash(:sha256, data) |> Base.encode16(case: :lower)

  defp sha256_file(path) do
    File.open!(path, [:read, :binary], fn io -> hash_io(:crypto.hash_init(:sha256), io) end)
    |> :crypto.hash_final()
    |> Base.encode16(case: :lower)
  end

  defp hash_io(context, io) do
    case IO.binread(io, 1_048_576) do
      :eof -> context
      data when is_binary(data) -> hash_io(:crypto.hash_update(context, data), io)
      {:error, reason} -> raise "read source: #{inspect(reason)}"
    end
  end

  defp process_hwm_bytes do
    case File.read("/proc/self/status") do
      {:ok, status} ->
        case Regex.run(~r/^VmHWM:\s+(\d+)\s+kB$/m, status) do
          [_, kib] -> String.to_integer(kib) * 1_024
          _ -> :erlang.memory(:total)
        end

      _ ->
        :erlang.memory(:total)
    end
  end

  defp observe_hwm(candidate_db) do
    current = process_rss_bytes()
    prior = :persistent_term.get({__MODULE__, :observed})

    :persistent_term.put({__MODULE__, :observed}, %{
      prior
      | rss_hwm: max(prior.rss_hwm, current),
        candidate_peak_bytes: max(prior.candidate_peak_bytes, physical_size(candidate_db))
    })
  end

  defp start_observation(candidate_db) do
    baseline = process_rss_bytes()

    :persistent_term.put(
      {__MODULE__, :observed},
      %{
        rss_baseline: baseline,
        rss_hwm: baseline,
        candidate_peak_bytes: physical_size(candidate_db)
      }
    )
  end

  defp process_rss_bytes do
    case File.read("/proc/self/status") do
      {:ok, status} ->
        case Regex.run(~r/^VmRSS:\s+(\d+)\s+kB$/m, status) do
          [_, kib] -> String.to_integer(kib) * 1_024
          _ -> :erlang.memory(:total)
        end

      _ ->
        :erlang.memory(:total)
    end
  end

  defp physical_size(path),
    do: file_size(path) + file_size(path <> "-wal") + file_size(path <> "-shm")

  defp file_size(path) do
    case File.stat(path) do
      {:ok, %{size: size}} -> size
      _ -> 0
    end
  end
end
