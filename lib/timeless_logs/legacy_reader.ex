defmodule TimelessLogs.LegacyReader do
  @moduledoc false

  @page_limit 8_192
  @default_max_stored_bytes 64 * 1_024 * 1_024
  @default_max_decoded_bytes 256 * 1_024 * 1_024

  defstruct [
    :root,
    :generation,
    :conn,
    :exclusive,
    :blocks,
    :block_data,
    :max_stored_bytes,
    :max_decoded_bytes
  ]

  @type cursor :: {String.t(), integer(), non_neg_integer()}
  @type t :: %__MODULE__{}

  def open(data_dir, opts \\ []) do
    root = Path.expand(data_dir)
    generation = Keyword.get(opts, :generation, detect_generation(root))

    limits = %{
      max_stored_bytes: Keyword.get(opts, :max_stored_bytes, @default_max_stored_bytes),
      max_decoded_bytes: Keyword.get(opts, :max_decoded_bytes, @default_max_decoded_bytes),
      exclusive: Keyword.get(opts, :exclusive, false)
    }

    case generation do
      :sqlite -> open_sqlite(root, limits)
      :snapshot_log -> open_snapshot(root, limits)
      other -> {:error, "unsupported logs legacy generation #{inspect(other)}"}
    end
  end

  def close(%__MODULE__{conn: nil}), do: :ok

  def close(%__MODULE__{conn: conn, exclusive: exclusive?}) do
    if exclusive?, do: execute(conn, "ROLLBACK")
    Exqlite.Sqlite3.close(conn)
  end

  def inventory(%__MODULE__{generation: :sqlite, conn: conn}) do
    with {:ok, [[blocks, records, bytes, ts_min, ts_max]]} <-
           execute(
             conn,
             "SELECT COUNT(*),COALESCE(SUM(entry_count),0),COALESCE(SUM(byte_size),0),MIN(ts_min),MAX(ts_max) FROM blocks"
           ) do
      {:ok,
       %{blocks: blocks, records: records, stored_bytes: bytes, ts_min: ts_min, ts_max: ts_max}}
    end
  end

  def inventory(%__MODULE__{blocks: blocks}) do
    values = Map.values(blocks)

    {:ok,
     %{
       blocks: length(values),
       records: Enum.sum_by(values, & &1.entry_count),
       stored_bytes: Enum.sum_by(values, & &1.byte_size),
       ts_min: values |> Enum.map(& &1.ts_min) |> Enum.min(fn -> nil end),
       ts_max: values |> Enum.map(& &1.ts_max) |> Enum.max(fn -> nil end)
     }}
  end

  def manifest_paths(%__MODULE__{root: root, generation: :sqlite}) do
    [
      Path.join(root, "logs_index.db"),
      Path.join(root, "logs_index.db-wal"),
      Path.join(root, "blocks")
    ]
    |> Enum.filter(&durable_manifest_path?/1)
  end

  def manifest_paths(%__MODULE__{root: root, generation: :snapshot_log}) do
    administrative =
      [
        Path.join(root, "index.snapshot"),
        Path.join(root, "index.log"),
        Path.join(root, "index.log.idx"),
        Path.join(root, "blocks")
      ]
      |> Enum.filter(&File.exists?/1)

    administrative
  end

  defp durable_manifest_path?(path) do
    case File.stat(path) do
      {:ok, %{type: :regular, size: 0}} -> not String.ends_with?(path, ".db-wal")
      {:ok, _} -> true
      {:error, _} -> false
    end
  end

  def page(reader, cursor \\ nil, limit \\ @page_limit)
      when is_integer(limit) and limit in 1..@page_limit do
    generation = Atom.to_string(reader.generation)
    {block_id, ordinal} = decode_cursor(cursor, generation)

    with {:ok, rows} <- block_rows(reader, block_id) do
      fill_page(reader, rows, block_id, ordinal, limit, [])
    end
  end

  defp open_sqlite(root, limits) do
    path = Path.join(root, "logs_index.db")

    with true <- File.regular?(path) || {:error, "missing logs SQLite index #{path}"},
         {:ok, conn} <- open_sqlite_connection(path, limits.exclusive) do
      case validate_sqlite_connection(conn) do
        :ok ->
          {:ok,
           struct!(
             __MODULE__,
             Map.merge(limits, %{
               root: root,
               generation: :sqlite,
               conn: conn,
               blocks: nil,
               block_data: nil
             })
           )}

        {:error, _} = error ->
          if limits.exclusive, do: execute(conn, "ROLLBACK")
          Exqlite.Sqlite3.close(conn)
          error
      end
    else
      {:error, _} = error -> error
      other -> {:error, "invalid logs SQLite index: #{inspect(other)}"}
    end
  rescue
    error -> {:error, "cannot open logs SQLite index read-only: #{Exception.message(error)}"}
  end

  defp validate_sqlite_connection(conn) do
    with {:ok, [["ok"]]} <- execute(conn, "PRAGMA integrity_check"),
         :ok <- verify_sqlite_schema(conn) do
      :ok
    else
      {:error, _} = error -> error
      other -> {:error, "invalid logs SQLite index: #{inspect(other)}"}
    end
  end

  defp open_sqlite_connection(path, false), do: Exqlite.Sqlite3.open(path, mode: :readonly)

  defp open_sqlite_connection(path, true) do
    case Exqlite.Sqlite3.open(path) do
      {:ok, conn} ->
        result =
          with :ok <- execute_once(conn, "PRAGMA busy_timeout=0"),
               :ok <- execute_once(conn, "BEGIN EXCLUSIVE") do
            {:ok, conn}
          end

        case result do
          {:ok, _} ->
            result

          {:error, reason} ->
            Exqlite.Sqlite3.close(conn)
            {:error, "cannot acquire exclusive legacy SQLite ownership: #{inspect(reason)}"}
        end

      {:error, reason} ->
        {:error, "cannot acquire exclusive legacy SQLite ownership: #{inspect(reason)}"}
    end
  end

  defp execute_once(conn, sql) do
    with {:ok, statement} <- Exqlite.Sqlite3.prepare(conn, sql) do
      try do
        case Exqlite.Sqlite3.step(conn, statement) do
          :done -> :ok
          {:row, _} -> :ok
          {:error, _} = error -> error
          other -> {:error, other}
        end
      after
        Exqlite.Sqlite3.release(conn, statement)
      end
    end
  end

  defp verify_sqlite_schema(conn) do
    with {:ok, [[version]]} <-
           execute(
             conn,
             "SELECT CAST(value AS INTEGER) FROM _metadata WHERE key='schema_version'"
           ),
         true <- version in 1..2,
         {:ok, [[8]]} <-
           execute(
             conn,
             "SELECT COUNT(*) FROM pragma_table_info('blocks') WHERE name IN ('block_id','file_path','byte_size','entry_count','ts_min','ts_max','format','created_at')"
           ) do
      :ok
    else
      other -> {:error, "unsupported logs SQLite index schema: #{inspect(other)}"}
    end
  end

  defp open_snapshot(root, limits) do
    snapshot_path = Path.join(root, "index.snapshot")

    with {:ok, stat} <- File.stat(snapshot_path),
         :ok <- enforce_size(stat.size, limits.max_stored_bytes, snapshot_path),
         {:ok, binary} <- File.read(snapshot_path),
         {:ok, snapshot} <- decode_snapshot(binary),
         {:ok, blocks} <- snapshot_blocks(snapshot, root),
         {:ok, blocks, block_data} <-
           replay_log(
             root,
             Map.get(snapshot, :timestamp, 0),
             blocks,
             Map.new(Map.get(snapshot, :block_data, []))
           ) do
      {:ok,
       struct!(
         __MODULE__,
         Map.merge(limits, %{
           root: root,
           generation: :snapshot_log,
           conn: nil,
           blocks: blocks,
           block_data: block_data
         })
       )}
    else
      {:error, reason} -> {:error, "cannot open logs snapshot generation: #{inspect(reason)}"}
    end
  end

  defp decode_snapshot(binary) do
    snapshot = :erlang.binary_to_term(binary, [:safe])

    if is_map(snapshot) and Map.get(snapshot, :version) == 1 and is_list(snapshot[:blocks]) do
      {:ok, snapshot}
    else
      {:error, :unsupported_snapshot}
    end
  rescue
    _ -> {:error, :corrupt_snapshot}
  end

  defp snapshot_blocks(snapshot, root) do
    Enum.reduce_while(snapshot.blocks, {:ok, %{}}, fn row, {:ok, blocks} ->
      case block_from_row(row, root) do
        {:ok, block} -> {:cont, {:ok, Map.put(blocks, block.id, block)}}
        {:error, _} = error -> {:halt, error}
      end
    end)
  end

  defp replay_log(root, snapshot_timestamp, blocks, block_data) do
    path = Path.join(root, "index.log")

    if File.exists?(path) do
      name = {:timeless_logs_migration_reader, System.unique_integer([:positive])}

      case :disk_log.open(
             name: name,
             file: String.to_charlist(path),
             type: :halt,
             format: :internal,
             mode: :read_only,
             repair: false
           ) do
        {:ok, ^name} ->
          try do
            replay_chunks(name, :start, snapshot_timestamp, blocks, block_data, root)
          after
            :disk_log.close(name)
          end

        {:error, reason} ->
          {:error, {:disk_log, reason}}
      end
    else
      {:ok, blocks, block_data}
    end
  end

  defp replay_chunks(name, continuation, timestamp, blocks, block_data, root) do
    case :disk_log.chunk(name, continuation) do
      :eof -> {:ok, blocks, block_data}
      {:error, reason} -> {:error, {:disk_log, reason}}
      {next, terms} -> replay_terms(name, next, terms, timestamp, blocks, block_data, root)
      {_next, _terms, bad_bytes} -> {:error, {:corrupt_disk_log, bad_bytes}}
    end
  end

  defp replay_terms(name, next, terms, timestamp, blocks, block_data, root) do
    result =
      Enum.reduce_while(terms, {:ok, blocks, block_data}, fn term, {:ok, acc, data} ->
        if is_tuple(term) and tuple_size(term) >= 2 and elem(term, 1) > timestamp do
          case apply_log_term(term, acc, data, root) do
            {:ok, updated, updated_data} -> {:cont, {:ok, updated, updated_data}}
            {:error, _} = error -> {:halt, error}
          end
        else
          {:cont, {:ok, acc, data}}
        end
      end)

    case result do
      {:ok, updated, updated_data} ->
        replay_chunks(name, next, timestamp, updated, updated_data, root)

      {:error, _} = error ->
        error
    end
  end

  defp apply_log_term({:index_block, _timestamp, meta, _terms}, blocks, data, root) do
    with {:ok, block} <- block_from_map(meta, root) do
      data = if meta[:data], do: Map.put(data, block.id, meta.data), else: data
      {:ok, Map.put(blocks, block.id, block), data}
    end
  end

  defp apply_log_term({:delete_blocks, _timestamp, ids}, blocks, data, _root)
       when is_list(ids) do
    {:ok, Map.drop(blocks, ids), Map.drop(data, ids)}
  end

  defp apply_log_term(
         {:compact_blocks, _timestamp, old_ids, new_meta_terms, _sizes},
         blocks,
         data,
         root
       ) do
    Enum.reduce_while(
      new_meta_terms,
      {:ok, Map.drop(blocks, old_ids), Map.drop(data, old_ids)},
      fn
        {meta, _terms}, {:ok, acc, acc_data} ->
          case block_from_map(meta, root) do
            {:ok, block} ->
              acc_data =
                if meta[:data], do: Map.put(acc_data, block.id, meta.data), else: acc_data

              {:cont, {:ok, Map.put(acc, block.id, block), acc_data}}

            {:error, _} = error ->
              {:halt, error}
          end
      end
    )
  end

  defp apply_log_term({:update_compression_stats, _, _, _}, blocks, data, _root),
    do: {:ok, blocks, data}

  defp apply_log_term(term, _blocks, _data, _root),
    do: {:error, {:unknown_disk_log_term, term}}

  defp block_rows(%__MODULE__{generation: :sqlite, conn: conn}, after_id) do
    execute(
      conn,
      "SELECT block_id,file_path,byte_size,entry_count,ts_min,ts_max,format,created_at FROM blocks WHERE block_id>=?1 ORDER BY block_id LIMIT 256",
      [after_id]
    )
  end

  defp block_rows(%__MODULE__{blocks: blocks}, after_id) do
    rows =
      blocks
      |> Map.values()
      |> Enum.filter(&(&1.id >= after_id))
      |> Enum.sort_by(& &1.id)

    {:ok, rows}
  end

  defp fill_page(%__MODULE__{generation: :sqlite} = reader, [], block_id, ordinal, remaining, acc) do
    case block_rows(reader, block_id) do
      {:ok, []} -> {:ok, Enum.reverse(acc), cursor(reader, block_id, ordinal), false}
      {:ok, rows} -> fill_page(reader, rows, block_id, ordinal, remaining, acc)
      {:error, _} = error -> error
    end
  end

  defp fill_page(reader, [], block_id, ordinal, _remaining, acc) do
    {:ok, Enum.reverse(acc), cursor(reader, block_id, ordinal), false}
  end

  defp fill_page(reader, [row | rest], block_id, ordinal, remaining, acc) do
    with {:ok, block} <- normalize_block_row(row, reader.root),
         {:ok, entries} <- decode_block(reader, block) do
      start = if block.id == block_id, do: ordinal, else: 0
      available = Enum.drop(entries, start)
      taken = Enum.take(available, remaining)
      next_ordinal = start + length(taken)
      acc = Enum.reverse(taken, acc)

      cond do
        length(taken) == remaining and next_ordinal < length(entries) ->
          {:ok, Enum.reverse(acc), cursor(reader, block.id, next_ordinal), true}

        length(taken) == remaining ->
          {:ok, Enum.reverse(acc), cursor(reader, block.id + 1, 0),
           rest != [] or more_blocks?(reader, block.id + 1)}

        true ->
          fill_page(reader, rest, block.id + 1, 0, remaining - length(taken), acc)
      end
    end
  end

  defp decode_block(reader, block) do
    with :ok <- enforce_size(block.byte_size, reader.max_stored_bytes, block.path),
         {:ok, data} <- block_data(reader, block),
         true <- byte_size(data) == block.byte_size || {:error, :stored_size_mismatch},
         {:ok, entries} <- TimelessLogs.Writer.decompress_block(data, block.format),
         true <- length(entries) == block.entry_count || {:error, :entry_count_mismatch},
         :ok <- enforce_size(:erlang.external_size(entries), reader.max_decoded_bytes, block.path) do
      {:ok, Enum.map(entries, &normalize_entry/1)}
    else
      {:error, _} = error -> error
      other -> {:error, {:invalid_block, block.id, other}}
    end
  end

  defp block_data(%__MODULE__{conn: conn}, %{path: nil, id: id}) when not is_nil(conn) do
    case execute(conn, "SELECT data FROM block_data WHERE block_id=?1", [id]) do
      {:ok, [[data]]} -> {:ok, data}
      other -> {:error, {:missing_block_data, id, other}}
    end
  end

  defp block_data(%__MODULE__{block_data: data}, %{path: nil, id: id}) do
    case Map.fetch(data, id) do
      {:ok, bytes} -> {:ok, bytes}
      :error -> {:error, {:missing_block_data, id}}
    end
  end

  defp block_data(_reader, %{path: path}), do: File.read(path)

  defp normalize_entry(%TimelessLogs.Entry{} = entry),
    do: TimelessLogs.Entry.from_map(Map.from_struct(entry))

  defp normalize_entry(entry) when is_map(entry), do: TimelessLogs.Entry.from_map(entry)

  defp normalize_block_row(%{id: _} = block, _root), do: {:ok, block}

  defp normalize_block_row([id, path, bytes, count, ts_min, ts_max, format, created], root) do
    block_from_row({id, path, bytes, count, ts_min, ts_max, format, created}, root)
  end

  defp block_from_row({id, path, bytes, count, ts_min, ts_max, format, created}, root) do
    block_from_map(
      %{
        block_id: id,
        file_path: path,
        byte_size: bytes,
        entry_count: count,
        ts_min: ts_min,
        ts_max: ts_max,
        format: format,
        created_at: created
      },
      root
    )
  end

  defp block_from_map(meta, root) do
    path = map_get(meta, :file_path)

    with {:ok, path} <- safe_block_path(root, path),
         format when format in [:raw, :zstd, :openzl] <- normalize_format(map_get(meta, :format)),
         id when is_integer(id) <- map_get(meta, :block_id),
         bytes when is_integer(bytes) and bytes >= 0 <- map_get(meta, :byte_size),
         count when is_integer(count) and count >= 0 <- map_get(meta, :entry_count) do
      {:ok,
       %{
         id: id,
         path: path,
         byte_size: bytes,
         entry_count: count,
         ts_min: map_get(meta, :ts_min),
         ts_max: map_get(meta, :ts_max),
         format: format,
         created_at: map_get(meta, :created_at)
       }}
    else
      other -> {:error, {:invalid_block_metadata, other}}
    end
  end

  defp safe_block_path(_root, nil), do: {:ok, nil}

  defp safe_block_path(root, path) when is_binary(path) do
    expanded = Path.expand(path, root)

    cond do
      not String.starts_with?(expanded <> "/", root <> "/") ->
        {:error, {:path_escape, path}}

      File.exists?(expanded) and File.lstat!(expanded).type == :symlink ->
        {:error, {:symlink, path}}

      not File.regular?(expanded) ->
        {:error, {:missing_block, path}}

      true ->
        {:ok, expanded}
    end
  end

  defp safe_block_path(_root, path), do: {:error, {:invalid_path, path}}

  defp more_blocks?(%__MODULE__{generation: :sqlite, conn: conn}, block_id) do
    match?(
      {:ok, [[1]]},
      execute(conn, "SELECT EXISTS(SELECT 1 FROM blocks WHERE block_id>=?1)", [block_id])
    )
  end

  defp more_blocks?(%__MODULE__{}, _block_id), do: false

  defp normalize_format(format) when format in [:raw, :zstd, :openzl], do: format
  defp normalize_format("raw"), do: :raw
  defp normalize_format("zstd"), do: :zstd
  defp normalize_format("openzl"), do: :openzl
  defp normalize_format(_), do: :invalid

  defp cursor(reader, block_id, ordinal),
    do: {Atom.to_string(reader.generation), block_id, ordinal}

  defp decode_cursor(nil, _generation), do: {-9_223_372_036_854_775_808, 0}
  defp decode_cursor({generation, block_id, ordinal}, generation), do: {block_id, ordinal}

  defp decode_cursor(cursor, generation),
    do: raise("cursor #{inspect(cursor)} is not for #{generation}")

  defp detect_generation(root) do
    sqlite? = File.regular?(Path.join(root, "logs_index.db"))
    snapshot? = File.regular?(Path.join(root, "index.snapshot"))

    case {sqlite?, snapshot?} do
      {true, false} -> :sqlite
      {false, true} -> :snapshot_log
      {false, false} -> :missing
      {true, true} -> :ambiguous
    end
  end

  defp enforce_size(size, maximum, _path) when size <= maximum, do: :ok
  defp enforce_size(size, maximum, path), do: {:error, {:oversized, path, size, maximum}}

  defp execute(conn, sql, params \\ []) do
    TimelessLogs.DB.execute(conn, sql, params)
  rescue
    error -> {:error, Exception.message(error)}
  end

  defp map_get(map, key), do: Map.get(map, key, Map.get(map, Atom.to_string(key)))
end
