defmodule TimelessLogs do
  @moduledoc """
  Embedded log compression and indexing for Elixir applications.

  TimelessLogs plugs into Elixir's Logger as a handler, compresses log entries
  into compressed blocks, and indexes them in ETS for fast querying.

  ## Setup

      # config/config.exs
      config :timeless_logs,
        data_dir: "priv/log_stream"

  The handler is installed automatically when the application starts.

  ## Querying

      # Find error logs from the last hour
      TimelessLogs.query(level: :error, since: DateTime.add(DateTime.utc_now(), -3600))

      # Paginated results
      TimelessLogs.query(level: :info, limit: 50, offset: 100, order: :asc)

      # Search log messages with metadata
      TimelessLogs.query(message: "timeout", metadata: %{service: "api"})
  """

  @doc """
  Query stored logs. Returns a `TimelessLogs.Result` struct.

  ## Filters

    * `:level` - Log level atom (`:debug`, `:info`, `:warning`, `:error`)
    * `:message` - Substring match on log message or metadata values
    * `:since` - DateTime or unix timestamp lower bound
    * `:until` - DateTime or unix timestamp upper bound
    * `:metadata` - Map of metadata key/value pairs to match

  ## Pagination & Ordering

    * `:limit` - Max entries to return (default 100)
    * `:offset` - Number of entries to skip (default 0)
    * `:order` - `:desc` (newest first, default) or `:asc` (oldest first)

  ## Examples

      TimelessLogs.query(level: :error)
      #=> {:ok, %TimelessLogs.Result{entries: [...], total: 42, limit: 100, offset: 0}}

      TimelessLogs.query(level: :warning, limit: 10, offset: 20)
      TimelessLogs.query(message: "connection refused", metadata: %{service: "api"})
  """
  @spec query(keyword()) :: {:ok, TimelessLogs.Result.t()} | {:error, term()}
  def query(filters \\ []) do
    filters
    |> normalize_filters()
    |> TimelessLogs.Index.query()
  end

  @doc """
  Flush the buffer, writing any pending log entries to disk immediately.
  """
  @spec flush() :: :ok
  def flush do
    TimelessLogs.Buffer.flush()
  end

  @doc """
  Ingest multiple log entries in one call.

  This is the lower-overhead path for batch producers such as NDJSON imports.
  """
  @spec ingest([map()]) :: :ok
  def ingest(entries) when is_list(entries) do
    TimelessLogs.Buffer.log_many(entries)
  end

  @doc """
  Collect distinct values and hit counts for a given field.

  Handles built-in fields (`"_msg"`, `"_time"`, `"level"`) and metadata fields.

  ## Examples

      TimelessLogs.field_values("level", since: DateTime.add(DateTime.utc_now(), -3600))
      #=> {:ok, [%{"value" => "info", "hits" => 706}, %{"value" => "error", "hits" => 42}]}
  """
  @spec field_values(String.t(), keyword()) :: {:ok, list(map())}
  def field_values(field_name, filters \\ []) do
    filters = normalize_filters(filters)

    counts =
      stream(filters)
      |> Enum.reduce(%{}, fn entry, acc ->
        value = extract_field(entry, field_name)

        if value do
          Map.update(acc, value, 1, &(&1 + 1))
        else
          acc
        end
      end)

    values =
      counts
      |> Enum.map(fn {value, hits} -> %{"value" => to_string(value), "hits" => hits} end)
      |> Enum.sort_by(& &1["hits"], :desc)

    {:ok, values}
  end

  @doc """
  Collect all field names and hit counts from matching entries.

  Always includes `_msg`, `_time`, and `level`. Metadata keys are also included.

  ## Examples

      TimelessLogs.field_names(since: DateTime.add(DateTime.utc_now(), -3600))
      #=> {:ok, [%{"value" => "_msg", "hits" => 1094}, ...]}
  """
  @spec field_names(keyword()) :: {:ok, list(map())}
  def field_names(filters \\ []) do
    filters = normalize_filters(filters)

    counts =
      stream(filters)
      |> Enum.reduce(%{}, fn entry, acc ->
        # Built-in fields always present
        acc =
          acc
          |> Map.update("_msg", 1, &(&1 + 1))
          |> Map.update("_time", 1, &(&1 + 1))
          |> Map.update("level", 1, &(&1 + 1))

        # Add metadata keys
        case entry.metadata do
          meta when is_map(meta) and map_size(meta) > 0 ->
            Enum.reduce(meta, acc, fn {k, _v}, inner ->
              Map.update(inner, to_string(k), 1, &(&1 + 1))
            end)

          _ ->
            acc
        end
      end)

    values =
      counts
      |> Enum.map(fn {name, hits} -> %{"value" => name, "hits" => hits} end)
      |> Enum.sort_by(& &1["hits"], :desc)

    {:ok, values}
  end

  defp extract_field(entry, "_msg"), do: entry.message
  defp extract_field(entry, "_time"), do: entry.timestamp
  defp extract_field(entry, "level"), do: entry.level

  defp extract_field(entry, field) do
    key = String.to_atom(field)

    case entry.metadata do
      meta when is_map(meta) -> Map.get(meta, key)
      _ -> nil
    end
  end

  @doc """
  Lazily stream matching log entries without loading all results into memory.

  Returns an Elixir `Stream` that yields `%TimelessLogs.Entry{}` structs.
  Blocks are decompressed on demand as the stream is consumed.

  Entries are returned in block order (oldest blocks first by default).
  For fully sorted results across blocks, use `query/1` instead.

  Accepts the same filter options as `query/1` except `:limit`, `:offset`,
  and `:order` which are ignored (use `Enum.take/2`, `Stream.drop/2`, etc.).

  ## Examples

      TimelessLogs.stream(level: :error)
      |> Enum.take(10)

      TimelessLogs.stream(since: DateTime.add(DateTime.utc_now(), -3600))
      |> Stream.filter(& &1.message =~ "timeout")
      |> Enum.to_list()
  """
  @spec stream(keyword()) :: Enumerable.t()
  def stream(filters \\ []) do
    filters = normalize_filters(filters)
    block_ids = TimelessLogs.Index.matching_block_ids(filters)
    search_filters = Keyword.drop(filters, [:limit, :offset, :order])
    storage = TimelessLogs.Config.storage()

    Stream.flat_map(block_ids, fn {block_id, file_path, format} ->
      format_atom = if is_binary(format), do: String.to_existing_atom(format), else: format

      read_result =
        case storage do
          :disk -> TimelessLogs.Writer.read_block(file_path, format_atom)
          :memory -> TimelessLogs.Index.read_block_data(block_id)
        end

      case read_result do
        {:ok, entries} ->
          entries
          |> TimelessLogs.Filter.filter(search_filters)
          |> Enum.map(&TimelessLogs.Entry.from_map/1)

        {:error, _reason} ->
          []
      end
    end)
  end

  @doc """
  Return aggregate statistics about stored log data without reading any blocks.

  ## Examples

      {:ok, stats} = TimelessLogs.stats()
      stats.total_blocks   #=> 42
      stats.total_entries   #=> 50000
      stats.disk_size       #=> 24_000_000
  """
  @spec stats() :: {:ok, TimelessLogs.Stats.t()}
  def stats do
    TimelessLogs.Index.stats()
  end

  @doc """
  Subscribe the calling process to receive new log entries as they arrive.

  The subscriber receives messages of the form:
  `{:timeless_logs, :entry, %TimelessLogs.Entry{}}`.

  ## Options

    * `:level` - Only receive entries at this level (e.g., `:error`)
    * `:metadata` - Map of metadata key/value pairs that must match

  ## Examples

      TimelessLogs.subscribe()
      receive do
        {:timeless_logs, :entry, entry} -> IO.inspect(entry)
      end

      TimelessLogs.subscribe(level: :error)
  """
  @spec subscribe(keyword()) :: {:ok, pid()}
  def subscribe(opts \\ []) do
    Registry.register(TimelessLogs.Registry, :log_entries, normalize_filters(opts))
  end

  @doc """
  Unsubscribe the calling process from log entry notifications.
  """
  @spec unsubscribe() :: :ok
  def unsubscribe do
    Registry.unregister(TimelessLogs.Registry, :log_entries)
  end

  @spec normalize_filters(keyword()) :: keyword()
  def normalize_filters(filters) do
    {metadata_any, metadata} =
      filters
      |> Keyword.get(:metadata, %{})
      |> Enum.reduce({[], %{}}, fn {key, value}, {any_filters, exact_filters} ->
        case semantic_metadata_aliases(key, value) do
          nil ->
            {any_filters, Map.put(exact_filters, key, value)}

          aliases ->
            {[{:metadata_any, aliases} | any_filters], exact_filters}
        end
      end)

    filters =
      case metadata do
        map when map_size(map) == 0 -> Keyword.delete(filters, :metadata)
        _ -> Keyword.put(filters, :metadata, metadata)
      end

    Keyword.merge(filters, Enum.reverse(metadata_any))
  end

  defp semantic_metadata_aliases(key, value) when key in [:host, "host"] do
    [
      {"host.name", value},
      {"host", value},
      {"hostname", value},
      {"node", value}
    ]
  end

  defp semantic_metadata_aliases(key, value) when key in [:service, "service"] do
    [
      {"service.name", value},
      {"service", value},
      {"application", value}
    ]
  end

  defp semantic_metadata_aliases(_key, _value), do: nil

  @doc """
  Merge multiple small compressed blocks into fewer, larger blocks.

  Returns `:ok` if blocks were merged, `:noop` if no merge was needed.
  """
  @spec merge_now() :: :ok | :noop
  defdelegate merge_now(), to: TimelessLogs.Compactor

  @doc """
  Create a consistent online backup of the log store.

  Flushes all in-flight data, writes an ETS index snapshot,
  and copies block files to the target directory.

  ## Parameters

    * `target_dir` - Directory to write backup files into (will be created)

  ## Returns

      {:ok, %{path: target_dir, files: [filenames], total_bytes: integer()}}

  ## Examples

      TimelessLogs.backup("/tmp/log_backup_2024")
  """
  @spec backup(String.t()) :: {:ok, map()} | {:error, term()}
  def backup(target_dir) do
    flush()

    File.mkdir_p!(target_dir)

    # Backup index snapshot
    index_target = Path.join(target_dir, "index.snapshot")

    case TimelessLogs.Index.backup(index_target) do
      :ok ->
        # Copy block files in parallel
        data_dir = TimelessLogs.Config.data_dir()
        blocks_src = Path.join(data_dir, "blocks")
        blocks_dst = Path.join(target_dir, "blocks")

        block_bytes = copy_block_files(blocks_src, blocks_dst)
        index_bytes = File.stat!(index_target).size

        {:ok,
         %{
           path: target_dir,
           files: ["index.snapshot", "blocks"],
           total_bytes: index_bytes + block_bytes
         }}

      {:error, _} = err ->
        err
    end
  end

  defp copy_block_files(src_dir, dst_dir) do
    case File.ls(src_dir) do
      {:ok, files} ->
        File.mkdir_p!(dst_dir)

        files
        |> Task.async_stream(
          fn file ->
            src = Path.join(src_dir, file)
            dst = Path.join(dst_dir, file)
            File.cp!(src, dst)
            File.stat!(dst).size
          end,
          max_concurrency: System.schedulers_online()
        )
        |> Enum.reduce(0, fn {:ok, size}, acc -> acc + size end)

      {:error, :enoent} ->
        0
    end
  end
end
