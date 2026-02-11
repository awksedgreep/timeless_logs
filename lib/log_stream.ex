defmodule LogStream do
  @moduledoc """
  Embedded log compression and indexing for Elixir applications.

  LogStream plugs into Elixir's Logger as a handler, compresses log entries
  into zstd blocks, and indexes them in SQLite for fast querying.

  ## Setup

      # config/config.exs
      config :log_stream,
        data_dir: "priv/log_stream"

  The handler is installed automatically when the application starts.

  ## Querying

      # Find error logs from the last hour
      LogStream.query(level: :error, since: DateTime.add(DateTime.utc_now(), -3600))

      # Paginated results
      LogStream.query(level: :info, limit: 50, offset: 100, order: :asc)

      # Search log messages with metadata
      LogStream.query(message: "timeout", metadata: %{service: "api"})
  """

  @doc """
  Query stored logs. Returns a `LogStream.Result` struct.

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

      LogStream.query(level: :error)
      #=> {:ok, %LogStream.Result{entries: [...], total: 42, limit: 100, offset: 0}}

      LogStream.query(level: :warning, limit: 10, offset: 20)
      LogStream.query(message: "connection refused", metadata: %{service: "api"})
  """
  @spec query(keyword()) :: {:ok, LogStream.Result.t()} | {:error, term()}
  def query(filters \\ []) do
    LogStream.Index.query(filters)
  end

  @doc """
  Flush the buffer, writing any pending log entries to disk immediately.
  """
  @spec flush() :: :ok
  def flush do
    LogStream.Buffer.flush()
  end

  @doc """
  Lazily stream matching log entries without loading all results into memory.

  Returns an Elixir `Stream` that yields `%LogStream.Entry{}` structs.
  Blocks are decompressed on demand as the stream is consumed.

  Entries are returned in block order (oldest blocks first by default).
  For fully sorted results across blocks, use `query/1` instead.

  Accepts the same filter options as `query/1` except `:limit`, `:offset`,
  and `:order` which are ignored (use `Enum.take/2`, `Stream.drop/2`, etc.).

  ## Examples

      LogStream.stream(level: :error)
      |> Enum.take(10)

      LogStream.stream(since: DateTime.add(DateTime.utc_now(), -3600))
      |> Stream.filter(& &1.message =~ "timeout")
      |> Enum.to_list()
  """
  @spec stream(keyword()) :: Enumerable.t()
  def stream(filters \\ []) do
    block_ids = LogStream.Index.matching_block_ids(filters)
    search_filters = Keyword.drop(filters, [:limit, :offset, :order])
    storage = LogStream.Config.storage()

    Stream.flat_map(block_ids, fn {block_id, file_path, format} ->
      format_atom = if is_binary(format), do: String.to_existing_atom(format), else: format

      read_result =
        case storage do
          :disk -> LogStream.Writer.read_block(file_path, format_atom)
          :memory -> LogStream.Index.read_block_data(block_id)
        end

      case read_result do
        {:ok, entries} ->
          entries
          |> LogStream.Filter.filter(search_filters)
          |> Enum.map(&LogStream.Entry.from_map/1)

        {:error, _reason} ->
          []
      end
    end)
  end

  @doc """
  Return aggregate statistics about stored log data without reading any blocks.

  ## Examples

      {:ok, stats} = LogStream.stats()
      stats.total_blocks   #=> 42
      stats.total_entries   #=> 50000
      stats.disk_size       #=> 24_000_000
  """
  @spec stats() :: {:ok, LogStream.Stats.t()}
  def stats do
    LogStream.Index.stats()
  end

  @doc """
  Subscribe the calling process to receive new log entries as they arrive.

  The subscriber receives messages of the form:
  `{:log_stream, :entry, %LogStream.Entry{}}`.

  ## Options

    * `:level` - Only receive entries at this level (e.g., `:error`)
    * `:metadata` - Map of metadata key/value pairs that must match

  ## Examples

      LogStream.subscribe()
      receive do
        {:log_stream, :entry, entry} -> IO.inspect(entry)
      end

      LogStream.subscribe(level: :error)
  """
  @spec subscribe(keyword()) :: {:ok, pid()}
  def subscribe(opts \\ []) do
    Registry.register(LogStream.Registry, :log_entries, opts)
  end

  @doc """
  Unsubscribe the calling process from log entry notifications.
  """
  @spec unsubscribe() :: :ok
  def unsubscribe do
    Registry.unregister(LogStream.Registry, :log_entries)
  end
end
