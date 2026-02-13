defmodule TimelessLogs.Writer do
  @moduledoc false

  require Logger

  @type block_meta :: %{
          block_id: integer(),
          file_path: String.t() | nil,
          byte_size: non_neg_integer(),
          entry_count: non_neg_integer(),
          ts_min: integer(),
          ts_max: integer(),
          format: :raw | :zstd
        }

  @spec write_block([map()], String.t() | :memory, :raw | :zstd, keyword()) ::
          {:ok, block_meta()} | {:error, term()}
  def write_block(entries, target, format \\ :raw, opts \\ [])

  def write_block(entries, :memory, format, opts) do
    binary = :erlang.term_to_binary(entries)

    data =
      case format do
        :raw ->
          binary

        :zstd ->
          :ezstd.compress(
            binary,
            Keyword.get(opts, :level, TimelessLogs.Config.compression_level())
          )
      end

    block_id = System.unique_integer([:positive, :monotonic])
    {ts_min, ts_max, count} = ts_min_max_count(entries)

    meta = %{
      block_id: block_id,
      file_path: nil,
      byte_size: byte_size(data),
      entry_count: count,
      ts_min: ts_min,
      ts_max: ts_max,
      data: data,
      format: format
    }

    {:ok, meta}
  end

  def write_block(entries, data_dir, format, opts) do
    binary = :erlang.term_to_binary(entries)

    {data, ext} =
      case format do
        :raw ->
          {binary, "raw"}

        :zstd ->
          {:ezstd.compress(
             binary,
             Keyword.get(opts, :level, TimelessLogs.Config.compression_level())
           ), "zst"}
      end

    block_id = System.unique_integer([:positive, :monotonic])
    filename = "#{String.pad_leading(Integer.to_string(block_id), 12, "0")}.#{ext}"
    file_path = Path.join([data_dir, "blocks", filename])

    case File.write(file_path, data) do
      :ok ->
        {ts_min, ts_max, count} = ts_min_max_count(entries)

        meta = %{
          block_id: block_id,
          file_path: file_path,
          byte_size: byte_size(data),
          entry_count: count,
          ts_min: ts_min,
          ts_max: ts_max,
          format: format
        }

        {:ok, meta}

      {:error, reason} ->
        {:error, reason}
    end
  end

  # Single pass: extract min timestamp, max timestamp, and count
  defp ts_min_max_count([first | rest]) do
    ts = first.timestamp

    Enum.reduce(rest, {ts, ts, 1}, fn entry, {mn, mx, c} ->
      t = entry.timestamp
      {min(t, mn), max(t, mx), c + 1}
    end)
  end

  @spec decompress_block(binary(), :raw | :zstd) :: {:ok, [map()]} | {:error, :corrupt_block}
  def decompress_block(data, format \\ :zstd)

  def decompress_block(data, :raw) do
    try do
      {:ok, :erlang.binary_to_term(data)}
    rescue
      e ->
        Logger.warning("TimelessLogs: corrupt raw block data: #{inspect(e)}")
        {:error, :corrupt_block}
    end
  end

  def decompress_block(compressed, :zstd) do
    try do
      binary = :ezstd.decompress(compressed)
      {:ok, :erlang.binary_to_term(binary)}
    rescue
      e ->
        Logger.warning("TimelessLogs: corrupt block data: #{inspect(e)}")
        {:error, :corrupt_block}
    end
  end

  @spec read_block(String.t(), :raw | :zstd) :: {:ok, [map()]} | {:error, term()}
  def read_block(file_path, format \\ :zstd) do
    case File.read(file_path) do
      {:ok, data} ->
        decompress_block(data, format)

      {:error, reason} ->
        Logger.warning("TimelessLogs: cannot read block #{file_path}: #{inspect(reason)}")
        {:error, reason}
    end
  end
end
