defmodule LogStream.Writer do
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

  @spec write_block([map()], String.t() | :memory, :raw | :zstd) ::
          {:ok, block_meta()} | {:error, term()}
  def write_block(entries, target, format \\ :raw)

  def write_block(entries, :memory, format) do
    binary = :erlang.term_to_binary(entries)

    data =
      case format do
        :raw -> binary
        :zstd -> :ezstd.compress(binary)
      end

    block_id = System.unique_integer([:positive, :monotonic])
    timestamps = Enum.map(entries, & &1.timestamp)

    meta = %{
      block_id: block_id,
      file_path: nil,
      byte_size: byte_size(data),
      entry_count: length(entries),
      ts_min: Enum.min(timestamps),
      ts_max: Enum.max(timestamps),
      data: data,
      format: format
    }

    {:ok, meta}
  end

  def write_block(entries, data_dir, format) do
    binary = :erlang.term_to_binary(entries)

    {data, ext} =
      case format do
        :raw -> {binary, "raw"}
        :zstd -> {:ezstd.compress(binary), "zst"}
      end

    block_id = System.unique_integer([:positive, :monotonic])
    filename = "#{String.pad_leading(Integer.to_string(block_id), 12, "0")}.#{ext}"
    file_path = Path.join([data_dir, "blocks", filename])

    case File.write(file_path, data) do
      :ok ->
        timestamps = Enum.map(entries, & &1.timestamp)

        meta = %{
          block_id: block_id,
          file_path: file_path,
          byte_size: byte_size(data),
          entry_count: length(entries),
          ts_min: Enum.min(timestamps),
          ts_max: Enum.max(timestamps),
          format: format
        }

        {:ok, meta}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @spec decompress_block(binary(), :raw | :zstd) :: {:ok, [map()]} | {:error, :corrupt_block}
  def decompress_block(data, format \\ :zstd)

  def decompress_block(data, :raw) do
    try do
      {:ok, :erlang.binary_to_term(data)}
    rescue
      e ->
        Logger.warning("LogStream: corrupt raw block data: #{inspect(e)}")
        {:error, :corrupt_block}
    end
  end

  def decompress_block(compressed, :zstd) do
    try do
      binary = :ezstd.decompress(compressed)
      {:ok, :erlang.binary_to_term(binary)}
    rescue
      e ->
        Logger.warning("LogStream: corrupt block data: #{inspect(e)}")
        {:error, :corrupt_block}
    end
  end

  @spec read_block(String.t(), :raw | :zstd) :: {:ok, [map()]} | {:error, term()}
  def read_block(file_path, format \\ :zstd) do
    case File.read(file_path) do
      {:ok, data} ->
        decompress_block(data, format)

      {:error, reason} ->
        Logger.warning("LogStream: cannot read block #{file_path}: #{inspect(reason)}")
        {:error, reason}
    end
  end
end
