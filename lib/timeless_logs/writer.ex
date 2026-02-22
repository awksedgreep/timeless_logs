defmodule TimelessLogs.Writer do
  @moduledoc false

  require Logger

  @level_to_int %{debug: 0, info: 1, warning: 2, error: 3}
  @int_to_level %{0 => :debug, 1 => :info, 2 => :warning, 3 => :error}

  @type block_meta :: %{
          block_id: integer(),
          file_path: String.t() | nil,
          byte_size: non_neg_integer(),
          entry_count: non_neg_integer(),
          ts_min: integer(),
          ts_max: integer(),
          format: :raw | :zstd | :openzl
        }

  @spec write_block([map()], String.t() | :memory, :raw | :zstd | :openzl, keyword()) ::
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
            Keyword.get(opts, :level, TimelessLogs.Config.zstd_compression_level())
          )

        :openzl ->
          {:ok, compressed} = columnar_serialize(entries, opts)
          compressed
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
             Keyword.get(opts, :level, TimelessLogs.Config.zstd_compression_level())
           ), "zst"}

        :openzl ->
          {:ok, compressed} = columnar_serialize(entries, opts)
          {compressed, "ozl"}
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

  @spec decompress_block(binary(), :raw | :zstd | :openzl) ::
          {:ok, [map()]} | {:error, :corrupt_block}
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

  def decompress_block(compressed, :openzl) do
    try do
      {:ok, dctx} = ExOpenzl.create_decompression_context()
      {:ok, outputs} = ExOpenzl.decompress_multi_typed(dctx, compressed)
      {:ok, columnar_deserialize(outputs)}
    rescue
      e ->
        Logger.warning("TimelessLogs: corrupt openzl block data: #{inspect(e)}")
        {:error, :corrupt_block}
    end
  end

  @spec read_block(String.t(), :raw | :zstd | :openzl) ::
          {:ok, [map()]} | {:error, term()}
  def read_block(file_path, format \\ :zstd) do
    case File.read(file_path) do
      {:ok, data} ->
        decompress_block(data, format)

      {:error, reason} ->
        Logger.warning("TimelessLogs: cannot read block #{file_path}: #{inspect(reason)}")
        {:error, reason}
    end
  end

  @doc false
  def columnar_serialize(entries, opts \\ []) do
    {ts_bin, levels_bin, msg_concat, msg_lengths_bin} =
      Enum.reduce(entries, {<<>>, <<>>, <<>>, <<>>}, fn entry,
                                                        {ts_acc, lv_acc, msg_acc, msg_len_acc} ->
        ts = entry.timestamp
        level_int = Map.fetch!(@level_to_int, entry.level)
        msg = entry.message

        {
          <<ts_acc::binary, ts::little-unsigned-64>>,
          <<lv_acc::binary, level_int::unsigned-8>>,
          <<msg_acc::binary, msg::binary>>,
          <<msg_len_acc::binary, byte_size(msg)::little-unsigned-32>>
        }
      end)

    # Batch all metadata into a single term_to_binary call for atom sharing
    meta_bin = :erlang.term_to_binary(Enum.map(entries, & &1.metadata))
    meta_lengths_bin = <<byte_size(meta_bin)::little-unsigned-32>>

    {:ok, cctx} = ExOpenzl.create_compression_context()
    level = Keyword.get(opts, :level, TimelessLogs.Config.openzl_compression_level())
    :ok = ExOpenzl.set_compression_level(cctx, level)

    inputs = [
      {:numeric, ts_bin, 8},
      {:numeric, levels_bin, 1},
      {:string, msg_concat, msg_lengths_bin},
      {:string, meta_bin, meta_lengths_bin}
    ]

    ExOpenzl.compress_multi_typed(cctx, inputs)
  end

  @doc false
  def columnar_deserialize([ts_info, levels_info, msgs_info, meta_info]) do
    timestamps = unpack_u64_le(ts_info.data)
    levels = unpack_u8(levels_info.data)
    msg_lengths = unpack_native_u32(msgs_info.string_lengths)
    meta_lengths = unpack_native_u32(meta_info.string_lengths)
    messages = split_by_lengths(msgs_info.data, msg_lengths)

    # Detect batched vs legacy per-entry metadata format
    metadatas = deserialize_metadata(meta_info.data, meta_lengths, length(timestamps))

    Enum.zip_with(
      [timestamps, levels, messages, metadatas],
      fn [ts, level_int, msg, metadata] ->
        %{
          timestamp: ts,
          level: Map.fetch!(@int_to_level, level_int),
          message: msg,
          metadata: metadata
        }
      end
    )
  end

  # Batched format: single term_to_binary blob containing list of all metadata maps
  defp deserialize_metadata(data, [_single_len], entry_count) when entry_count > 1 do
    :erlang.binary_to_term(data)
  end

  # Single entry: could be either format
  defp deserialize_metadata(data, [_single_len], 1) do
    result = :erlang.binary_to_term(data)
    if is_list(result), do: result, else: [result]
  end

  # Legacy per-entry format: N separate term_to_binary blobs
  defp deserialize_metadata(data, lengths, _entry_count) do
    split_by_lengths(data, lengths) |> Enum.map(&:erlang.binary_to_term/1)
  end

  defp unpack_u64_le(<<>>), do: []

  defp unpack_u64_le(<<val::little-unsigned-64, rest::binary>>),
    do: [val | unpack_u64_le(rest)]

  defp unpack_u8(<<>>), do: []
  defp unpack_u8(<<val::unsigned-8, rest::binary>>), do: [val | unpack_u8(rest)]

  defp unpack_native_u32(<<>>), do: []

  defp unpack_native_u32(<<val::native-unsigned-32, rest::binary>>),
    do: [val | unpack_native_u32(rest)]

  defp split_by_lengths(_bin, []), do: []

  defp split_by_lengths(bin, [len | rest]) do
    <<chunk::binary-size(len), remaining::binary>> = bin
    [chunk | split_by_lengths(remaining, rest)]
  end
end
