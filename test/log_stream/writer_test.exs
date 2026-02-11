defmodule LogStream.WriterTest do
  use ExUnit.Case, async: true

  @data_dir "test/tmp/writer_#{System.unique_integer([:positive])}"

  setup do
    blocks_dir = Path.join(@data_dir, "blocks")
    File.mkdir_p!(blocks_dir)
    on_exit(fn -> File.rm_rf!(@data_dir) end)
    :ok
  end

  describe "write_block/3" do
    test "writes a raw block file" do
      entries = [
        %{timestamp: 1000, level: :info, message: "hello", metadata: %{}},
        %{timestamp: 1001, level: :error, message: "boom", metadata: %{"key" => "val"}}
      ]

      assert {:ok, meta} = LogStream.Writer.write_block(entries, @data_dir, :raw)
      assert meta.entry_count == 2
      assert meta.ts_min == 1000
      assert meta.ts_max == 1001
      assert meta.byte_size > 0
      assert meta.format == :raw
      assert File.exists?(meta.file_path)
      assert String.ends_with?(meta.file_path, ".raw")
    end

    test "writes a zstd compressed block file" do
      entries = [
        %{timestamp: 1000, level: :info, message: "hello", metadata: %{}},
        %{timestamp: 1001, level: :error, message: "boom", metadata: %{"key" => "val"}}
      ]

      assert {:ok, meta} = LogStream.Writer.write_block(entries, @data_dir, :zstd)
      assert meta.format == :zstd
      assert File.exists?(meta.file_path)
      assert String.ends_with?(meta.file_path, ".zst")
    end

    test "compressed size is smaller than raw" do
      entries =
        for i <- 1..500 do
          %{
            timestamp: 1000 + i,
            level: :info,
            message: "repeated log message #{i}",
            metadata: %{"i" => "#{i}"}
          }
        end

      {:ok, meta} = LogStream.Writer.write_block(entries, @data_dir, :zstd)
      raw_size = byte_size(:erlang.term_to_binary(entries))
      assert meta.byte_size < raw_size
    end

    test "returns error for invalid directory" do
      entries = [%{timestamp: 1, level: :info, message: "x", metadata: %{}}]
      assert {:error, _reason} = LogStream.Writer.write_block(entries, "/nonexistent/path")
    end
  end

  describe "read_block/2" do
    test "roundtrips raw data through write and read" do
      entries = [
        %{timestamp: 1000, level: :info, message: "hello", metadata: %{"a" => "b"}},
        %{timestamp: 1001, level: :error, message: "world", metadata: %{}}
      ]

      {:ok, meta} = LogStream.Writer.write_block(entries, @data_dir, :raw)
      assert {:ok, read_entries} = LogStream.Writer.read_block(meta.file_path, :raw)
      assert read_entries == entries
    end

    test "roundtrips zstd data through write and read" do
      entries = [
        %{timestamp: 1000, level: :info, message: "hello", metadata: %{"a" => "b"}},
        %{timestamp: 1001, level: :error, message: "world", metadata: %{}}
      ]

      {:ok, meta} = LogStream.Writer.write_block(entries, @data_dir, :zstd)
      assert {:ok, read_entries} = LogStream.Writer.read_block(meta.file_path, :zstd)
      assert read_entries == entries
    end

    test "returns error for missing file" do
      assert {:error, :enoent} = LogStream.Writer.read_block("/nonexistent/file.zst")
    end

    test "returns error for corrupt zstd file" do
      corrupt_path = Path.join([@data_dir, "blocks", "corrupt.zst"])
      File.write!(corrupt_path, "not valid zstd data")

      assert {:error, :corrupt_block} = LogStream.Writer.read_block(corrupt_path, :zstd)
    end

    test "returns error for truncated zstd file" do
      entries = [%{timestamp: 1, level: :info, message: "test", metadata: %{}}]
      {:ok, meta} = LogStream.Writer.write_block(entries, @data_dir, :zstd)

      # Truncate the file
      data = File.read!(meta.file_path)
      File.write!(meta.file_path, binary_part(data, 0, div(byte_size(data), 2)))

      assert {:error, :corrupt_block} = LogStream.Writer.read_block(meta.file_path, :zstd)
    end
  end
end
