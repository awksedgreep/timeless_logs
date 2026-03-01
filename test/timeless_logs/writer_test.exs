defmodule TimelessLogs.WriterTest do
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

      assert {:ok, meta} = TimelessLogs.Writer.write_block(entries, @data_dir, :raw)
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

      assert {:ok, meta} = TimelessLogs.Writer.write_block(entries, @data_dir, :zstd)
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

      {:ok, meta} = TimelessLogs.Writer.write_block(entries, @data_dir, :zstd)
      raw_size = byte_size(:erlang.term_to_binary(entries))
      assert meta.byte_size < raw_size
    end

    test "returns error for invalid directory" do
      entries = [%{timestamp: 1, level: :info, message: "x", metadata: %{}}]
      assert {:error, _reason} = TimelessLogs.Writer.write_block(entries, "/nonexistent/path")
    end
  end

  describe "read_block/2" do
    test "roundtrips raw data through write and read" do
      entries = [
        %{timestamp: 1000, level: :info, message: "hello", metadata: %{"a" => "b"}},
        %{timestamp: 1001, level: :error, message: "world", metadata: %{}}
      ]

      {:ok, meta} = TimelessLogs.Writer.write_block(entries, @data_dir, :raw)
      assert {:ok, read_entries} = TimelessLogs.Writer.read_block(meta.file_path, :raw)
      assert read_entries == entries
    end

    test "roundtrips zstd data through write and read" do
      entries = [
        %{timestamp: 1000, level: :info, message: "hello", metadata: %{"a" => "b"}},
        %{timestamp: 1001, level: :error, message: "world", metadata: %{}}
      ]

      {:ok, meta} = TimelessLogs.Writer.write_block(entries, @data_dir, :zstd)
      assert {:ok, read_entries} = TimelessLogs.Writer.read_block(meta.file_path, :zstd)
      assert read_entries == entries
    end

    test "returns error for missing file" do
      assert {:error, :enoent} = TimelessLogs.Writer.read_block("/nonexistent/file.zst")
    end

    test "returns error for corrupt zstd file" do
      corrupt_path = Path.join([@data_dir, "blocks", "corrupt.zst"])
      File.write!(corrupt_path, "not valid zstd data")

      assert {:error, :corrupt_block} = TimelessLogs.Writer.read_block(corrupt_path, :zstd)
    end

    test "returns error for truncated zstd file" do
      entries = [%{timestamp: 1, level: :info, message: "test", metadata: %{}}]
      {:ok, meta} = TimelessLogs.Writer.write_block(entries, @data_dir, :zstd)

      # Truncate the file
      data = File.read!(meta.file_path)
      File.write!(meta.file_path, binary_part(data, 0, div(byte_size(data), 2)))

      assert {:error, :corrupt_block} = TimelessLogs.Writer.read_block(meta.file_path, :zstd)
    end
  end

  describe "openzl format" do
    test "writes openzl block to disk with .ozl extension" do
      entries = [
        %{timestamp: 1000, level: :info, message: "hello", metadata: %{}},
        %{timestamp: 1001, level: :error, message: "boom", metadata: %{"key" => "val"}}
      ]

      assert {:ok, meta} = TimelessLogs.Writer.write_block(entries, @data_dir, :openzl)
      assert meta.format == :openzl
      assert meta.entry_count == 2
      assert meta.ts_min == 1000
      assert meta.ts_max == 1001
      assert meta.byte_size > 0
      assert File.exists?(meta.file_path)
      assert String.ends_with?(meta.file_path, ".ozl")
    end

    test "writes openzl block to memory" do
      entries = [
        %{timestamp: 1000, level: :info, message: "hello", metadata: %{}},
        %{timestamp: 1001, level: :error, message: "boom", metadata: %{"key" => "val"}}
      ]

      assert {:ok, meta} = TimelessLogs.Writer.write_block(entries, :memory, :openzl)
      assert meta.format == :openzl
      assert meta.file_path == nil
      assert is_binary(meta.data)
      assert meta.byte_size > 0
    end

    test "openzl smaller than raw for large blocks" do
      entries =
        for i <- 1..500 do
          %{
            timestamp: 1000 + i,
            level: :info,
            message: "repeated log message #{i}",
            metadata: %{"i" => "#{i}"}
          }
        end

      {:ok, meta} = TimelessLogs.Writer.write_block(entries, @data_dir, :openzl)
      raw_size = byte_size(:erlang.term_to_binary(entries))
      assert meta.byte_size < raw_size
    end

    test "roundtrips via disk read_block" do
      entries = [
        %{timestamp: 1000, level: :info, message: "hello", metadata: %{"a" => "b"}},
        %{timestamp: 1001, level: :error, message: "world", metadata: %{}}
      ]

      {:ok, meta} = TimelessLogs.Writer.write_block(entries, @data_dir, :openzl)
      assert {:ok, read_entries} = TimelessLogs.Writer.read_block(meta.file_path, :openzl)
      assert read_entries == entries
    end

    test "roundtrips via memory decompress_block" do
      entries = [
        %{timestamp: 1000, level: :info, message: "hello", metadata: %{"a" => "b"}},
        %{timestamp: 1001, level: :error, message: "world", metadata: %{}}
      ]

      {:ok, meta} = TimelessLogs.Writer.write_block(entries, :memory, :openzl)
      assert {:ok, read_entries} = TimelessLogs.Writer.decompress_block(meta.data, :openzl)
      assert read_entries == entries
    end

    test "corrupt openzl returns error" do
      corrupt_path = Path.join([@data_dir, "blocks", "corrupt.ozl"])
      File.write!(corrupt_path, "not valid openzl data")

      assert {:error, :corrupt_block} = TimelessLogs.Writer.read_block(corrupt_path, :openzl)
    end

    test "truncated openzl returns error" do
      entries = [
        %{timestamp: 1000, level: :info, message: "test", metadata: %{}},
        %{timestamp: 1001, level: :error, message: "boom", metadata: %{"k" => "v"}}
      ]

      {:ok, meta} = TimelessLogs.Writer.write_block(entries, @data_dir, :openzl)

      data = File.read!(meta.file_path)
      File.write!(meta.file_path, binary_part(data, 0, div(byte_size(data), 2)))

      assert {:error, :corrupt_block} = TimelessLogs.Writer.read_block(meta.file_path, :openzl)
    end

    test "preserves all 4 log levels" do
      entries = [
        %{timestamp: 1000, level: :debug, message: "debug msg", metadata: %{}},
        %{timestamp: 1001, level: :info, message: "info msg", metadata: %{}},
        %{timestamp: 1002, level: :warning, message: "warning msg", metadata: %{}},
        %{timestamp: 1003, level: :error, message: "error msg", metadata: %{}}
      ]

      {:ok, meta} = TimelessLogs.Writer.write_block(entries, :memory, :openzl)
      {:ok, read_entries} = TimelessLogs.Writer.decompress_block(meta.data, :openzl)

      assert Enum.map(read_entries, & &1.level) == [:debug, :info, :warning, :error]

      assert Enum.map(read_entries, & &1.message) == [
               "debug msg",
               "info msg",
               "warning msg",
               "error msg"
             ]
    end

    test "handles all OTP/syslog severity levels without crashing" do
      for level <- [
            :debug,
            :info,
            :notice,
            :warning,
            :warn,
            :error,
            :critical,
            :alert,
            :emergency
          ] do
        entries = [
          %{timestamp: 1000, level: level, message: "#{level} msg", metadata: %{}}
        ]

        {:ok, meta} = TimelessLogs.Writer.write_block(entries, :memory, :openzl)
        {:ok, [read_entry]} = TimelessLogs.Writer.decompress_block(meta.data, :openzl)

        assert read_entry.message == "#{level} msg",
               "Failed to round-trip log with level #{level}"
      end
    end

    test "handles unknown severity levels without crashing" do
      entries = [
        %{timestamp: 1000, level: :fatal, message: "unknown level", metadata: %{}},
        %{timestamp: 1001, level: :trace, message: "another unknown", metadata: %{}}
      ]

      {:ok, meta} = TimelessLogs.Writer.write_block(entries, :memory, :openzl)
      {:ok, read_entries} = TimelessLogs.Writer.decompress_block(meta.data, :openzl)

      assert length(read_entries) == 2
      assert Enum.map(read_entries, & &1.message) == ["unknown level", "another unknown"]
    end

    test "preserves metadata maps" do
      entries = [
        %{
          timestamp: 1000,
          level: :info,
          message: "a",
          metadata: %{"key" => "val", "num" => "42"}
        },
        %{timestamp: 1001, level: :info, message: "b", metadata: %{}},
        %{
          timestamp: 1002,
          level: :info,
          message: "c",
          metadata: %{"nested" => "data", "x" => "y"}
        }
      ]

      {:ok, meta} = TimelessLogs.Writer.write_block(entries, :memory, :openzl)
      {:ok, read_entries} = TimelessLogs.Writer.decompress_block(meta.data, :openzl)

      assert Enum.map(read_entries, & &1.metadata) == [
               %{"key" => "val", "num" => "42"},
               %{},
               %{"nested" => "data", "x" => "y"}
             ]
    end

    test "large block (500 entries) roundtrip" do
      entries =
        for i <- 1..500 do
          %{
            timestamp: 1000 + i,
            level: Enum.at([:debug, :info, :warning, :error], rem(i, 4)),
            message: "log message number #{i} with some content",
            metadata: %{"index" => "#{i}", "module" => "TestModule"}
          }
        end

      {:ok, meta} = TimelessLogs.Writer.write_block(entries, :memory, :openzl)
      {:ok, read_entries} = TimelessLogs.Writer.decompress_block(meta.data, :openzl)
      assert read_entries == entries
    end
  end
end
