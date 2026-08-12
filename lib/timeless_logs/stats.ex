defmodule TimelessLogs.Stats do
  @moduledoc """
  Aggregate statistics about stored log data.
  """

  defstruct storage_mode: nil,
            total_blocks: 0,
            total_entries: 0,
            total_bytes: 0,
            oldest_timestamp: nil,
            newest_timestamp: nil,
            disk_size: 0,
            index_size: 0,
            raw_blocks: 0,
            raw_bytes: 0,
            raw_entries: 0,
            # Authoritative compressed totals, format-agnostic. The libSQL
            # engine writes adaptive columnar blocks (zstd only appears as an
            # internal per-column strategy), so per-format zstd_*/openzl_*
            # fields below stay 0 there — they are the LEGACY block engine's
            # breakdown and remain populated for it.
            compressed_blocks: 0,
            compressed_bytes: 0,
            zstd_blocks: 0,
            zstd_bytes: 0,
            zstd_entries: 0,
            openzl_blocks: 0,
            openzl_bytes: 0,
            openzl_entries: 0,
            compression_raw_bytes_in: 0,
            compression_compressed_bytes_out: 0,
            compaction_count: 0

  @type t :: %__MODULE__{
          storage_mode: :libsql | :disk | :memory | nil,
          total_blocks: non_neg_integer(),
          total_entries: non_neg_integer(),
          total_bytes: non_neg_integer(),
          oldest_timestamp: integer() | nil,
          newest_timestamp: integer() | nil,
          disk_size: non_neg_integer(),
          index_size: non_neg_integer(),
          raw_blocks: non_neg_integer(),
          raw_bytes: non_neg_integer(),
          raw_entries: non_neg_integer(),
          compressed_blocks: non_neg_integer(),
          compressed_bytes: non_neg_integer(),
          zstd_blocks: non_neg_integer(),
          zstd_bytes: non_neg_integer(),
          zstd_entries: non_neg_integer(),
          openzl_blocks: non_neg_integer(),
          openzl_bytes: non_neg_integer(),
          openzl_entries: non_neg_integer(),
          compression_raw_bytes_in: non_neg_integer(),
          compression_compressed_bytes_out: non_neg_integer(),
          compaction_count: non_neg_integer()
        }
end
