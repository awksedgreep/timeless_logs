defmodule TimelessLogs.Stats do
  @moduledoc """
  Aggregate statistics about stored log data.
  """

  defstruct total_blocks: 0,
            total_entries: 0,
            total_bytes: 0,
            oldest_timestamp: nil,
            newest_timestamp: nil,
            disk_size: 0,
            index_size: 0,
            raw_blocks: 0,
            raw_bytes: 0,
            raw_entries: 0,
            zstd_blocks: 0,
            zstd_bytes: 0,
            zstd_entries: 0,
            openzl_blocks: 0,
            openzl_bytes: 0,
            openzl_entries: 0

  @type t :: %__MODULE__{
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
          zstd_blocks: non_neg_integer(),
          zstd_bytes: non_neg_integer(),
          zstd_entries: non_neg_integer(),
          openzl_blocks: non_neg_integer(),
          openzl_bytes: non_neg_integer(),
          openzl_entries: non_neg_integer()
        }
end
