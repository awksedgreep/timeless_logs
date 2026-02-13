defmodule TimelessLogs.Result do
  @moduledoc """
  Query result with pagination metadata.
  """

  defstruct entries: [], total: 0, limit: 100, offset: 0

  @type t :: %__MODULE__{
          entries: [TimelessLogs.Entry.t()],
          total: non_neg_integer(),
          limit: pos_integer(),
          offset: non_neg_integer()
        }
end
