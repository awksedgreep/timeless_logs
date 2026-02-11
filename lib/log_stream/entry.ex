defmodule LogStream.Entry do
  @moduledoc """
  A single log entry.
  """

  defstruct [:timestamp, :level, :message, :metadata]

  @type t :: %__MODULE__{
          timestamp: integer(),
          level: atom(),
          message: String.t(),
          metadata: %{String.t() => String.t()}
        }

  @spec from_map(map()) :: t()
  def from_map(%{timestamp: ts, level: level, message: message, metadata: metadata}) do
    %__MODULE__{
      timestamp: ts,
      level: level,
      message: message,
      metadata: metadata
    }
  end
end
