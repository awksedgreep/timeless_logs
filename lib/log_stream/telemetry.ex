defmodule LogStream.Telemetry do
  @moduledoc """
  Telemetry events emitted by LogStream.

  ## Events

    * `[:log_stream, :flush, :stop]` - A buffer flush completed.
      * Measurements: `%{duration: native_time, entry_count: integer, byte_size: integer}`
      * Metadata: `%{block_id: integer}`

    * `[:log_stream, :query, :stop]` - A query completed.
      * Measurements: `%{duration: native_time, total: integer, blocks_read: integer}`
      * Metadata: `%{filters: keyword}`

    * `[:log_stream, :retention, :stop]` - A retention cleanup completed.
      * Measurements: `%{duration: native_time, blocks_deleted: integer}`
      * Metadata: `%{}`

    * `[:log_stream, :block, :error]` - A block read failed (corrupt or missing).
      * Measurements: `%{}`
      * Metadata: `%{file_path: string, reason: atom}`
  """

  @doc false
  @spec span([atom()], map(), (-> {map(), map()})) :: {map(), map()}
  def span(event_prefix, meta, fun) do
    start_time = System.monotonic_time()
    result = fun.()
    duration = System.monotonic_time() - start_time
    {measurements, extra_meta} = result

    :telemetry.execute(
      event_prefix ++ [:stop],
      Map.put(measurements, :duration, duration),
      Map.merge(meta, extra_meta)
    )

    result
  end

  @doc false
  @spec event([atom()], map(), map()) :: :ok
  def event(event_name, measurements, metadata) do
    :telemetry.execute(event_name, measurements, metadata)
  end
end
