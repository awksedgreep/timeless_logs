defmodule TimelessLogs.Handler do
  @moduledoc false

  @handler_id :timeless_logs

  @spec handler_id() :: atom()
  def handler_id, do: @handler_id

  @spec adding_handler(map()) :: {:ok, map()}
  def adding_handler(config) do
    {:ok, config}
  end

  @spec removing_handler(map()) :: :ok
  def removing_handler(_config) do
    :ok
  end

  @spec changing_config(:set | :update, map(), map()) :: {:ok, map()}
  def changing_config(:set, _old, new), do: {:ok, new}
  def changing_config(:update, old, new), do: {:ok, Map.merge(old, new)}

  @spec log(map(), map()) :: :ok
  def log(%{level: level, msg: msg, meta: meta}, _config) do
    timestamp =
      case meta do
        %{time: time} -> div(time, 1_000_000)
        _ -> System.system_time(:second)
      end

    message = format_msg(msg)

    entry = %{
      timestamp: timestamp,
      level: level,
      message: message,
      metadata: extract_metadata(meta)
    }

    TimelessLogs.Buffer.log(entry)
  end

  defp format_msg({:string, msg}), do: IO.chardata_to_string(msg)
  defp format_msg({:report, report}), do: inspect(report)
  defp format_msg({format, args}), do: IO.chardata_to_string(:io_lib.format(format, args))

  defp extract_metadata(meta) do
    meta
    |> Map.drop([:time, :gl, :pid, :mfa, :file, :line, :domain, :report_cb])
    |> Map.new(fn {k, v} -> {to_string(k), to_string_safe(v)} end)
  end

  defp to_string_safe(v) when is_binary(v), do: v
  defp to_string_safe(v) when is_atom(v), do: Atom.to_string(v)
  defp to_string_safe(v) when is_number(v), do: to_string(v)
  defp to_string_safe(v), do: inspect(v)
end
