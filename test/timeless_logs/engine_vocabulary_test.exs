defmodule TimelessLogs.EngineVocabularyTest do
  @moduledoc """
  The engine option refuses values it does not understand.

  The three Timeless packages spell their previous-generation engine
  differently — `:elixir` here, `:rust` in timeless_metrics — and they used to
  resolve an unrecognised value in opposite directions: this package fell
  through to its legacy engine, while timeless_metrics fell through to libSQL. The
  same typo therefore downgraded one signal and upgraded another, in silence
  and with no way to notice.

  Falling back to the legacy engine was the quieter half of that: the store
  keeps working, so nothing looks wrong, and the operator simply never gets the
  engine they configured.
  """

  use ExUnit.Case, async: false

  alias TimelessLogs.Config

  setup do
    previous = Application.get_env(:timeless_logs, :engine)

    on_exit(fn ->
      if previous,
        do: Application.put_env(:timeless_logs, :engine, previous),
        else: Application.delete_env(:timeless_logs, :engine)
    end)

    :ok
  end

  defp engine(value) do
    Application.put_env(:timeless_logs, :engine, value)
    Config.engine()
  end

  test "the sibling package's legacy engine name is refused, not silently downgraded" do
    message = assert_raise(ArgumentError, fn -> engine(:rust) end).message

    assert message =~ ":rust"
    # The error has to name the value that actually works here, since the
    # failure mode is an operator carrying vocabulary between packages.
    assert message =~ ":elixir"
  end

  test "an unrecognised engine is refused rather than resolved to the legacy engine" do
    message = assert_raise(ArgumentError, fn -> engine(:libsq1) end).message

    assert message =~ "invalid"
    assert message =~ ":libsq1"
  end

  test "the supported engines are returned unchanged" do
    assert engine(:libsql) == :libsql
    assert engine(:elixir) == :elixir
  end
end
