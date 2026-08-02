defmodule TimelessLogs.ExternalOwnerTest do
  use ExUnit.Case, async: true

  test "external ownership starts no storage, buffer, logger, or Rocket child" do
    assert [] = TimelessLogs.Application.configured_children(:external)
  end

  test "unknown ownership fails explicitly" do
    assert_raise ArgumentError, ~r/expected :embedded or :external/, fn ->
      TimelessLogs.Application.configured_children(:automatic)
    end
  end
end
