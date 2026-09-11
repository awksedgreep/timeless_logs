defmodule TimelessLogs.FilterTest do
  use ExUnit.Case, async: true

  alias TimelessLogs.Filter

  test "prepare/1 normalizes query constants once and preserves matching semantics" do
    prepared = Filter.prepare(message: "TiMeOuT", metadata: %{"attempt" => 3})

    assert {:message_downcase, "timeout"} in prepared
    assert {:metadata_prepared, [{"attempt", 3, "3"}]} in prepared

    assert Filter.matches?(
             %{message: "DATABASE TIMEOUT", metadata: %{"attempt" => "3"}},
             prepared
           )

    refute Filter.matches?(%{message: "all good", metadata: %{"attempt" => "3"}}, prepared)
  end

  test "a missing metadata key never matches an empty filter value" do
    refute Filter.matches?(%{message: "entry", metadata: %{}}, metadata: %{"missing" => ""})
  end
end
