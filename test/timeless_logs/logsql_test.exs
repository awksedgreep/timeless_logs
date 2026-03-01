defmodule TimelessLogs.LogsQLTest do
  use ExUnit.Case, async: true

  alias TimelessLogs.LogsQL

  describe "parse/1" do
    test "wildcard returns empty filters" do
      assert {:query, []} = LogsQL.parse("*")
    end

    test "empty string returns empty filters" do
      assert {:query, []} = LogsQL.parse("")
    end

    test "parses level filter" do
      {:query, filters} = LogsQL.parse("level:error")
      assert Keyword.get(filters, :level) == :error
    end

    test "parses duration time filter" do
      {:query, filters} = LogsQL.parse("_time:1h")
      since = Keyword.get(filters, :since)
      assert is_integer(since)
      # Should be roughly 1 hour ago in microseconds
      now = System.os_time(:microsecond)
      assert_in_delta since, now - 3_600_000_000, 1_000_000
    end

    test "parses bracket time range" do
      {:query, filters} =
        LogsQL.parse("_time:[2024-01-01T00:00:00Z, 2024-01-02T00:00:00Z)")

      assert Keyword.get(filters, :since) == 1_704_067_200_000_000
      assert Keyword.get(filters, :until) == 1_704_153_600_000_000
    end

    test "parses >= time filter" do
      {:query, filters} = LogsQL.parse("_time:>=2024-06-01T00:00:00Z")
      assert Keyword.get(filters, :since) == 1_717_200_000_000_000
    end

    test "parses < time filter" do
      {:query, filters} = LogsQL.parse("_time:<2024-06-01T00:00:00Z")
      assert Keyword.get(filters, :until) == 1_717_200_000_000_000
    end

    test "parses metadata field filter" do
      {:query, filters} = LogsQL.parse("service:dhcp")
      meta = Keyword.get(filters, :metadata, %{})
      assert meta[:service] == "dhcp"
    end

    test "parses quoted metadata value" do
      {:query, filters} = LogsQL.parse(~s(service:"my app"))
      meta = Keyword.get(filters, :metadata, %{})
      assert meta[:service] == "my app"
    end

    test "parses quoted message search" do
      {:query, filters} = LogsQL.parse(~s("search term"))
      assert Keyword.get(filters, :message) == "search term"
    end

    test "parses sort pipe" do
      {:query, filters} = LogsQL.parse("* | sort by (_time) desc")
      assert Keyword.get(filters, :order) == :desc
    end

    test "parses sort asc pipe" do
      {:query, filters} = LogsQL.parse("* | sort by (_time) asc")
      assert Keyword.get(filters, :order) == :asc
    end

    test "parses limit pipe" do
      {:query, filters} = LogsQL.parse("* | limit 50")
      assert Keyword.get(filters, :limit) == 50
    end

    test "parses offset pipe" do
      {:query, filters} = LogsQL.parse("* | offset 10")
      assert Keyword.get(filters, :offset) == 10
    end

    test "parses stats count pipe" do
      assert {:stats_count, _} = LogsQL.parse("* | stats count() as total")
    end

    test "parses complex query with multiple filters and pipes" do
      {:query, filters} =
        LogsQL.parse(
          "_time:1h level:error service:dhcp \"search term\" | sort by (_time) desc | limit 50 | offset 0"
        )

      assert Keyword.get(filters, :level) == :error
      assert Keyword.get(filters, :message) == "search term"
      assert Keyword.get(filters, :limit) == 50
      assert Keyword.get(filters, :offset) == 0
      assert Keyword.get(filters, :order) == :desc
      meta = Keyword.get(filters, :metadata, %{})
      assert meta[:service] == "dhcp"
      assert is_integer(Keyword.get(filters, :since))
    end

    test "parses multiple duration units" do
      for {unit, expected_seconds} <- [
            {"15m", 900},
            {"6h", 21600},
            {"24h", 86400},
            {"7d", 604_800}
          ] do
        {:query, filters} = LogsQL.parse("_time:#{unit}")
        since = Keyword.get(filters, :since)
        now = System.os_time(:microsecond)
        assert_in_delta since, now - expected_seconds * 1_000_000, 1_000_000
      end
    end
  end
end
