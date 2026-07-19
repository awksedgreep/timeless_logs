defmodule TimelessLogs.HotTail do
  @moduledoc false

  # In-memory tail of recently accepted entries, queryable before (and
  # after) they reach disk. An ETS ordered_set keyed {timestamp_µs, uniq}
  # is fed at shard accept time and pruned by age and size.
  #
  # Queries are partitioned by a single boundary timestamp: the tail
  # serves ts >= boundary, disk serves ts < boundary. The union is exact
  # with no deduplication because the two ranges never overlap:
  #
  #   boundary = max(now - lag, floor, oldest tail key)
  #
  # - `now - lag` guarantees anything older has had time to be written
  #   AND indexed under normal operation (lag >> flush + index interval)
  # - `floor` (server start) keeps a freshly restarted server — whose
  #   tail is empty but whose disk holds recent entries — serving those
  #   from disk instead of a hole
  # - `oldest tail key` rises when cap-pruning shrinks the window below
  #   the lag, again pushing already-indexed entries back to disk
  # - an empty tail means boundary = now: disk serves everything
  #
  # Entries whose client-supplied timestamps are far in the past land in
  # the tail but below the boundary; they stay invisible until indexed —
  # exactly the pre-hot-tail behavior.

  use GenServer

  @table __MODULE__
  @meta_key {__MODULE__, :meta}
  @sweep_interval 1_000

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @spec insert_many([map()]) :: :ok
  def insert_many(entries) do
    case meta() do
      nil ->
        :ok

      {table, _floor} ->
        rows =
          Enum.map(entries, fn entry ->
            ts =
              case entry do
                %{timestamp: t} -> TimelessLogs.Timestamp.to_microseconds(t)
                _ -> System.os_time(:microsecond)
              end

            {{ts, :erlang.unique_integer([:monotonic])}, entry}
          end)

        safe(fn -> :ets.insert(table, rows) end, :ok)
        :ok
    end
  end

  @spec boundary() :: integer()
  def boundary do
    now = System.os_time(:microsecond)

    case meta() do
      nil ->
        now

      {table, floor} ->
        base = max(now - TimelessLogs.Config.hot_tail_lag_ms() * 1_000, floor)

        safe(
          fn ->
            case :ets.first(table) do
              :"$end_of_table" -> now
              {ts, _uniq} when ts > base -> min(ts, now)
              {_ts, _uniq} -> base
            end
          end,
          now
        )
    end
  end

  @doc false
  # All tail entries with key timestamp in [since_us, until_us] (nil =
  # unbounded) matching the given per-entry filters. Returns raw entry
  # maps, unsorted.
  @spec select(keyword(), integer() | nil, integer() | nil) :: [map()]
  def select(search_filters, since_us, until_us) do
    case meta() do
      nil ->
        []

      {table, _floor} ->
        safe(
          fn ->
            table
            |> :ets.select(range_spec(since_us, until_us))
            |> TimelessLogs.Filter.filter(search_filters)
          end,
          []
        )
    end
  end

  @doc false
  # Retention support: deleted must mean gone, even from the tail.
  @spec prune_before(integer()) :: :ok
  def prune_before(cutoff_us) do
    case meta() do
      nil -> :ok
      {table, _floor} -> safe(fn -> prune_older_than(table, cutoff_us) end, :ok)
    end
  end

  @impl true
  def init(_opts) do
    table =
      :ets.new(@table, [
        :ordered_set,
        :public,
        :named_table,
        {:write_concurrency, true},
        {:read_concurrency, true}
      ])

    :persistent_term.put(@meta_key, {table, System.os_time(:microsecond)})
    schedule_sweep()
    {:ok, %{table: table}}
  end

  @impl true
  def terminate(_reason, _state) do
    :persistent_term.erase(@meta_key)
    :ok
  end

  @impl true
  def handle_info(:sweep, state) do
    prune(state.table)
    schedule_sweep()
    {:noreply, state}
  end

  defp prune(table) do
    window_us = TimelessLogs.Config.hot_tail_window_seconds() * 1_000_000
    cutoff = System.os_time(:microsecond) - window_us
    prune_older_than(table, cutoff)
    prune_over_cap(table, TimelessLogs.Config.hot_tail_max_entries())
  end

  defp prune_older_than(table, cutoff) do
    case :ets.first(table) do
      {ts, _uniq} = key when ts < cutoff ->
        :ets.delete(table, key)
        prune_older_than(table, cutoff)

      _ ->
        :ok
    end
  end

  defp prune_over_cap(table, cap) do
    if :ets.info(table, :size) > cap do
      case :ets.first(table) do
        :"$end_of_table" ->
          :ok

        key ->
          :ets.delete(table, key)
          prune_over_cap(table, cap)
      end
    else
      :ok
    end
  end

  defp range_spec(since_us, until_us) do
    guards =
      Enum.reject(
        [
          since_us && {:>=, :"$1", since_us},
          until_us && {:"=<", :"$1", until_us}
        ],
        &is_nil/1
      )

    [{{{:"$1", :"$2"}, :"$3"}, guards, [:"$3"]}]
  end

  defp meta do
    :persistent_term.get(@meta_key, nil)
  end

  # The table lives and dies with this GenServer; racing callers during a
  # restart get a harmless default instead of an ArgumentError.
  defp safe(fun, default) do
    fun.()
  rescue
    ArgumentError -> default
  end

  defp schedule_sweep do
    Process.send_after(self(), :sweep, @sweep_interval)
  end
end
