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
  # Exact count of matching entries in the range, traversed in bounded
  # chunks so no query ever materializes the whole tail.
  @spec count_matching(keyword(), integer() | nil, integer() | nil) :: non_neg_integer()
  def count_matching(search_filters, since_us, until_us) do
    case meta() do
      nil ->
        0

      {table, _floor} ->
        search_filters = TimelessLogs.Filter.prepare(search_filters)

        safe(
          fn ->
            table
            |> :ets.select(range_spec(since_us, until_us), 5_000)
            |> count_chunks(search_filters, 0)
          end,
          0
        )
    end
  end

  defp count_chunks(:"$end_of_table", _filters, acc), do: acc

  defp count_chunks({chunk, cont}, filters, acc) do
    n = Enum.count(chunk, &TimelessLogs.Filter.matches?(&1, filters))
    count_chunks(:ets.select(cont), filters, acc + n)
  end

  @doc false
  # Bounded key walk: at most `max` matching entries from the range,
  # newest-first for :desc / oldest-first for :asc. O(walked keys), never
  # a full-table materialization — this is what queries use.
  @spec take(keyword(), integer() | nil, integer() | nil, :asc | :desc, non_neg_integer()) ::
          [map()]
  def take(_search_filters, _since_us, _until_us, _order, 0), do: []

  def take(search_filters, since_us, until_us, order, max) do
    case meta() do
      nil ->
        []

      {table, _floor} ->
        search_filters = TimelessLogs.Filter.prepare(search_filters)

        safe(
          fn ->
            selection =
              case order do
                :desc -> :ets.select_reverse(table, range_spec(since_us, until_us), 5_000)
                :asc -> :ets.select(table, range_spec(since_us, until_us), 5_000)
              end

            take_chunks(selection, search_filters, max, [])
          end,
          []
        )
    end
  end

  defp take_chunks(:"$end_of_table", _filters, _max, acc), do: Enum.reverse(acc)
  defp take_chunks(_selection, _filters, 0, acc), do: Enum.reverse(acc)

  defp take_chunks({chunk, continuation}, filters, max, acc) do
    matches = chunk |> Enum.filter(&TimelessLogs.Filter.matches?(&1, filters)) |> Enum.take(max)
    next_acc = Enum.reverse(matches, acc)
    remaining = max - length(matches)

    if remaining == 0 do
      Enum.reverse(next_acc)
    else
      take_chunks(:ets.select(continuation), filters, remaining, next_acc)
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

  # Bulk delete in one locked pass instead of key-at-a-time (at high
  # ingest the sweep would otherwise fight writers for the table lock
  # hundreds of thousands of times per second).
  defp prune_older_than(table, cutoff) do
    :ets.select_delete(table, [{{{:"$1", :_}, :_}, [{:<, :"$1", cutoff}], [true]}])
    :ok
  end

  defp prune_over_cap(table, cap) do
    over = :ets.info(table, :size) - cap

    if over > 0 do
      case key_at_offset(table, over) do
        :"$end_of_table" ->
          :ok

        {ts, _uniq} ->
          # Bulk-delete everything strictly older than the nth-oldest
          # key, then finish the same-µs cluster individually.
          prune_older_than(table, ts)
          delete_oldest(table, :ets.info(table, :size) - cap)
      end
    else
      :ok
    end
  end

  defp delete_oldest(_table, n) when n <= 0, do: :ok

  defp delete_oldest(table, n) do
    case :ets.first(table) do
      :"$end_of_table" ->
        :ok

      key ->
        :ets.delete(table, key)
        delete_oldest(table, n - 1)
    end
  end

  defp key_at_offset(table, offset) do
    limit = min(offset + 1, 5_000)
    selection = :ets.select(table, [{{:"$1", :_}, [], [:"$1"]}], limit)
    key_at_offset_chunks(selection, offset)
  end

  defp key_at_offset_chunks(:"$end_of_table", _offset), do: :"$end_of_table"

  defp key_at_offset_chunks({keys, continuation}, offset) do
    if offset < length(keys) do
      Enum.at(keys, offset)
    else
      key_at_offset_chunks(:ets.select(continuation), offset - length(keys))
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
