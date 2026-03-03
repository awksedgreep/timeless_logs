defmodule TimelessLogs.Retention do
  @moduledoc false

  use GenServer

  require Logger

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @spec run_now() :: :noop | {:ok, non_neg_integer()}
  def run_now do
    GenServer.call(__MODULE__, :run_now, 60_000)
  end

  @impl true
  def init(_opts) do
    schedule(TimelessLogs.Config.retention_check_interval())
    {:ok, %{}}
  end

  @impl true
  def handle_call(:run_now, _from, state) do
    result = do_cleanup()
    {:reply, result, state}
  end

  @impl true
  def handle_info(:retention_check, state) do
    do_cleanup()
    schedule(TimelessLogs.Config.retention_check_interval())
    {:noreply, state}
  end

  defp schedule(interval) do
    Process.send_after(self(), :retention_check, interval)
  end

  defp do_cleanup do
    max_age = TimelessLogs.Config.retention_max_age()
    max_size = TimelessLogs.Config.retention_max_size()
    max_term_entries = TimelessLogs.Config.max_term_index_entries()

    if is_nil(max_age) and is_nil(max_size) and is_nil(max_term_entries) do
      :noop
    else
      start_time = System.monotonic_time()
      deleted_age = if max_age, do: cleanup_by_age(max_age), else: 0
      deleted_size = if max_size, do: cleanup_by_size(max_size), else: 0
      deleted_terms = if max_term_entries, do: cleanup_by_term_pressure(max_term_entries), else: 0
      total_deleted = deleted_age + deleted_size + deleted_terms
      duration = System.monotonic_time() - start_time

      TimelessLogs.Telemetry.event(
        [:timeless_logs, :retention, :stop],
        %{duration: duration, blocks_deleted: total_deleted},
        %{}
      )

      {:ok, total_deleted}
    end
  end

  defp cleanup_by_age(max_age_seconds) do
    cutoff = System.system_time(:second) - max_age_seconds
    TimelessLogs.Index.delete_blocks_before(cutoff)
  end

  defp cleanup_by_size(max_bytes) do
    TimelessLogs.Index.delete_blocks_over_size(max_bytes)
  end

  defp cleanup_by_term_pressure(max_entries) do
    start_time = System.monotonic_time()
    deleted = TimelessLogs.Index.delete_oldest_blocks_until_term_limit(max_entries)

    if deleted > 0 do
      duration = System.monotonic_time() - start_time

      TimelessLogs.Telemetry.event(
        [:timeless_logs, :retention, :term_pressure],
        %{duration: duration, blocks_deleted: deleted},
        %{}
      )
    end

    deleted
  end
end
