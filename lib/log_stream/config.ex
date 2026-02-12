defmodule LogStream.Config do
  @moduledoc false

  @spec storage() :: :disk | :memory
  def storage do
    Application.get_env(:log_stream, :storage, :disk)
  end

  @spec data_dir() :: String.t()
  def data_dir do
    Application.get_env(:log_stream, :data_dir, "priv/log_stream")
  end

  @spec flush_interval() :: pos_integer()
  def flush_interval do
    Application.get_env(:log_stream, :flush_interval, 1_000)
  end

  @spec max_buffer_size() :: pos_integer()
  def max_buffer_size do
    Application.get_env(:log_stream, :max_buffer_size, 1_000)
  end

  @spec query_timeout() :: pos_integer()
  def query_timeout do
    Application.get_env(:log_stream, :query_timeout, 30_000)
  end

  # 7 days in seconds
  @default_retention_max_age 7 * 86_400

  # 512 MB
  @default_retention_max_size 512 * 1_048_576

  @spec retention_max_age() :: pos_integer() | nil
  def retention_max_age do
    Application.get_env(:log_stream, :retention_max_age, @default_retention_max_age)
  end

  @spec retention_max_size() :: pos_integer() | nil
  def retention_max_size do
    Application.get_env(:log_stream, :retention_max_size, @default_retention_max_size)
  end

  @spec retention_check_interval() :: pos_integer()
  def retention_check_interval do
    Application.get_env(:log_stream, :retention_check_interval, 300_000)
  end

  @spec compaction_threshold() :: pos_integer()
  def compaction_threshold do
    Application.get_env(:log_stream, :compaction_threshold, 500)
  end

  @spec compaction_interval() :: pos_integer()
  def compaction_interval do
    Application.get_env(:log_stream, :compaction_interval, 30_000)
  end

  @spec compaction_max_raw_age() :: pos_integer()
  def compaction_max_raw_age do
    Application.get_env(:log_stream, :compaction_max_raw_age, 60)
  end
end
