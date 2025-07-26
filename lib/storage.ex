defmodule Storage do
  @config_table :config
  @storage_table :redis_storage
  @stream_table :stream_storage
  @replicas_table :replicas

  def init_tables() do
    :ets.new(@config_table, [
      :set,
      :public,
      :named_table,
      read_concurrency: true,
      write_concurrency: true
    ])

    :ets.new(@storage_table, [
      :set,
      :public,
      :named_table,
      read_concurrency: true,
      write_concurrency: true
    ])

    :ets.new(@stream_table, [
      :ordered_set,
      :public,
      :named_table,
      read_concurrency: true,
      write_concurrency: true
    ])

    :ets.new(@replicas_table, [
      :bag,
      :public,
      :named_table,
      read_concurrency: true,
      write_concurrency: true
    ])
  end

  # Config

  def add_config(data) do
    :ets.insert(@config_table, data)
  end

  def get_config(key) do
    case :ets.lookup(@config_table, key) do
      [] -> nil
      [{_key, value}] -> value
    end
  end

  def delete_config(key) do
    :ets.delete(@config_table, key)
  end

  # Storage
  def add_to_store(data) do
    :ets.insert(@storage_table, data)
  end

  def get_stored(key) do
    case :ets.lookup(@storage_table, key) do
      [] -> nil
      [{^key, value}] -> value
    end
  end

  def delete_key(key) do
    :ets.delete(@storage_table, key)
  end

  def match_keys() do
    :ets.match(@storage_table, {:"$1", :_})
  end
end
