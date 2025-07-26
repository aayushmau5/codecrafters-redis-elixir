defmodule BlockPop do
  use GenServer

  @config_table :config

  def start_link(args) do
    GenServer.start_link(__MODULE__, args)
  end

  def wait_for_push(pid) do
    GenServer.call(pid, :block, :infinity)
  end

  def push_element(pid, key) do
    GenServer.cast(pid, {:push, key})
  end

  @impl true
  def init(args) do
    timeout_ms = Keyword.get(args, :timeout_ms)
    required_key = Keyword.get(args, :key)

    case Storage.get_config(:waiting_blpop_pids) do
      nil ->
        Storage.add_config({:waiting_blpop_pids, [self()]})

      waiting_blpop_pids ->
        Storage.add_config({:waiting_blpop_pids, waiting_blpop_pids ++ [self()]})
    end

    {:ok,
     %{
       timeout_ms: timeout_ms,
       required_key: required_key,
       timer_ref: nil,
       caller: nil
     }}
  end

  @impl true
  def handle_call(:block, from, %{timeout_ms: timeout_ms} = state) do
    dbg("Block for push")

    timer_ref =
      if timeout_ms != 0, do: Process.send_after(self(), :timeout, timeout_ms), else: nil

    {:noreply, %{state | timer_ref: timer_ref, caller: from}}
  end

  @impl true
  def handle_cast({:push, key}, %{caller: caller, timer_ref: timer_ref} = state) do
    dbg("Got push for #{key}")

    if state.required_key == key do
      # Remove from waiting list when we're done
      case Storage.get_config(:waiting_blpop_pids) do
        nil ->
          :ok

        pids ->
          updated_pids = List.delete(pids, self()) |> dbg()
          Storage.add_config({:waiting_blpop_pids, updated_pids})
      end

      if timer_ref, do: Process.cancel_timer(timer_ref)
      if caller, do: GenServer.reply(caller, {:ok, key})

      {:noreply, %{state | caller: nil, timer_ref: nil}}
    else
      {:noreply, state}
    end
  end

  @impl true
  def handle_info(:timeout, %{caller: caller} = state) do
    dbg("timed out")

    if caller do
      GenServer.reply(caller, {:error, :nopush})

      # Remove from waiting list on timeout
      case :ets.lookup(@config_table, :waiting_blpop_pids) do
        [waiting_blpop_pids: pids] ->
          updated_pids = List.delete(pids, self())
          Storage.add_config({:waiting_blpop_pids, updated_pids})

        [] ->
          :ok
      end
    end

    {:noreply, %{state | caller: nil, timer_ref: nil}}
  end
end
