defmodule Block do
  use GenServer

  @config_table :config

  def start_link(args) do
    GenServer.start_link(__MODULE__, args)
  end

  def wait_for_stream(pid) do
    GenServer.call(pid, :block, :infinity)
  end

  def add_stream(pid, stream_key, id) do
    GenServer.cast(pid, {:stream, stream_key, id})
  end

  @impl true
  def init(args) do
    timeout_ms = Keyword.get(args, :timeout_ms)
    required_stream_key = Keyword.get(args, :stream_key)
    required_id = Keyword.get(args, :id)
    :ets.insert(@config_table, {:current_block_pid, self()})

    {:ok,
     %{
       timeout_ms: timeout_ms,
       required_stream_key: required_stream_key,
       required_id: required_id,
       timer_ref: nil,
       caller: nil
     }}
  end

  @impl true
  def handle_call(:block, from, %{timeout_ms: timeout_ms} = state) do
    timer_ref = Process.send_after(self(), :timeout, timeout_ms)
    {:noreply, %{state | timer_ref: timer_ref, caller: from}}
  end

  @impl true
  def handle_cast({:stream, stream_key, id}, %{caller: caller, timer_ref: timer_ref} = state) do
    {required_ms, required_offset} = id_to_tuple(state.required_id)
    {ms, offset} = id_to_tuple(id)

    cond do
      stream_key == state.required_stream_key ->
        if ms >= required_ms and offset >= required_offset do
          if timer_ref, do: Process.cancel_timer(timer_ref)
          if caller, do: GenServer.reply(caller, {:ok, {stream_key, id}})
          {:noreply, %{state | caller: nil, timer_ref: nil}}
        else
          {:noreply, state}
        end

      true ->
        {:noreply, state}
    end
  end

  @impl true
  def handle_info(:timeout, %{caller: caller} = state) do
    if caller do
      GenServer.reply(caller, {:error, :nostream})
    end

    {:noreply, %{state | caller: nil, timer_ref: nil}}
  end

  defp id_to_tuple(id) when is_binary(id) do
    String.split(id, "-")
    |> then(fn [last_ms, last_offset] ->
      {String.to_integer(last_ms), String.to_integer(last_offset)}
    end)
  end
end
