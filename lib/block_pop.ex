defmodule BlockPop do
  use GenServer

  def start_link(_args) do
    GenServer.start_link(__MODULE__, %{}, name: __MODULE__)
  end

  def wait_for_element(key, timeout_ms) do
    case try_pop_element(key) do
      {:ok, response} ->
        {:ok, response}

      :empty ->
        GenServer.call(__MODULE__, {:wait, key, timeout_ms}, :infinity)
    end
  end

  def notify_push(key) do
    GenServer.cast(__MODULE__, {:push, key})
  end

  defp try_pop_element(key) do
    case Storage.get_stored(key) do
      match when match in [nil, {[], nil}] ->
        :empty

      {[element | rest], _} ->
        Storage.add_to_store({key, {rest, nil}})
        # encode_array()
        response =
          "*2\r\n$#{String.length(key)}\r\n#{key}\r\n$#{String.length(element)}\r\n#{element}\r\n"

        {:ok, response}
    end
  end

  @impl true
  def init(_) do
    {:ok, %{waiting: %{}}}
  end

  @impl true
  def handle_call({:wait, key, timeout_ms}, from, state) do
    dbg("Block for push")
    waiters = Map.get(state.waiting, key, [])
    new_waiters = waiters ++ [from]
    new_state = %{state | waiting: Map.put(state.waiting, key, new_waiters)}

    if timeout_ms > 0, do: Process.send_after(self(), {:timeout, key, from}, timeout_ms)

    {:noreply, new_state}
  end

  @impl true
  def handle_cast({:push, key}, state) do
    case Map.get(state.waiting, key, []) do
      [] ->
        {:noreply, state}

      [first_waiter | rest_waiters] ->
        case try_pop_element(key) do
          {:ok, response} ->
            GenServer.reply(first_waiter, {:ok, response})

            new_waiting =
              if rest_waiters == [] do
                Map.delete(state.waiting, key)
              else
                Map.put(state.waiting, key, rest_waiters)
              end

            {:noreply, %{state | waiting: new_waiting}}

          :empty ->
            {:noreply, state}
        end
    end
  end

  @impl true
  def handle_info({:timeout, key, from}, state) do
    dbg("timed out")

    waiters = Map.get(state.waiting, key, [])

    new_waiters = List.delete(waiters, from)

    new_waiting =
      if new_waiters == [] do
        Map.delete(state.waiting, key)
      else
        Map.put(state.waiting, key, new_waiters)
      end

    GenServer.reply(from, {:error, :timeout})
    {:noreply, %{state | waiting: new_waiting}}
  end
end
