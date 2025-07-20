defmodule Wait do
  use GenServer

  @impl true
  def init(args) do
    required_replicas = Keyword.get(args, :required_replicas)
    timeout_ms = Keyword.get(args, :timeout_ms)
    ref = :ets.new(:acks, [:set, :public, read_concurrency: true, write_concurrency: true])
    :ets.insert(ref, {:ack, 0})

    dbg(
      "Wait GenServer initialized with #{required_replicas} replicas and #{timeout_ms}ms timeout"
    )

    {:ok,
     %{
       ack_table: ref,
       timeout_ms: timeout_ms,
       required_replicas: required_replicas,
       caller: nil,
       timer_ref: nil
     }}
  end

  def wait_for_replicas(pid) do
    GenServer.call(pid, :wait)
  end

  @impl true
  def handle_call(:wait, from, %{timeout_ms: timeout_ms} = state) do
    dbg("GenServer handle_call :wait received")
    timer_ref = Process.send_after(self(), :timeout, timeout_ms)
    new_state = %{state | caller: from, timer_ref: timer_ref}

    replicas = :ets.match(:replicas, {:client, :"$1"}) |> List.flatten()
    dbg("Found #{length(replicas)} replicas: #{inspect(replicas)}")

    if Enum.empty?(replicas) do
      {:reply, 0, new_state}
    else
      pid = self()

      Enum.each(replicas, fn replica_connection ->
        Task.start(fn ->
          case send_getack(replica_connection) do
            :ok -> wait_response(replica_connection, pid)
            _ -> nil
          end
        end)
      end)

      {:noreply, new_state}
    end
  end

  @impl true
  def handle_info(:ack_received, %{ack_table: ack_table} = state) do
    dbg("ack_received")
    :ets.update_counter(ack_table, :ack, 1)

    # if new_ack_count >= required_replicas do
    #   if timer_ref, do: Process.cancel_timer(timer_ref)
    #   if caller != nil, do: GenServer.reply(caller, new_ack_count)
    #   {:noreply, %{new_state | caller: nil, timer_ref: nil}}
    # else
    #   {:noreply, new_state}
    # end
    {:noreply, state}
  end

  @impl true
  def handle_info(:timeout, %{ack_table: ack_table, caller: caller} = state) do
    dbg("timed out")

    if caller do
      [ack: ack] = :ets.lookup(ack_table, :ack)
      GenServer.reply(caller, ack)
    end

    {:noreply, %{state | caller: nil, timer_ref: nil}}
  end

  def send_getack(replica_connection) do
    getack_command = "*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n"
    :gen_tcp.send(replica_connection, getack_command)
  end

  def wait_response(replica_connection, wait_pid) do
    case :gen_tcp.recv(replica_connection, 0, 1000) do
      {:ok, response} ->
        dbg(response)
        send(wait_pid, :ack_received)

      {:error, :ealready} ->
        dbg("SOCKET ALREADY IN USE #{inspect(replica_connection)}")

      # wait_response(replica_connection, wait_pid)

      error ->
        dbg(error)
    end
  end
end
