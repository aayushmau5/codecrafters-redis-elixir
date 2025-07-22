defmodule Wait do
  use GenServer

  @config_table :config
  @replicas_table :replicas

  # Public API
  def start_link(args) do
    GenServer.start_link(__MODULE__, args)
  end

  def wait_for_replicas(pid) do
    GenServer.call(pid, :wait, :infinity)
  end

  def notify_ack(pid) do
    GenServer.cast(pid, :ack_received)
  end

  # GenServer Implementation
  @impl true
  def init(args) do
    required_replicas = Keyword.get(args, :required_replicas)
    timeout_ms = Keyword.get(args, :timeout_ms)

    dbg(
      "Wait GenServer initialized with #{required_replicas} replicas and #{timeout_ms}ms timeout"
    )

    # Register this process globally so the main server can find it
    :ets.insert(@config_table, {:current_wait_pid, self()})

    {:ok,
     %{
       ack_count: 0,
       timeout_ms: timeout_ms,
       required_replicas: required_replicas,
       caller: nil,
       timer_ref: nil
     }}
  end

  @impl true
  def handle_call(:wait, from, %{timeout_ms: timeout_ms} = state) do
    dbg("GenServer handle_call :wait received")

    replicas = :ets.match(@replicas_table, {:client, :"$1"}) |> List.flatten()
    dbg("Found #{length(replicas)} replicas: #{inspect(replicas)}")

    if Enum.empty?(replicas) do
      {:reply, 0, state}
    else
      # Send GETACK to all replicas
      Enum.each(replicas, fn replica_connection ->
        send_getack(replica_connection)
      end)

      # Set up timeout
      timer_ref = Process.send_after(self(), :timeout, timeout_ms)
      new_state = %{state | caller: from, timer_ref: timer_ref, ack_count: 0}

      {:noreply, new_state}
    end
  end

  @impl true
  def handle_cast(:ack_received, state) do
    dbg("ack_received")

    %{
      ack_count: ack_count,
      required_replicas: required_replicas,
      caller: caller,
      timer_ref: timer_ref
    } = state

    new_ack_count = ack_count + 1

    # Check if we've received enough ACKs
    if new_ack_count >= required_replicas do
      # Cancel timer and reply immediately
      if timer_ref, do: Process.cancel_timer(timer_ref)
      if caller, do: GenServer.reply(caller, new_ack_count)
      {:noreply, %{state | caller: nil, timer_ref: nil, ack_count: new_ack_count}}
    else
      {:noreply, %{state | ack_count: new_ack_count}}
    end
  end

  @impl true
  def handle_info(:timeout, %{ack_count: ack_count, caller: caller} = state) do
    dbg("timed out")

    if caller do
      GenServer.reply(caller, ack_count)
    end

    {:noreply, %{state | caller: nil, timer_ref: nil}}
  end

  # Private Functions
  defp send_getack(replica_connection) do
    getack_command = "*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n"
    :gen_tcp.send(replica_connection, getack_command)
  end
end
