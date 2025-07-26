defmodule Commands.Wait do
  @replicas_table :replicas

  def handle_wait([_, num_replicas, _, wait_time]) do
    required_replicas = String.to_integer(num_replicas)
    wait_time_ms = String.to_integer(wait_time)

    # Check if there are pending writes since the last WAIT
    case Storage.get_config(:pending_writes) do
      false ->
        # No pending writes, return number of replicas immediately
        replicas = :ets.match(@replicas_table, {:client, :"$1"}) |> List.flatten()
        replica_count = length(replicas)
        ":#{replica_count}\r\n"

      true ->
        # There are pending writes, send GETACK and wait for responses
        {:ok, wait_pid} =
          Wait.start_link(required_replicas: required_replicas, timeout_ms: wait_time_ms)

        ack_count = Wait.wait_for_replicas(wait_pid) |> dbg()

        # Clean up the wait process reference and reset pending writes
        Storage.delete_config(:current_wait_pid)
        Storage.add_config({:pending_writes, false})

        ":#{ack_count}\r\n"
    end
  end
end
