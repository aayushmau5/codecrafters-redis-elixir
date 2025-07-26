defmodule Server do
  @moduledoc """
  Your implementation of a Redis server
  """

  @config_table :config
  @replicas_table :replicas

  use Application

  def start(_type, _args) do
    args = parse_arguments()
    dir = Keyword.get(args, :dir)
    dbfilename = Keyword.get(args, :dbfilename)
    port = Keyword.get(args, :port, 6379)
    replica = Keyword.get(args, :replicaof)

    Storage.init_tables()
    Storage.add_config({:port, port})
    Storage.add_config({:offset, 0})
    Storage.add_config({:pending_writes, false})

    if dir != nil and dbfilename != nil do
      # db config
      Storage.add_config({:dir, dir})
      Storage.add_config({:dbfilename, dbfilename})

      rdb_file_path = Path.join(dir, dbfilename)
      RDB.load_from_file(rdb_file_path)
    end

    if replica do
      [host, port] = String.split(replica, " ")
      port = String.to_integer(port)

      Storage.add_config({:is_replica, true})
      Storage.add_config({:master_host, host})
      Storage.add_config({:master_port, port})
    else
      master_repl_id = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
      master_repl_offset = 0
      Storage.add_config({:master_repl_id, master_repl_id})
      Storage.add_config({:master_repl_offset, master_repl_offset})
    end

    Supervisor.start_link([{Task, fn -> Server.listen() end}], strategy: :one_for_one)
  end

  @doc """
  Listen for incoming connections
  """
  def listen() do
    port = Storage.get_config(:port)

    # When replica, perform handshake with master and kickoff syncing process
    if Utils.replica?() do
      master_host = Storage.get_config(:master_host)
      master_port = Storage.get_config(:master_port)

      Supervisor.start_link(
        [
          {
            ReplicaConnection,
            [host: master_host, port: master_port, name: ReplicaConnectionOne]
          }
        ],
        strategy: :one_for_one
      )

      ReplicaConnection.initialize(ReplicaConnectionOne)
      ReplicaConnection.run_handler(ReplicaConnectionOne)
    end

    # Since the tester restarts your program quite often, setting SO_REUSEADDR
    # ensures that we don't run into 'Address already in use' errors
    {:ok, socket} = :gen_tcp.listen(port, [:binary, active: false, reuseaddr: true])
    loop_accept(socket)
  end

  defp loop_accept(socket) do
    case :gen_tcp.accept(socket) do
      {:ok, client} ->
        Task.start(fn ->
          # Five minutes
          :timer.kill_after(300_000)
          handle_socket(client)
        end)

        loop_accept(socket)

      {:error, reason} ->
        IO.puts("Error accepting connection: #{reason}")
        :gen_tcp.close(socket)
    end
  end

  defp handle_socket(client) do
    case :gen_tcp.recv(client, 0, 5000) do
      {:ok, binary_data} ->
        dbg("RECEIVED ON MAIN THREAD: #{binary_data}")
        data = Utils.split_data(binary_data)
        commands = Utils.separate_commands(data, [])

        Enum.each(commands, fn command_body ->
          command_binary = Commands.reconstruct_binary_command(command_body)
          {command, rest} = Commands.get_command(command_body)

          is_replica_connection = is_replica_connection?(command)

          if is_replica_connection do
            :ets.insert(@replicas_table, {:client, client})
          end

          response = handle_command(command, rest) |> dbg()
          :gen_tcp.send(client, response) |> dbg()

          update_offset(command, command_binary)
          propagate(command, command_binary)
        end)

        handle_socket(client)

      {:error, :timeout} ->
        # For replica connections, timeout is normal - just continue listening
        # Check if this is a replica connection
        is_replica = :ets.match(@replicas_table, {:client, client}) != []
        if is_replica, do: handle_socket(client)

      {:error, reason} ->
        IO.puts("Error receiving data: #{reason}")
    end
  end

  def handle_command(command, data) do
    # If in transaction, queue up commands
    if Process.get(:in_transaction) && should_queue_command?(command) do
      queued = Process.get(:queued_commands)
      Process.put(:queued_commands, [{command, data} | queued])
      "+QUEUED\r\n"
    else
      # Non-transaction, handle commands(in request-response manner)
      Commands.execute(command, data)
    end
  end

  defp needs_propagation?(command), do: Enum.member?(["set"], command)

  # Handle what kind of commands we need to send over
  defp propagate(command, command_binary) do
    if Utils.master?() and needs_propagation?(command) do
      # Mark that we have pending writes
      Storage.add_config({:pending_writes, true})

      clients = :ets.match(@replicas_table, {:client, :"$1"})

      clients
      |> List.flatten()
      |> Task.async_stream(
        fn client ->
          :gen_tcp.send(client, command_binary)
        end,
        max_concurrency: 10,
        timeout: 1000
      )
      |> Stream.run()
    end
  end

  def needs_offset_update?(command), do: command in ["ping", "set", "del", "replconf"]

  def update_offset(command, command_binary) do
    if needs_offset_update?(command),
      do: :ets.update_counter(@config_table, :offset, byte_size(command_binary))
  end

  defp parse_arguments() do
    {options, _, _} =
      OptionParser.parse(System.argv(),
        strict: [dir: :string, dbfilename: :string, port: :integer, replicaof: :string]
      )

    options
  end

  defp is_replica_connection?(command), do: Utils.master?() and command == "psync"

  defp should_queue_command?(command), do: command not in ["multi", "exec", "discard", "ping"]
end

defmodule CLI do
  def main(_args) do
    # Start the Server application
    {:ok, _pid} = Application.ensure_all_started(:codecrafters_redis)

    # Run forever
    Process.sleep(:infinity)
  end
end
