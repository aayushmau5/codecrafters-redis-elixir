defmodule Server do
  @moduledoc """
  Your implementation of a Redis server
  """

  use Application

  def start(_type, _args) do
    args = parse_arguments()
    dir = Keyword.get(args, :dir)
    dbfilename = Keyword.get(args, :dbfilename)
    port = Keyword.get(args, :port, 6379)
    replica = Keyword.get(args, :replicaof)

    :ets.new(:config, [
      :set,
      :public,
      :named_table,
      read_concurrency: true,
      write_concurrency: true
    ])

    :ets.insert(:config, {:port, port})
    :ets.insert(:config, {:offset, 0})
    :ets.insert(:config, {:pending_writes, false})

    :ets.new(:redis_storage, [
      :set,
      :public,
      :named_table,
      read_concurrency: true,
      write_concurrency: true
    ])

    :ets.new(:replicas, [
      :bag,
      :public,
      :named_table,
      read_concurrency: true,
      write_concurrency: true
    ])

    if dir != nil and dbfilename != nil do
      # db config
      :ets.insert(:config, {:dir, dir})
      :ets.insert(:config, {:dbfilename, dbfilename})

      rdb_file_path = Path.join(dir, dbfilename)
      :ets.insert(:config, {:dbfilepath, rdb_file_path})

      Storage.run(rdb_file_path)
    end

    if replica do
      [host, port] = String.split(replica, " ")
      port = String.to_integer(port)
      :ets.insert(:config, {:is_replica, true})
      :ets.insert(:config, {:master_host, host})
      :ets.insert(:config, {:master_port, port})
    else
      master_repl_id = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
      master_repl_offset = 0
      :ets.insert(:config, {:master_repl_id, master_repl_id})
      :ets.insert(:config, {:master_repl_offset, master_repl_offset})
    end

    Supervisor.start_link([{Task, fn -> Server.listen() end}], strategy: :one_for_one)
  end

  @doc """
  Listen for incoming connections
  """
  def listen() do
    [port: port] = :ets.lookup(:config, :port)

    # When replica, perform handshake with master and kickoff syncing process
    if replica?() do
      [master_host: master_host] = :ets.lookup(:config, :master_host)
      [master_port: master_port] = :ets.lookup(:config, :master_port)

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
        Task.start(fn -> handle_socket(client) end)
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
          command_binary = reconstruct_binary_command(command_body)
          {command, rest} = get_command(command_body)

          is_replica_connection = is_replica_connection?(command)

          if is_replica_connection do
            :ets.insert(:replicas, {:client, client})
          end

          dbg(command_binary)
          response = handle_command(command, rest) |> dbg()
          :gen_tcp.send(client, response) |> dbg()

          update_offset(command, command_binary)
          propagate(command, command_binary)
        end)

        handle_socket(client)

      {:error, :timeout} ->
        # Check if this is a replica connection
        is_replica = :ets.match(:replicas, {:client, client}) != []

        # For replica connections, timeout is normal - just continue listening
        if is_replica, do: handle_socket(client)

      {:error, reason} ->
        IO.puts("Error receiving data: #{reason}")
    end
  end

  def get_command([_prefix, _length, command | rest]) do
    {String.downcase(command), rest}
  end

  def handle_command(command, data) do
    case command do
      "replconf" -> handle_repl_conf(data)
      "psync" -> handle_psync(data)
      "info" -> handle_info(data)
      "wait" -> handle_wait(data)
      "config" -> handle_config(data)
      "keys" -> handle_key(data)
      "type" -> handle_type(data)
      "set" -> handle_set(data)
      "get" -> handle_get(data)
      "echo" -> handle_echo(data)
      "ping" -> handle_ping(data)
    end
  end

  # REPLCONF
  defp handle_repl_conf([_, "listening-port", _, _port]), do: "+OK\r\n"
  defp handle_repl_conf([_, "capa", _, "psync2"]), do: "+OK\r\n"

  defp handle_repl_conf([_, "GETACK", _, "*"]) do
    [offset: offset] = :ets.lookup(:config, :offset)
    offset_length = String.length("#{offset}")
    "*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$#{offset_length}\r\n#{offset}\r\n"
  end

  defp handle_repl_conf([_, "ACK", _, _]) do
    # Notify the current wait process if one exists
    case :ets.lookup(:config, :current_wait_pid) do
      [{:current_wait_pid, wait_pid}] when is_pid(wait_pid) ->
        if Process.alive?(wait_pid) do
          Wait.notify_ack(wait_pid)
        end

      _ ->
        :ok
    end

    ""
  end

  # PYSNC
  defp handle_psync([_, "?", _, "-1"]) do
    [master_repl_id: master_repl_id] = :ets.lookup(:config, :master_repl_id)

    db_file_path =
      case :ets.lookup(:config, :dbfilepath) do
        [dbfilepath: dbfilepath] -> dbfilepath
        [] -> "empty.rdb"
      end

    empty_rdb_bytes = File.read!(db_file_path)

    "+FULLRESYNC #{master_repl_id} 0\r\n$#{IO.iodata_length(empty_rdb_bytes)}\r\n#{empty_rdb_bytes}"
  end

  # INFO replication
  defp handle_info([_, key]) do
    case key do
      "replication" ->
        case :ets.lookup(:config, :is_replica) do
          [] ->
            [master_repl_id: master_repl_id] = :ets.lookup(:config, :master_repl_id)
            [master_repl_offset: master_repl_offset] = :ets.lookup(:config, :master_repl_offset)
            master_repl_id = "master_replid:#{master_repl_id}"
            master_repl_offset = "master_repl_offset:#{master_repl_offset}"

            data = """
            role:master
            #{master_repl_id}
            #{master_repl_offset}
            """

            "$#{String.length(data)}\r\n#{data}\r\n"

          [is_replica: true] ->
            "$10\r\nrole:slave\r\n"
        end

      _ ->
        return_nil()
    end
  end

  # WAIT command
  defp handle_wait([_, num_replicas, _, wait_time]) do
    required_replicas = String.to_integer(num_replicas)
    wait_time_ms = String.to_integer(wait_time)

    # Check if there are pending writes since the last WAIT
    case :ets.lookup(:config, :pending_writes) do
      [{:pending_writes, false}] ->
        # No pending writes, return number of replicas immediately
        replicas = :ets.match(:replicas, {:client, :"$1"}) |> List.flatten()
        replica_count = length(replicas)
        ":#{replica_count}\r\n"

      [{:pending_writes, true}] ->
        # There are pending writes, send GETACK and wait for responses
        {:ok, wait_pid} =
          Wait.start_link(required_replicas: required_replicas, timeout_ms: wait_time_ms)

        ack_count = Wait.wait_for_replicas(wait_pid) |> dbg()

        # Clean up the wait process reference and reset pending writes
        :ets.delete(:config, :current_wait_pid)
        :ets.insert(:config, {:pending_writes, false})

        ":#{ack_count}\r\n"
    end
  end

  # CONFIG
  defp handle_config([_, command | rest]) do
    case String.downcase(command) do
      "get" -> config_get(rest)
    end
  end

  defp config_get([_, conf]) do
    case String.downcase(conf) do
      "dir" ->
        [dir: dir] = :ets.lookup(:config, :dir)
        "*2\r\n$3\r\ndir\r\n$#{String.length(dir)}\r\n#{dir}\r\n"

      "dbfilename" ->
        [dbfilename: name] = :ets.lookup(:config, :dbfilename)
        "*2\r\n$10\r\ndbfilename\r\n$#{String.length(name)}\r\n#{name}\r\n"
    end
  end

  # KEYS
  defp handle_key([_, "*"]) do
    keys = :ets.match(:redis_storage, {:"$1", :_})
    key_list = List.flatten(keys)

    Enum.reduce(key_list, "*#{length(keys)}\r\n", fn value, acc ->
      acc <> "$#{String.length(value)}\r\n#{value}\r\n"
    end)
  end

  defp handle_key([_, key]) do
    if :ets.lookup(:redis_storage, key) != [] do
      "*1\r\n$#{String.length(key)}\r\n#{key}\r\n"
    else
      return_nil()
    end
  end

  # TYPE
  defp handle_type([_, _key] = data) do
    case handle_get(data) do
      "$-1\r\n" -> "+none\r\n"
      _ -> "+string\r\n"
    end
  end

  # ["$3", "foo", "$4", "bar"]
  # ["$3", "foo", "$4", "bar", "$2", "px", "$3", "100"]
  defp handle_set([_, key, _, value | rest]) do
    options = handle_set_options(rest, %{})

    value_data =
      case options do
        %{ttl_ms: ttl_ms} ->
          expiry_time = :erlang.system_time(:millisecond) + ttl_ms
          {value, expiry_time}

        _ ->
          {value, nil}
      end

    :ets.insert(:redis_storage, {key, value_data})
    "+OK\r\n"
  end

  defp handle_set_options(["$" <> _, name, "$" <> _, value | rest] = _options, map) do
    case name do
      "px" ->
        map = Map.put(map, :ttl_ms, String.to_integer(value))
        handle_set_options(rest, map)

      other ->
        dbg("Unhandled option #{other}")
        map
    end
  end

  defp handle_set_options([], map), do: map

  defp handle_get([_, key]) do
    case :ets.lookup(:redis_storage, key) do
      [] ->
        return_nil()

      [{^key, {value, nil}}] ->
        "$#{String.length(value)}\r\n#{value}\r\n"

      [{^key, {value, expiry_time}}] ->
        current_time = :erlang.system_time(:millisecond)

        if current_time < expiry_time do
          "$#{String.length(value)}\r\n#{value}\r\n"
        else
          :ets.delete(:redis_storage, key)
          return_nil()
        end
    end
  end

  defp handle_echo([_, message]) do
    "$#{String.length(message)}\r\n#{message}\r\n"
  end

  defp handle_ping(data) do
    case data do
      ["$" <> _len, pong] ->
        "$#{String.length(pong)}\r\n#{pong}\r\n"

      [] ->
        "+PONG\r\n"
    end
  end

  defp return_nil(), do: "$-1\r\n"

  defp master?(), do: !replica?()

  defp replica?() do
    case :ets.lookup(:config, :is_replica) do
      [] -> false
      [is_replica: true] -> true
    end
  end

  def reconstruct_binary_command(command_body) do
    Enum.join(command_body, "\r\n") <> "\r\n"
  end

  defp needs_propagation?(command), do: Enum.member?(["set"], command)

  # Handle what kind of commands we need to send over
  defp propagate(command, command_binary) do
    if master?() and needs_propagation?(command) do
      # Mark that we have pending writes
      :ets.insert(:config, {:pending_writes, true})

      clients = :ets.match(:replicas, {:client, :"$1"})

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
      do: :ets.update_counter(:config, :offset, byte_size(command_binary))
  end

  defp parse_arguments() do
    {options, _, _} =
      OptionParser.parse(System.argv(),
        strict: [dir: :string, dbfilename: :string, port: :integer, replicaof: :string]
      )

    options
  end

  defp is_replica_connection?(command), do: master?() and command == "psync"
end

defmodule CLI do
  def main(_args) do
    # Start the Server application
    {:ok, _pid} = Application.ensure_all_started(:codecrafters_redis)

    # Run forever
    Process.sleep(:infinity)
  end
end
