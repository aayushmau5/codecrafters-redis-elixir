defmodule Server do
  @moduledoc """
  Your implementation of a Redis server
  """

  use Application

  def start(_type, _args) do
    {options, _, _} =
      OptionParser.parse(System.argv(),
        strict: [dir: :string, dbfilename: :string, port: :integer, replicaof: :string]
      )

    dir = Keyword.get(options, :dir)
    dbfilename = Keyword.get(options, :dbfilename)
    port = Keyword.get(options, :port, 6379)
    replica = Keyword.get(options, :replicaof)

    :ets.new(:config, [:set, :public, :named_table])
    :ets.insert(:config, {:port, port})
    :ets.insert(:config, {:offset, 0})

    {:ok, _pid} = Agent.start_link(fn -> %{} end, name: :redis_storage)
    {:ok, _pid} = Agent.start_link(fn -> [] end, name: :replicas)

    if dir != nil and dbfilename != nil do
      # db config
      :ets.insert(:config, {:dir, dir})
      :ets.insert(:config, {:dbfilename, dbfilename})

      rdb_file_path = Path.join(dir, dbfilename)
      :ets.insert(:config, {:dbfilepath, rdb_file_path})

      Storage.run(rdb_file_path, :redis_storage)
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
    case :gen_tcp.recv(client, 0) do
      {:ok, binary_data} ->
        data = Utils.split_data(binary_data)
        commands = Utils.separate_commands(data, [])

        Enum.each(commands, fn command_body ->
          command_binary_data = reconstruct_binary_command(command_body)
          {command, rest} = get_command(command_body)

          if master?() and command == "psync" do
            Agent.update(:replicas, fn clients -> [client | clients] end)
          end

          response = handle_command(command, rest)
          :gen_tcp.send(client, response)
          :ets.update_counter(:config, :offset, byte_size(command_binary_data))

          if master?() do
            handle_propagation(command_binary_data, command)
          end
        end)

        handle_socket(client)

      {:error, reason} ->
        IO.puts("Error receiving data: #{reason}")
    end
  end

  # Handle what kind of commands we need to send over
  defp handle_propagation(binary_data, command) do
    case command do
      "set" ->
        Agent.get(:replicas, fn clients ->
          Enum.each(clients, fn client ->
            :gen_tcp.send(client, binary_data) |> dbg()
          end)
        end)

      _ ->
        nil
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
      "set" -> handle_set(data)
      "get" -> handle_get(data)
      "echo" -> handle_echo(data)
      "ping" -> handle_ping(data)
    end
  end

  # REPLCONF
  defp handle_repl_conf([_, "listening-port", _, _port]) do
    "+OK\r\n"
  end

  defp handle_repl_conf([_, "capa", _, "psync2"]) do
    "+OK\r\n"
  end

  defp handle_repl_conf([_, "GETACK", _, "*"]) do
    [offset: offset] = :ets.lookup(:config, :offset)
    offset_length = String.length("#{offset}")
    "*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$#{offset_length}\r\n#{offset}\r\n"
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
    _num_replicas = String.to_integer(num_replicas)
    _wait_time = String.to_integer(wait_time)
    connected_replicas = get_connected_replicas()
    ":#{connected_replicas}\r\n"
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

  defp handle_key([_, "*"]) do
    keys =
      Agent.get(:redis_storage, fn state ->
        Map.keys(state)
      end)

    Enum.reduce(keys, "*#{length(keys)}\r\n", fn value, acc ->
      acc <> "$#{String.length(value)}\r\n#{value}\r\n"
    end)
  end

  defp handle_key([_, key]) do
    keys =
      Agent.get(:redis_storage, fn state ->
        Map.keys(state)
      end)

    if Enum.member?(keys, key) do
      "*1\r\n$#{String.length(key)}\r\n#{key}\r\n"
    else
      return_nil()
    end
  end

  # ["$3", "foo", "$4", "bar"]
  # ["$3", "foo", "$4", "bar", "$2", "px", "$3", "100"]
  defp handle_set([_, key, _, value | rest]) do
    options = handle_set_options(rest, %{})
    value = Map.merge(%{value: value}, options)

    Agent.update(:redis_storage, fn map ->
      Map.put(map, key, value)
    end)

    "+OK\r\n"
  end

  defp handle_set_options(["$" <> _, name, "$" <> _, value | rest] = _options, map) do
    case name do
      "px" ->
        map =
          Map.merge(map, %{
            ttl: DateTime.add(DateTime.utc_now(), String.to_integer(value), :millisecond)
          })

        handle_set_options(rest, map)

      other ->
        dbg("Unhandled option #{other}")
        map
    end
  end

  defp handle_set_options([], map), do: map

  defp handle_get([_, key]) do
    value =
      Agent.get(:redis_storage, fn map ->
        Map.get(map, key)
      end)

    case value do
      nil ->
        return_nil()

      %{ttl: ttl, value: value} ->
        if DateTime.diff(ttl, DateTime.utc_now(), :millisecond) > 0 do
          "$#{String.length(value)}\r\n#{value}\r\n"
        else
          Agent.update(:redis_storage, fn map -> Map.delete(map, key) end)
          return_nil()
        end

      %{value: value} ->
        "$#{String.length(value)}\r\n#{value}\r\n"
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

  defp get_connected_replicas() do
    Agent.get(:replicas, &Enum.count(&1))
  end

  def reconstruct_binary_command(command_body) do
    Enum.join(command_body, "\r\n") <> "\r\n"
  end
end

defmodule CLI do
  def main(_args) do
    # Start the Server application
    {:ok, _pid} = Application.ensure_all_started(:codecrafters_redis)

    # Run forever
    Process.sleep(:infinity)
  end
end
