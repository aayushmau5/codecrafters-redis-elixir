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

    :ets.new(:config, [:set, :protected, :named_table])
    :ets.insert(:config, {:port, port})

    {:ok, _pid} = Agent.start_link(fn -> %{} end, name: :redis_storage)

    if dir != nil and dbfilename != nil do
      # db config
      :ets.insert(:config, {:dir, dir})
      :ets.insert(:config, {:dbfilename, dbfilename})

      Storage.run(Path.join(dir, dbfilename), :redis_storage)
    end

    if replica !== nil do
      [_host, port] = String.split(replica, " ")
      _port = String.to_integer(port)
      :ets.insert(:config, {:is_replica, true})
    end

    Supervisor.start_link([{Task, fn -> Server.listen() end}], strategy: :one_for_one)
  end

  @doc """
  Listen for incoming connections
  """
  def listen() do
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    IO.puts("Logs from your program will appear here!")

    [port: port] = :ets.lookup(:config, :port)

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
      {:ok, data} ->
        response = handle_data(data)
        :gen_tcp.send(client, response)
        handle_socket(client)

      {:error, reason} ->
        IO.puts("Error receiving data: #{reason}")
    end
  end

  defp handle_data(data) do
    command =
      data
      |> String.trim()
      |> String.split("\r\n")
      |> dbg()

    handle_command(command)
  end

  defp handle_command(["*" <> _len | rest]) do
    handle_command(rest)
  end

  defp handle_command(["+" | rest]) do
    handle_command(rest)
  end

  defp handle_command([_, command | rest]) do
    case String.downcase(command) do
      "info" -> handle_info(rest)
      "config" -> handle_config(rest)
      "keys" -> handle_key(rest)
      "set" -> handle_set(rest)
      "get" -> handle_get(rest)
      "echo" -> handle_echo(rest)
      "ping" -> handle_ping(rest)
    end
  end

  # INFO replication
  defp handle_info([_, key]) do
    case key do
      "replication" ->
        case :ets.lookup(:config, :isreplica) do
          [] -> "$11\r\nrole:master\r\n"
          [is_replica: true] -> "$10\r\nrole:slave\r\n"
        end

      _ ->
        return_nil()
    end
  end

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

      _ ->
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

  defp handle_ping([]) do
    "+PONG\r\n"
  end

  defp handle_ping([_, pong]) do
    "$#{String.length(pong)}\r\n#{pong}\r\n"
  end

  defp return_nil(), do: "$-1\r\n"
end

defmodule CLI do
  def main(_args) do
    # Start the Server application
    {:ok, _pid} = Application.ensure_all_started(:codecrafters_redis)

    # Run forever
    Process.sleep(:infinity)
  end
end
