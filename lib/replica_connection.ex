defmodule ReplicaConnection do
  # from replica -> master connection
  use GenServer

  @config_table :config

  def start_link(args) do
    name = Keyword.get(args, :name)
    GenServer.start_link(__MODULE__, args, name: name)
  end

  def initialize(server) do
    GenServer.call(server, :init, 15_000)
  end

  def run_handler(server) do
    GenServer.cast(server, :handle_commands)
  end

  @impl true
  def init(config) do
    host = Keyword.get(config, :host)
    port = Keyword.get(config, :port)
    name = Keyword.get(config, :name)

    {:ok, socket} =
      :gen_tcp.connect(String.to_charlist(host), port, [:binary, active: false])

    {:ok, %{socket: socket, host: host, port: port, name: name}}
  end

  @impl true
  def handle_call(:init, _from, %{socket: socket} = state) do
    IO.puts("Initializing replica instance")
    # PING
    :ok = :gen_tcp.send(socket, "*1\r\n$4\r\nPING\r\n")
    {:ok, "+PONG\r\n"} = :gen_tcp.recv(socket, 0)

    [port: port] = :ets.lookup(@config_table, :port)

    # REPLCONF listening-port <port>
    :ok =
      :gen_tcp.send(
        socket,
        "*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n#{port}\r\n"
      )

    {:ok, "+OK\r\n"} = :gen_tcp.recv(socket, 0)

    # REPLCONF capa psync2
    :ok = :gen_tcp.send(socket, "*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n")
    {:ok, "+OK\r\n"} = :gen_tcp.recv(socket, 0)

    # PSYNC ? -1
    :ok = :gen_tcp.send(socket, "*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n")
    {:ok, "+FULLRESYNC" <> binary_data} = :gen_tcp.recv(socket, 0)

    # cases:
    # binary_data doesn't contain any command(we need to handle data later)
    # binary_data contains file
    # binary_data contains file + a pipelined command
    case :binary.match(binary_data, <<"*">>) do
      {position, _} ->
        <<_::binary-size(position), binary_data::binary>> = binary_data
        handler(binary_data, socket)

      :nomatch ->
        response = :gen_tcp.recv(socket, 0, 5_000) |> dbg()

        case response do
          {:ok, binary_data} ->
            case :binary.match(binary_data, <<"*">>) do
              {position, _} ->
                <<_::binary-size(position), binary_data::binary>> = binary_data
                handler(binary_data, socket)

              :nomatch ->
                dbg(binary_data)
            end

          error ->
            dbg(error)
        end
    end

    {:reply, "done", state}
  end

  @impl true
  def handle_cast(:handle_commands, %{socket: socket}) do
    handle_propagated_responses(socket)
  end

  defp handle_propagated_responses(socket) do
    case :gen_tcp.recv(socket, 0) do
      {:ok, binary_data} ->
        handler(binary_data, socket)
        handle_propagated_responses(socket)

      {:error, :closed} ->
        dbg("Connection closed")

      {:error, :enotconn} ->
        dbg("Not connected to the socket")

      error ->
        dbg(error)
    end
  end

  defp handler(binary_data, client) do
    dbg(binary_data)

    data = Utils.split_data(binary_data)
    commands = Utils.separate_commands(data, []) |> dbg()

    Enum.each(commands, fn command_body ->
      command_binary = Server.reconstruct_binary_command(command_body)
      {command, rest} = Server.get_command(command_body)

      dbg("HANDLING IN REPLICA connection #{inspect(client)} #{command_binary}")

      response = Server.handle_command(command, rest)
      if reply?(command), do: :gen_tcp.send(client, response)
      Server.update_offset(command, command_binary)
    end)
  end

  defp reply?(command) do
    Enum.member?(["replconf"], command)
  end
end
