defmodule ReplicaConnection do
  # from replica -> master connection
  use GenServer

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

    [port: port] = :ets.lookup(:config, :port)

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
    {:ok, "+FULLRESYNC" <> _response} = :gen_tcp.recv(socket, 0)

    case :gen_tcp.recv(socket, 0, 10_000) do
      {:ok, "$" <> response} ->
        # I'm assuming this is a RDB file sent over the network
        dbg(response)

      _ ->
        dbg("failed")
    end

    Supervisor.start_link([{Task, fn -> handle_propagated_responses(socket) end}],
      strategy: :one_for_one
    )

    {:reply, "done", state}
  end

  defp handle_propagated_responses(socket) do
    case :gen_tcp.recv(socket, 0) do
      {:ok, binary_data} ->
        data =
          binary_data
          |> String.trim()
          |> String.split("\r\n")
          |> dbg()

        response = Server.handle_data(data) |> dbg()
        :gen_tcp.send(socket, response)

        handle_propagated_responses(socket)

      _ ->
        nil
    end
  end
end
