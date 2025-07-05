defmodule Server do
  @moduledoc """
  Your implementation of a Redis server
  """

  use Application

  def start(_type, _args) do
    {:ok, _pid} = Agent.start_link(fn -> %{} end, name: :redis_storage)
    Supervisor.start_link([{Task, fn -> Server.listen() end}], strategy: :one_for_one)
  end

  @doc """
  Listen for incoming connections
  """
  def listen() do
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    IO.puts("Logs from your program will appear here!")

    # Since the tester restarts your program quite often, setting SO_REUSEADDR
    # ensures that we don't run into 'Address already in use' errors
    {:ok, socket} = :gen_tcp.listen(6379, [:binary, active: false, reuseaddr: true])
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
      |> String.downcase()
      |> String.trim()
      |> String.split("\r\n")
      |> dbg()

    handle_command(command)
  end

  defp handle_command([_, _, "set", _, key, _, value]) do
    Agent.update(:redis_storage, fn map ->
      Map.put(map, key, value)
    end)

    "+OK\r\n"
  end

  defp handle_command([_, _, "get", _, key]) do
    value =
      Agent.get(:redis_storage, fn map ->
        Map.get(map, key)
      end)

    case value do
      nil -> "$-1\r\n"
      value -> "$#{String.length(value)}\r\n#{value}\r\n"
    end
  end

  defp handle_command([_, _, "echo", _, message]),
    do: "$#{String.length(message)}\r\n#{message}\r\n"

  defp handle_command([_, _, "ping"]), do: "+PONG\r\n"
  defp handle_command([_, _, "ping", _, pong]), do: "$#{String.length(pong)}\r\n#{pong}\r\n"

  defp handle_command(cmd) do
    cmd
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
