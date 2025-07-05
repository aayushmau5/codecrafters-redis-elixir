defmodule Server do
  @moduledoc """
  Your implementation of a Redis server
  """

  use Application

  def start(_type, _args) do
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
        dbg(data)
        response = handle_data(data)
        :gen_tcp.send(client, response)

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

    handle_command(command)
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
