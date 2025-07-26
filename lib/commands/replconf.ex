defmodule Commands.Replconf do
  def handle_repl_conf([_, "listening-port", _, _port]), do: Utils.return_ok()
  def handle_repl_conf([_, "capa", _, "psync2"]), do: Utils.return_ok()

  def handle_repl_conf([_, "GETACK", _, "*"]) do
    offset = Storage.get_config(:offset)
    offset_length = String.length("#{offset}")
    "*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$#{offset_length}\r\n#{offset}\r\n"
  end

  def handle_repl_conf([_, "ACK", _, _]) do
    # Notify the current wait process if one exists
    case Storage.get_config(:current_wait_pid) do
      nil ->
        :ok

      wait_pid when is_pid(wait_pid) ->
        if Process.alive?(wait_pid) do
          Wait.notify_ack(wait_pid)
        end
    end

    ""
  end
end
