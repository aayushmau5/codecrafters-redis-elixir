defmodule Commands.Ping do
  def handle_ping(data) do
    case data do
      ["$" <> _len, pong] ->
        "$#{String.length(pong)}\r\n#{pong}\r\n"

      [] ->
        "+PONG\r\n"
    end
  end
end
