defmodule Commands.Echo do
  def handle_echo([_, message]) do
    "$#{String.length(message)}\r\n#{message}\r\n"
  end
end
