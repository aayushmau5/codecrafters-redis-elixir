defmodule Commands.Discard do
  def handle_discard(_) do
    if Process.get(:in_transaction) do
      Process.delete(:in_transaction)
      Process.delete(:queued_commands)
      Utils.return_ok()
    else
      "-ERR DISCARD without MULTI\r\n"
    end
  end
end
