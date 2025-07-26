defmodule Commands.Multi do
  def handle_multi(_) do
    # Using process dictionary to store transaction details and queued up commands
    Process.put(:in_transaction, true)
    Process.put(:queued_commands, [])
    Utils.return_ok()
  end
end
