defmodule Commands.Exec do
  def handle_exec(_data) do
    case Process.get(:in_transaction) do
      true ->
        queued_commands = Process.get(:queued_commands)
        dbg(queued_commands)

        commands_results =
          Enum.map(Enum.reverse(queued_commands), fn {command, data} ->
            Commands.execute(command, data)
          end)

        Process.delete(:in_transaction)
        Process.delete(:queued_commands)
        "*#{length(commands_results)}\r\n" <> Enum.join(commands_results)

      nil ->
        "-ERR EXEC without MULTI\r\n"
    end
  end
end
