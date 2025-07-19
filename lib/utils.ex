defmodule Utils do
  def split_data(data) do
    data
    |> String.trim()
    |> String.split("\r\n")
  end

  def separate_commands([], commands), do: Enum.reverse(commands)

  def separate_commands(data, commands) do
    case data do
      ["*" <> len_str = prefix | rest] when len_str != "" ->
        len = String.to_integer(len_str)

        # Take len * 2 elements (length + value pairs)
        command_body = Enum.take(rest, len * 2)
        command = [prefix | command_body]
        rest = Enum.drop(rest, len * 2)
        separate_commands(rest, [command | commands])

      [_other | rest] ->
        separate_commands(rest, commands)
    end
  end
end
