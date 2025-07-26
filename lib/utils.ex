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

  # ID
  def id_to_tuple(id) when is_binary(id) do
    String.split(id, "-")
    |> then(fn [last_ms, last_offset] ->
      {String.to_integer(last_ms), String.to_integer(last_offset)}
    end)
  end

  # Common responses
  def return_nil(), do: "$-1\r\n"
  def return_ok(), do: "+OK\r\n"

  # Replica
  def master?(), do: !replica?()

  def replica?() do
    Storage.get_config(:is_replica)
  end

  # List
  def get_list_elements(elements) do
    Enum.chunk_every(elements, 2)
    |> Enum.map(fn [_, element] -> element end)
  end
end
