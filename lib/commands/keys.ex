defmodule Commands.Keys do
  def handle_key([_, "*"]) do
    keys = Storage.match_keys()
    key_list = List.flatten(keys)

    Enum.reduce(key_list, "*#{length(keys)}\r\n", fn value, acc ->
      acc <> "$#{String.length(value)}\r\n#{value}\r\n"
    end)
  end

  def handle_key([_, key]) do
    if Storage.get_stored(key) != nil do
      "*1\r\n$#{String.length(key)}\r\n#{key}\r\n"
    else
      Utils.return_nil()
    end
  end
end
