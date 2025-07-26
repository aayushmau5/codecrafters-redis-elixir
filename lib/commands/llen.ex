defmodule Commands.Llen do
  def handle_llen([_, key]) do
    case Storage.get_stored(key) do
      nil ->
        ":0\r\n"

      {value, _} ->
        if is_list(value) do
          ":#{length(value)}\r\n"
        else
          ":0\r\n"
        end
    end
  end
end
