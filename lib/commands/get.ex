defmodule Commands.Get do
  def handle_get([_, key]) do
    case Storage.get_stored(key) do
      nil ->
        Utils.return_nil()

      {value, nil} ->
        "$#{String.length(value)}\r\n#{value}\r\n"

      {value, expiry_time} ->
        current_time = :erlang.system_time(:millisecond)

        if current_time < expiry_time do
          "$#{String.length(value)}\r\n#{value}\r\n"
        else
          Storage.delete_key(key)
          Utils.return_nil()
        end
    end
  end
end
