defmodule Commands.Incr do
  def handle_incr([_, key]) do
    case Storage.get_stored(key) do
      nil ->
        Storage.add_to_store({key, {"#{1}", nil}})
        ":1\r\n"

      {value, extra} ->
        case Integer.parse(value) do
          {value, ""} ->
            value = value + 1
            Storage.add_to_store({key, {"#{value}", extra}})
            ":#{value}\r\n"

          :error ->
            "-ERR value is not an integer or out of range\r\n"
        end
    end
  end
end
