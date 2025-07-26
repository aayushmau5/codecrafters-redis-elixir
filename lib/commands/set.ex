defmodule Commands.Set do
  def handle_set([_, key, _, value | rest]) do
    options = handle_set_options(rest, %{})

    value_data =
      case options do
        %{ttl_ms: ttl_ms} ->
          expiry_time = :erlang.system_time(:millisecond) + ttl_ms
          {value, expiry_time}

        _ ->
          {value, nil}
      end

    Storage.add_to_store({key, value_data})
    Utils.return_ok()
  end

  defp handle_set_options(["$" <> _, name, "$" <> _, value | rest] = _options, map) do
    case name do
      "px" ->
        map = Map.put(map, :ttl_ms, String.to_integer(value))
        handle_set_options(rest, map)

      other ->
        dbg("Unhandled option #{other}")
        map
    end
  end

  defp handle_set_options([], map), do: map
end
