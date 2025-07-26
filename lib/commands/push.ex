defmodule Commands.Push do
  def handle_lpush([_, key | elements]) do
    elements = Utils.get_list_elements(elements)

    case Storage.get_stored(key) do
      nil ->
        Storage.add_to_store({key, {Enum.reverse(elements), nil}})
        notify_waiters(key)
        ":#{length(elements)}\r\n"

      {value, _} ->
        if is_list(value) do
          new_list = Enum.concat(Enum.reverse(elements), value)
          Storage.add_to_store({key, {new_list, nil}})
          notify_waiters(key)
          ":#{length(new_list)}\r\n"
        else
          "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
        end
    end
  end

  def handle_rpush([_, key | elements]) do
    elements = Utils.get_list_elements(elements)

    case Storage.get_stored(key) do
      nil ->
        Storage.add_to_store({key, {elements, nil}})
        dbg("notifying waiters")
        notify_waiters(key)
        ":#{length(elements)}\r\n"

      {value, _} ->
        if is_list(value) do
          new_list = Enum.concat(value, elements)
          Storage.add_to_store({key, {new_list, nil}})
          dbg("notifying waiters")
          notify_waiters(key)
          ":#{length(new_list)}\r\n"
        else
          "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
        end
    end
  end

  defp notify_waiters(key) do
    BlockPop.notify_push(key)
  end
end
