defmodule Commands.Lpop do
  def handle_lpop([_, key, _, num]) do
    num = String.to_integer(num)

    case Storage.get_stored(key) do
      nil ->
        Utils.return_nil()

      {value, _} ->
        if is_list(value) do
          rest = Enum.drop(value, num)
          Storage.add_to_store({key, {rest, nil}})

          elements = Enum.slice(value, 0..(num - 1))

          result =
            Enum.reduce(elements, "", fn value, acc ->
              acc <> "$#{String.length(value)}\r\n#{value}\r\n"
            end)

          "*#{length(elements)}\r\n" <> result
        else
          Utils.return_nil()
        end
    end
  end

  def handle_lpop([_, key]) do
    case Storage.get_stored(key) do
      nil ->
        Utils.return_nil()

      {value, _} ->
        if is_list(value) do
          [head | rest] = value
          Storage.add_to_store({key, {rest, nil}})
          "$#{String.length(head)}\r\n#{head}\r\n"
        else
          Utils.return_nil()
        end
    end
  end
end
