defmodule Commands.Lrange do
  def handle_lrange([_, key, _, start_index, _, end_index]) do
    start_index = String.to_integer(start_index)
    end_index = String.to_integer(end_index)

    case Storage.get_stored(key) do
      nil ->
        "*0\r\n"

      {value, _} ->
        if is_list(value) do
          len = length(value)

          cond do
            start_index > len ->
              "*0\r\n"

            end_index > len ->
              start_index = normalize_index(start_index, len)
              end_index = len - 1

              elements = Enum.slice(value, start_index, end_index - start_index + 1)

              result =
                Enum.reduce(elements, "", fn value, acc ->
                  acc <> "$#{String.length(value)}\r\n#{value}\r\n"
                end)

              "*#{length(elements)}\r\n" <> result

            true ->
              start_index = normalize_index(start_index, len)
              end_index = normalize_index(end_index, len)

              if start_index > end_index do
                "*0\r\n"
              else
                elements = Enum.slice(value, start_index, end_index - start_index + 1)

                result =
                  Enum.reduce(elements, "", fn value, acc ->
                    acc <> "$#{String.length(value)}\r\n#{value}\r\n"
                  end)

                "*#{length(elements)}\r\n" <> result
              end
          end
        else
          "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
        end
    end
  end

  defp normalize_index(idx, len) when idx < 0, do: max(len + idx, 0)
  defp normalize_index(idx, _len), do: idx
end
