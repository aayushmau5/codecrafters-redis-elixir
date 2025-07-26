defmodule Commands.Blpop do
  def handle_blpop([_, key, _, timeout]) do
    case Storage.get_stored(key) do
      match when match in [nil, {[], nil}] ->
        # Handle both int and float
        {:ok, timeout} =
          case Integer.parse(timeout) do
            {int, ""} ->
              {:ok, int}

            _ ->
              case Float.parse(timeout) do
                {float, ""} -> {:ok, float}
                _ -> {:error, :invalid_number}
              end
          end

        {:ok, pid} =
          BlockPop.start_link(timeout_ms: round(timeout * 1000), key: key)

        case BlockPop.wait_for_push(pid) do
          {:ok, _} ->
            {[element | rest], _} = Storage.get_stored(key)
            Storage.add_to_store({key, {rest, nil}})

            "*2\r\n$#{String.length(key)}\r\n#{key}\r\n$#{String.length(element)}\r\n#{element}\r\n"

          {:error, :nopush} ->
            Utils.return_nil()
        end

      {[element | rest], _} ->
        # Item available immediately, no need to block
        Storage.add_to_store({key, {rest, nil}})
        "*2\r\n$#{String.length(key)}\r\n#{key}\r\n$#{String.length(element)}\r\n#{element}\r\n"
    end
  end
end
