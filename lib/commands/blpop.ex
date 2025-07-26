defmodule Commands.Blpop do
  def handle_blpop([_, key, _, timeout]) do
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

    timeout_ms = round(timeout * 1000)

    case BlockPop.wait_for_element(key, timeout_ms) do
      {:ok, response} ->
        response

      {:error, :timeout} ->
        Utils.return_nil()
    end
  end
end
