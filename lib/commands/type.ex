defmodule Commands.Type do
  @stream_table :stream_storage

  def handle_type([_, key]) do
    case Storage.get_stored(key) do
      nil ->
        case :ets.match_object(@stream_table, {{key, :_}, :_}) do
          [] ->
            "+none\r\n"

          _ ->
            "+stream\r\n"
        end

      _ ->
        "+string\r\n"
    end
  end
end
