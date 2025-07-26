defmodule Commands.Info do
  def handle_info([_, key]) do
    case key do
      "replication" ->
        case Storage.get_config(:is_replica) do
          nil ->
            master_repl_id = Storage.get_config(:master_repl_id)
            master_repl_offset = Storage.get_config(:master_repl_offset)
            master_repl_id = "master_replid:#{master_repl_id}"
            master_repl_offset = "master_repl_offset:#{master_repl_offset}"

            data = """
            role:master
            #{master_repl_id}
            #{master_repl_offset}
            """

            "$#{String.length(data)}\r\n#{data}\r\n"

          true ->
            "$10\r\nrole:slave\r\n"
        end

      _ ->
        Utils.return_nil()
    end
  end
end
