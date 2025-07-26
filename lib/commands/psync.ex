defmodule Commands.Psync do
  def handle_psync([_, "?", _, "-1"]) do
    master_repl_id = Storage.get_config(:master_repl_id)

    db_file_path =
      case Storage.get_config(:dbfilepath) do
        nil -> "empty.rdb"
        dbfilepath -> dbfilepath
      end

    empty_rdb_bytes = File.read!(db_file_path)

    "+FULLRESYNC #{master_repl_id} 0\r\n$#{IO.iodata_length(empty_rdb_bytes)}\r\n#{empty_rdb_bytes}"
  end
end
