defmodule Server do
  @moduledoc """
  Your implementation of a Redis server
  """

  @config_table :config
  @storage_table :redis_storage
  @stream_table :stream_storage
  @replicas_table :replicas

  use Application

  def start(_type, _args) do
    args = parse_arguments()
    dir = Keyword.get(args, :dir)
    dbfilename = Keyword.get(args, :dbfilename)
    port = Keyword.get(args, :port, 6379)
    replica = Keyword.get(args, :replicaof)

    :ets.new(@config_table, [
      :set,
      :public,
      :named_table,
      read_concurrency: true,
      write_concurrency: true
    ])

    :ets.insert(@config_table, {:port, port})
    :ets.insert(@config_table, {:offset, 0})
    :ets.insert(@config_table, {:pending_writes, false})

    :ets.new(@storage_table, [
      :set,
      :public,
      :named_table,
      read_concurrency: true,
      write_concurrency: true
    ])

    :ets.new(@stream_table, [
      :ordered_set,
      :public,
      :named_table,
      read_concurrency: true,
      write_concurrency: true
    ])

    :ets.new(@replicas_table, [
      :bag,
      :public,
      :named_table,
      read_concurrency: true,
      write_concurrency: true
    ])

    if dir != nil and dbfilename != nil do
      # db config
      :ets.insert(@config_table, {:dir, dir})
      :ets.insert(@config_table, {:dbfilename, dbfilename})

      rdb_file_path = Path.join(dir, dbfilename)
      :ets.insert(@config_table, {:dbfilepath, rdb_file_path})

      Storage.run(rdb_file_path)
    end

    if replica do
      [host, port] = String.split(replica, " ")
      port = String.to_integer(port)
      :ets.insert(@config_table, {:is_replica, true})
      :ets.insert(@config_table, {:master_host, host})
      :ets.insert(@config_table, {:master_port, port})
    else
      master_repl_id = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
      master_repl_offset = 0
      :ets.insert(@config_table, {:master_repl_id, master_repl_id})
      :ets.insert(@config_table, {:master_repl_offset, master_repl_offset})
    end

    Supervisor.start_link([{Task, fn -> Server.listen() end}], strategy: :one_for_one)
  end

  @doc """
  Listen for incoming connections
  """
  def listen() do
    [port: port] = :ets.lookup(@config_table, :port)

    # When replica, perform handshake with master and kickoff syncing process
    if replica?() do
      [master_host: master_host] = :ets.lookup(@config_table, :master_host)
      [master_port: master_port] = :ets.lookup(@config_table, :master_port)

      Supervisor.start_link(
        [
          {
            ReplicaConnection,
            [host: master_host, port: master_port, name: ReplicaConnectionOne]
          }
        ],
        strategy: :one_for_one
      )

      ReplicaConnection.initialize(ReplicaConnectionOne)
      ReplicaConnection.run_handler(ReplicaConnectionOne)
    end

    # Since the tester restarts your program quite often, setting SO_REUSEADDR
    # ensures that we don't run into 'Address already in use' errors
    {:ok, socket} = :gen_tcp.listen(port, [:binary, active: false, reuseaddr: true])
    loop_accept(socket)
  end

  defp loop_accept(socket) do
    case :gen_tcp.accept(socket) do
      {:ok, client} ->
        Task.start(fn -> handle_socket(client) end)
        loop_accept(socket)

      {:error, reason} ->
        IO.puts("Error accepting connection: #{reason}")
        :gen_tcp.close(socket)
    end
  end

  defp handle_socket(client) do
    case :gen_tcp.recv(client, 0, 5000) do
      {:ok, binary_data} ->
        dbg("RECEIVED ON MAIN THREAD: #{binary_data}")
        data = Utils.split_data(binary_data)
        commands = Utils.separate_commands(data, [])

        Enum.each(commands, fn command_body ->
          command_binary = reconstruct_binary_command(command_body)
          {command, rest} = get_command(command_body)

          is_replica_connection = is_replica_connection?(command)

          if is_replica_connection do
            :ets.insert(@replicas_table, {:client, client})
          end

          response = handle_command(command, rest) |> dbg()
          :gen_tcp.send(client, response) |> dbg()

          update_offset(command, command_binary)
          propagate(command, command_binary)
        end)

        handle_socket(client)

      {:error, :timeout} ->
        # For replica connections, timeout is normal - just continue listening
        # Check if this is a replica connection
        is_replica = :ets.match(@replicas_table, {:client, client}) != []
        if is_replica, do: handle_socket(client)

      {:error, reason} ->
        IO.puts("Error receiving data: #{reason}")
    end
  end

  def get_command([_prefix, _length, command | rest]) do
    {String.downcase(command), rest}
  end

  def handle_command(command, data) do
    case command do
      "xadd" -> handle_xadd(data)
      "xrange" -> handle_xrange(data)
      "replconf" -> handle_repl_conf(data)
      "psync" -> handle_psync(data)
      "info" -> handle_info(data)
      "wait" -> handle_wait(data)
      "config" -> handle_config(data)
      "keys" -> handle_key(data)
      "type" -> handle_type(data)
      "set" -> handle_set(data)
      "get" -> handle_get(data)
      "echo" -> handle_echo(data)
      "ping" -> handle_ping(data)
    end
  end

  # XADD(streams)
  defp handle_xadd([_, stream_key, _, "*" | rest]) do
    prefix_id = DateTime.utc_now() |> DateTime.to_unix(:millisecond)
    handle_xadd([nil, stream_key, nil, "#{prefix_id}-*" | rest])
  end

  defp handle_xadd([_, _, _, "0-0" | _rest]),
    do: "-ERR The ID specified in XADD must be greater than 0-0\r\n"

  defp handle_xadd([_, stream_key, _, id | rest]) do
    case String.split(id, "-") do
      # "something-*"
      [ms, "*"] ->
        ms = String.to_integer(ms)

        # :ets.select takes in match spec as [{pattern, guard, result}]
        # Here, the pattern is { {stream_key, {:"$1", :"$2"}}, :"$3" }
        # guard: []
        # result: [:"$$"] which lets your return every parameters
        matches =
          :ets.select_reverse(
            @stream_table,
            [{{{stream_key, {ms, :"$1"}}, :"$2"}, [], [:"$$"]}],
            1
          )

        case matches do
          match when match in [:"$end_of_table", []] ->
            # 0-0 already exists, so start 0-1
            id = if ms == 0, do: "0-1", else: "#{ms}-0"
            insert_stream(stream_key, id, rest)

          {[last_entry], _} ->
            # [1, %{"temperature" => "69"}]
            [last_offset, _] = last_entry
            insert_stream(stream_key, "#{ms}-#{last_offset + 1}", rest)
        end

      # "something-something"
      _ ->
        matches =
          :ets.select_reverse(
            @stream_table,
            [{{{stream_key, :"$1"}, :"$2"}, [], [:"$$"]}],
            1
          )

        case matches do
          match when match in [:"$end_of_table", []] ->
            insert_stream(stream_key, id, rest)

          {[last_entry], _} ->
            [last_entry_id, _] = last_entry

            if valid_id?(id_to_tuple(id), last_entry_id) do
              insert_stream(stream_key, id, rest)
            else
              xadd_id_error()
            end
        end
    end
  end

  defp valid_id?({ms, offset}, {last_ms, last_offset}) do
    cond do
      ms > last_ms -> true
      ms == last_ms and offset > last_offset -> true
      true -> false
    end
  end

  defp insert_stream(stream_key, id, rest) do
    kv = get_kv_map(rest)
    id_tuple = id_to_tuple(id)
    # {{stream_key, {id_num, offset_num}}, kv}
    # key -> stream_key, {id_num, offset}
    :ets.insert(@stream_table, {{stream_key, id_tuple}, kv})

    "$#{String.length(id)}\r\n#{id}\r\n"
  end

  defp id_to_tuple(id) when is_binary(id) do
    String.split(id, "-")
    |> then(fn [last_ms, last_offset] ->
      {String.to_integer(last_ms), String.to_integer(last_offset)}
    end)
  end

  defp xadd_id_error(),
    do: "-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n"

  defp get_kv_map(rest) do
    Enum.chunk_every(rest, 4)
    |> Enum.reduce(%{}, fn [_, key, _, value], map ->
      Map.put(map, key, value)
    end)
  end

  # XRANGE
  defp handle_xrange([_, stream_key, _, "-", _, "+"]) do
    matches =
      :ets.select(
        @stream_table,
        [
          {
            {{stream_key, {:"$1", :"$2"}}, :"$3"},
            [],
            [:"$$"]
          }
        ]
      )

    handle_xrange_match(matches)
  end

  defp handle_xrange([_, stream_key, _, "-", _, end_id]) do
    {end_ms, end_offset, end_offset_specified?} =
      if String.contains?(end_id, "-") do
        {ms, offset} = id_to_tuple(end_id)
        {ms, offset, true}
      else
        {String.to_integer(end_id), 0, false}
      end

    # Build end condition based on whether offset was specified
    end_condition =
      if end_offset_specified? do
        # Include entries <= end_ms-end_offset
        {:orelse, {:<, :"$1", end_ms},
         {:andalso, {:==, :"$1", end_ms}, {:"=<", :"$2", end_offset}}}
      else
        # Include all entries <= end_ms (any offset)
        {:"=<", :"$1", end_ms}
      end

    matches =
      :ets.select(
        @stream_table,
        [
          {
            {{stream_key, {:"$1", :"$2"}}, :"$3"},
            [end_condition],
            [:"$$"]
          }
        ]
      )

    handle_xrange_match(matches)
  end

  defp handle_xrange([_, stream_key, _, start_id, _, "+"]) do
    {start_ms, start_offset} =
      if String.contains?(start_id, "-"),
        do: id_to_tuple(start_id),
        else: {String.to_integer(start_id), 0}

    matches =
      :ets.select(
        @stream_table,
        [
          {
            {{stream_key, {:"$1", :"$2"}}, :"$3"},
            [
              {:orelse, {:>, :"$1", start_ms},
               {:andalso, {:==, :"$1", start_ms}, {:>=, :"$2", start_offset}}}
            ],
            [:"$$"]
          }
        ]
      )

    handle_xrange_match(matches)
  end

  defp handle_xrange([_, stream_key, _, start_id, _, end_id]) do
    {start_ms, start_offset} =
      if String.contains?(start_id, "-"),
        do: id_to_tuple(start_id),
        else: {String.to_integer(start_id), 0}

    {end_ms, end_offset, end_offset_specified?} =
      if String.contains?(end_id, "-") do
        {ms, offset} = id_to_tuple(end_id)
        {ms, offset, true}
      else
        {String.to_integer(end_id), 0, false}
      end

    # Build end condition based on whether offset was specified
    end_condition =
      if end_offset_specified? do
        # Include entries <= end_ms-end_offset
        {:orelse, {:<, :"$1", end_ms},
         {:andalso, {:==, :"$1", end_ms}, {:"=<", :"$2", end_offset}}}
      else
        # Include all entries <= end_ms (any offset)
        {:"=<", :"$1", end_ms}
      end

    matches =
      :ets.select(
        @stream_table,
        [
          {
            {{stream_key, {:"$1", :"$2"}}, :"$3"},
            # Filter entries within [start_id, end_id] (inclusive)
            # Since Redis IDs are {ms}-{offset}, we need to check both components:
            # 1. entry_id >= start_id: (ms > start_ms) OR (ms == start_ms AND offset >= start_offset)
            # 2. entry_id <= end_id: (ms < end_ms) OR (ms == end_ms AND offset <= end_offset)
            [
              {
                :andalso,
                {:orelse, {:>, :"$1", start_ms},
                 {:andalso, {:==, :"$1", start_ms}, {:>=, :"$2", start_offset}}},
                end_condition
              }
            ],
            [:"$$"]
          }
        ]
      )

    handle_xrange_match(matches)
  end

  defp handle_xrange_match(matches) do
    case matches do
      match when match in [:"$end_of_table", []] -> return_nil()
      _ -> encode_xrange_result(matches)
    end
  end

  defp encode_xrange_result(list) do
    init = "*#{length(list)}\r\n"

    Enum.reduce(list, init, fn entry, acc ->
      [ms, offset, kv] = entry
      kv_size = map_size(kv) * 2
      id = "#{ms}-#{offset}"

      encoded_map =
        Map.to_list(kv)
        |> Enum.reduce("", fn {key, value}, acc ->
          acc <> "$#{String.length(key)}\r\n#{key}\r\n$#{String.length(value)}\r\n#{value}\r\n"
        end)

      acc <>
        "*2\r\n" <>
        "$#{String.length(id)}\r\n#{id}\r\n" <>
        "*#{kv_size}\r\n" <>
        encoded_map
    end)
  end

  # REPLCONF
  defp handle_repl_conf([_, "listening-port", _, _port]), do: return_ok()
  defp handle_repl_conf([_, "capa", _, "psync2"]), do: return_ok()

  defp handle_repl_conf([_, "GETACK", _, "*"]) do
    [offset: offset] = :ets.lookup(@config_table, :offset)
    offset_length = String.length("#{offset}")
    "*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$#{offset_length}\r\n#{offset}\r\n"
  end

  defp handle_repl_conf([_, "ACK", _, _]) do
    # Notify the current wait process if one exists
    case :ets.lookup(@config_table, :current_wait_pid) do
      [{:current_wait_pid, wait_pid}] when is_pid(wait_pid) ->
        if Process.alive?(wait_pid) do
          Wait.notify_ack(wait_pid)
        end

      _ ->
        :ok
    end

    ""
  end

  # PYSNC
  defp handle_psync([_, "?", _, "-1"]) do
    [master_repl_id: master_repl_id] = :ets.lookup(@config_table, :master_repl_id)

    db_file_path =
      case :ets.lookup(@config_table, :dbfilepath) do
        [dbfilepath: dbfilepath] -> dbfilepath
        [] -> "empty.rdb"
      end

    empty_rdb_bytes = File.read!(db_file_path)

    "+FULLRESYNC #{master_repl_id} 0\r\n$#{IO.iodata_length(empty_rdb_bytes)}\r\n#{empty_rdb_bytes}"
  end

  # INFO replication
  defp handle_info([_, key]) do
    case key do
      "replication" ->
        case :ets.lookup(@config_table, :is_replica) do
          [] ->
            [master_repl_id: master_repl_id] = :ets.lookup(@config_table, :master_repl_id)

            [master_repl_offset: master_repl_offset] =
              :ets.lookup(@config_table, :master_repl_offset)

            master_repl_id = "master_replid:#{master_repl_id}"
            master_repl_offset = "master_repl_offset:#{master_repl_offset}"

            data = """
            role:master
            #{master_repl_id}
            #{master_repl_offset}
            """

            "$#{String.length(data)}\r\n#{data}\r\n"

          [is_replica: true] ->
            "$10\r\nrole:slave\r\n"
        end

      _ ->
        return_nil()
    end
  end

  # WAIT command
  defp handle_wait([_, num_replicas, _, wait_time]) do
    required_replicas = String.to_integer(num_replicas)
    wait_time_ms = String.to_integer(wait_time)

    # Check if there are pending writes since the last WAIT
    case :ets.lookup(@config_table, :pending_writes) do
      [{:pending_writes, false}] ->
        # No pending writes, return number of replicas immediately
        replicas = :ets.match(@replicas_table, {:client, :"$1"}) |> List.flatten()
        replica_count = length(replicas)
        ":#{replica_count}\r\n"

      [{:pending_writes, true}] ->
        # There are pending writes, send GETACK and wait for responses
        {:ok, wait_pid} =
          Wait.start_link(required_replicas: required_replicas, timeout_ms: wait_time_ms)

        ack_count = Wait.wait_for_replicas(wait_pid) |> dbg()

        # Clean up the wait process reference and reset pending writes
        :ets.delete(@config_table, :current_wait_pid)
        :ets.insert(@config_table, {:pending_writes, false})

        ":#{ack_count}\r\n"
    end
  end

  # CONFIG
  defp handle_config([_, command | rest]) do
    case String.downcase(command) do
      "get" -> config_get(rest)
    end
  end

  defp config_get([_, conf]) do
    case String.downcase(conf) do
      "dir" ->
        [dir: dir] = :ets.lookup(@config_table, :dir)
        "*2\r\n$3\r\ndir\r\n$#{String.length(dir)}\r\n#{dir}\r\n"

      "dbfilename" ->
        [dbfilename: name] = :ets.lookup(@config_table, :dbfilename)
        "*2\r\n$10\r\ndbfilename\r\n$#{String.length(name)}\r\n#{name}\r\n"
    end
  end

  # KEYS
  defp handle_key([_, "*"]) do
    keys = :ets.match(@storage_table, {:"$1", :_})
    key_list = List.flatten(keys)

    Enum.reduce(key_list, "*#{length(keys)}\r\n", fn value, acc ->
      acc <> "$#{String.length(value)}\r\n#{value}\r\n"
    end)
  end

  defp handle_key([_, key]) do
    if :ets.lookup(@storage_table, key) != [] do
      "*1\r\n$#{String.length(key)}\r\n#{key}\r\n"
    else
      return_nil()
    end
  end

  # TYPE
  defp handle_type([_, key]) do
    case :ets.lookup(@storage_table, key) do
      [] ->
        case :ets.match_object(@stream_table, {{key, :_}, :_}) do
          [] ->
            "+none\r\n"

          _ ->
            "+stream\r\n"
        end

      [{^key, {_value, _}}] ->
        "+string\r\n"
    end
  end

  # ["$3", "foo", "$4", "bar"]
  # ["$3", "foo", "$4", "bar", "$2", "px", "$3", "100"]
  defp handle_set([_, key, _, value | rest]) do
    options = handle_set_options(rest, %{})

    value_data =
      case options do
        %{ttl_ms: ttl_ms} ->
          expiry_time = :erlang.system_time(:millisecond) + ttl_ms
          {value, expiry_time}

        _ ->
          {value, nil}
      end

    :ets.insert(@storage_table, {key, value_data})
    return_ok()
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

  defp handle_get([_, key]) do
    case :ets.lookup(@storage_table, key) do
      [] ->
        return_nil()

      [{^key, {value, nil}}] ->
        "$#{String.length(value)}\r\n#{value}\r\n"

      [{^key, {value, expiry_time}}] ->
        current_time = :erlang.system_time(:millisecond)

        if current_time < expiry_time do
          "$#{String.length(value)}\r\n#{value}\r\n"
        else
          :ets.delete(@storage_table, key)
          return_nil()
        end
    end
  end

  defp handle_echo([_, message]) do
    "$#{String.length(message)}\r\n#{message}\r\n"
  end

  defp handle_ping(data) do
    case data do
      ["$" <> _len, pong] ->
        "$#{String.length(pong)}\r\n#{pong}\r\n"

      [] ->
        "+PONG\r\n"
    end
  end

  defp return_nil(), do: "$-1\r\n"
  defp return_ok(), do: "+OK\r\n"

  defp master?(), do: !replica?()

  defp replica?() do
    case :ets.lookup(@config_table, :is_replica) do
      [] -> false
      [is_replica: true] -> true
    end
  end

  def reconstruct_binary_command(command_body) do
    Enum.join(command_body, "\r\n") <> "\r\n"
  end

  defp needs_propagation?(command), do: Enum.member?(["set"], command)

  # Handle what kind of commands we need to send over
  defp propagate(command, command_binary) do
    if master?() and needs_propagation?(command) do
      # Mark that we have pending writes
      :ets.insert(@config_table, {:pending_writes, true})

      clients = :ets.match(@replicas_table, {:client, :"$1"})

      clients
      |> List.flatten()
      |> Task.async_stream(
        fn client ->
          :gen_tcp.send(client, command_binary)
        end,
        max_concurrency: 10,
        timeout: 1000
      )
      |> Stream.run()
    end
  end

  def needs_offset_update?(command), do: command in ["ping", "set", "del", "replconf"]

  def update_offset(command, command_binary) do
    if needs_offset_update?(command),
      do: :ets.update_counter(@config_table, :offset, byte_size(command_binary))
  end

  defp parse_arguments() do
    {options, _, _} =
      OptionParser.parse(System.argv(),
        strict: [dir: :string, dbfilename: :string, port: :integer, replicaof: :string]
      )

    options
  end

  defp is_replica_connection?(command), do: master?() and command == "psync"
end

defmodule CLI do
  def main(_args) do
    # Start the Server application
    {:ok, _pid} = Application.ensure_all_started(:codecrafters_redis)

    # Run forever
    Process.sleep(:infinity)
  end
end
