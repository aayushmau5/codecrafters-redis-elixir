defmodule Storage do
  def run(path, agent_name) do
    read_rdb_file(path)
    |> parse_rdb()
    |> load_in_agent(agent_name)
  end

  def read_rdb_file(path) do
    # as bitstrings
    case File.read(path) do
      {:ok, data} -> data
      {:error, _} -> ""
    end
  end

  def parse_rdb(data) do
    RDB.Parse.parse(data)
  end

  defp load_in_agent({map, _}, agent_name) do
    data = Map.get(map, :data)
    dbg(data)

    Agent.update(agent_name, fn _ ->
      data
    end)
  end
end

defmodule RDB.Parse do
  def parse(""), do: {%{version: "", metadata: %{}, data: %{}}, <<>>}

  def parse(data) do
    data
    |> handle_header()
    |> handle_metadata()
    |> handle_database()
    |> handle_data()
    |> handle_eof()
  end

  # Header
  def handle_header(<<"REDIS", version::binary-size(4), rest::binary>>) do
    {%{version: version, metadata: %{}, data: %{}}, rest}
  end

  # Metadata
  def handle_metadata({map, <<0xFE, rest::binary>>}), do: {map, rest}

  def handle_metadata({map, <<0xFA, rest::binary>>}) do
    {key, rest} = RDB.StringEncoding.parse_string(rest)
    {value, rest} = RDB.StringEncoding.parse_string(rest)
    meta_map = Map.put(map.metadata, key, value)
    data = {%{map | metadata: meta_map}, rest}
    handle_metadata(data)
  end

  # Database
  def handle_database({map, rest}) do
    {db_index, rest} = RDB.SizeEncoding.parse_size_encoding(rest)

    <<0xFB, rest::binary>> = rest
    {total_kv, rest} = RDB.SizeEncoding.parse_size_encoding(rest)
    {expiry_kv, rest} = RDB.SizeEncoding.parse_size_encoding(rest)

    map =
      map
      |> Map.put(:index, db_index)
      |> Map.put(:total, total_kv)
      |> Map.put(:ttl, expiry_kv)

    {map, rest}
  end

  # Data
  def handle_data({map, <<0xFF, rest::binary>>}), do: {map, rest}

  def handle_data({map, <<0xFD, rest::binary>>}) do
    <<timestamp_seconds::little-32, rest::binary>> = rest
    timestamp_ms = timestamp_seconds * 1000

    parse_key_value_with_expiry(map, rest, timestamp_ms)
    |> handle_data()
  end

  def handle_data({map, <<0xFC, rest::binary>>}) do
    <<timestamp::little-64, rest::binary>> = rest

    parse_key_value_with_expiry(map, rest, timestamp)
    |> handle_data()
  end

  def handle_data({map, rest}) do
    parse_key_value_with_expiry(map, rest, nil)
    |> handle_data()
  end

  def handle_eof({map, <<checksum::binary-size(8), rest::binary>>}) do
    map = Map.put(map, :checksum, checksum)
    {map, rest}
  end

  defp parse_key_value_with_expiry(map, rest, expiry_info) do
    <<0x00, rest::binary>> = rest

    {key, rest} = RDB.StringEncoding.parse_string(rest)
    {value, rest} = RDB.StringEncoding.parse_string(rest)

    value_with_expiry =
      if expiry_info do
        ttl = DateTime.from_unix!(expiry_info, :millisecond)
        %{value: value, ttl: ttl}
      else
        %{value: value}
      end

    data_map = Map.put(map.data, key, value_with_expiry)
    map = %{map | data: data_map}

    {map, rest}
  end
end

defmodule RDB.SizeEncoding do
  import Bitwise

  def parse_size_encoding(<<first_byte, rest::binary>>) do
    first_two_bits = first_byte >>> 6

    IO.puts(
      "Parsing byte: #{inspect(first_byte, base: :hex)}, first_two_bits: #{first_two_bits})"
    )

    case first_two_bits do
      0 ->
        size = first_byte &&& 0b00111111
        {size, rest}

      1 ->
        first_bits = first_byte &&& 0b00111111
        <<second_byte, rest::binary>> = rest

        size = (first_bits <<< 8) + second_byte
        {size, rest}

      2 ->
        <<size::big-32, rest::binary>> = rest
        {size, rest}

      3 ->
        format = first_byte &&& 0b00111111

        case format do
          0 ->
            <<int_value, rest::binary>> = rest
            string_value = Integer.to_string(int_value)
            {byte_size(string_value), <<string_value::binary, rest::binary>>}

          2 ->
            <<first, second, third, fourth, rest::binary>> = rest
            value = (first <<< 24) + (second <<< 16) + (third <<< 8) + fourth
            string_value = Integer.to_string(value)
            {byte_size(string_value), <<string_value::binary, rest::binary>>}

          _ ->
            {:error, "integer format not implemented: #{format}"}
        end

      value ->
        dbg(value)
        {:error, "not implemented yet"}
    end
  end
end

defmodule RDB.StringEncoding do
  def parse_string(data) do
    {size, rest} = RDB.SizeEncoding.parse_size_encoding(data)

    <<string::binary-size(size), remaining::binary>> = rest

    {string, remaining}
  end
end
