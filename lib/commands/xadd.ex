defmodule Commands.Xadd do
  @stream_table :stream_storage

  def handle_xadd([_, stream_key, _, "*" | rest]) do
    prefix_id = DateTime.utc_now() |> DateTime.to_unix(:millisecond)
    handle_xadd([nil, stream_key, nil, "#{prefix_id}-*" | rest])
  end

  def handle_xadd([_, _, _, "0-0" | _rest]),
    do: "-ERR The ID specified in XADD must be greater than 0-0\r\n"

  def handle_xadd([_, stream_key, _, id | rest]) do
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

    case Storage.get_config(:current_block_pid) do
      nil ->
        :ok

      block_pid when is_pid(block_pid) ->
        if Process.alive?(block_pid) do
          Block.add_stream(block_pid, stream_key, id)
        end
    end

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
end
