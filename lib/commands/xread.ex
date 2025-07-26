defmodule Commands.Xread do
  @stream_table :stream_storage

  def handle_xread([_, "block", _, block_time, _, "streams", _, stream_key, _, "$"]) do
    {:ok, pid} =
      Block.start_link(timeout_ms: String.to_integer(block_time), stream_key: stream_key)

    response =
      case Block.wait_for_stream(pid) do
        {:ok, {_, new_id}} ->
          case get_stream_match(stream_key, new_id) do
            match when match in [:"$end_of_table", []] -> Utils.return_nil()
            match -> encode_xread_result(stream_key, match)
          end

        {:error, :nostream} ->
          Utils.return_nil()
      end

    Storage.delete_config(:current_block_pid)

    response
  end

  # Infinite block time
  def handle_xread([_, "block", _, "0", _, "streams", _, stream_key, _, id]) do
    case get_stream_matches(stream_key, id) do
      match when match in [:"$end_of_table", []] ->
        {:ok, pid} =
          Block.start_link(
            timeout_ms: 0,
            stream_key: stream_key,
            id: id
          )

        case Block.wait_for_stream(pid) do
          {:ok, _} ->
            Storage.delete_config(:current_block_pid)
            handle_xread([nil, "streams", nil, stream_key, nil, id])

          {:error, :nostream} ->
            Storage.delete_config(:current_block_pid)
            Utils.return_nil()
        end

      match ->
        encode_xread_result(stream_key, match)
    end
  end

  def handle_xread([_, "block", _, block_time, _, "streams", _, stream_key, _, id]) do
    {:ok, pid} =
      Block.start_link(timeout_ms: String.to_integer(block_time), stream_key: stream_key, id: id)

    case Block.wait_for_stream(pid) do
      {:ok, _} ->
        Storage.delete_config(:current_block_pid)
        handle_xread([nil, "streams", nil, stream_key, nil, id])

      {:error, :nostream} ->
        Storage.delete_config(:current_block_pid)
        Utils.return_nil()
    end
  end

  def handle_xread([
        _,
        "streams",
        _,
        first_stream_key,
        _,
        second_stream_key,
        _,
        first_id,
        _,
        second_id
      ]) do
    stream_configs = [
      {first_stream_key, first_id},
      {second_stream_key, second_id}
    ]

    stream_results =
      Enum.map(stream_configs, fn {stream_key, id} ->
        {stream_key, get_stream_matches(stream_key, id)}
      end)

    encode_multi_stream_result(stream_results)
  end

  def handle_xread([_, "streams", _, stream_key, _, id]) do
    case get_stream_matches(stream_key, id) do
      match when match in [:"$end_of_table", []] -> Utils.return_nil()
      matches -> encode_xread_result(stream_key, matches)
    end
  end

  defp get_stream_matches(stream_key, id) do
    {ms, offset} = Utils.id_to_tuple(id)

    :ets.select(@stream_table, [
      {
        {{stream_key, {:"$1", :"$2"}}, :"$3"},
        [
          {
            :orelse,
            {:>, :"$1", ms},
            {:>, :"$2", offset}
          }
        ],
        [:"$$"]
      }
    ])
  end

  defp get_stream_match(stream_key, id) do
    {ms, offset} = Utils.id_to_tuple(id)

    :ets.select(@stream_table, [
      {
        {{stream_key, {:"$1", :"$2"}}, :"$3"},
        [
          {
            :andalso,
            {:==, :"$1", ms},
            {:==, :"$2", offset}
          }
        ],
        [:"$$"]
      }
    ])
  end

  defp encode_xread_result(stream_key, matches) do
    "*1\r\n" <> encode_single_stream_for_multi(stream_key, matches)
  end

  defp encode_multi_stream_result(stream_results) do
    "*2\r\n" <>
      Enum.map_join(stream_results, "", fn {stream_key, matches} ->
        encode_single_stream_for_multi(stream_key, matches)
      end)
  end

  defp encode_single_stream_for_multi(stream_key, matches) do
    list_size = length(matches)
    encoded_stream_key = "$#{String.length(stream_key)}\r\n#{stream_key}"
    stream_header = "*2\r\n#{encoded_stream_key}\r\n*#{list_size}\r\n"

    matches_encoded =
      Enum.map_join(matches, "", fn [ms, offset, kv] ->
        encode_stream_entry(ms, offset, kv)
      end)

    stream_header <> matches_encoded
  end

  defp encode_stream_entry(ms, offset, kv) do
    id = "#{ms}-#{offset}"
    kv_list = kv |> Enum.map(&Tuple.to_list/1) |> List.flatten()

    encoded_kv =
      Enum.reduce(kv_list, "*#{length(kv_list)}\r\n", fn x, acc ->
        acc <> "$#{String.length(x)}\r\n#{x}\r\n"
      end)

    "*2\r\n$#{String.length(id)}\r\n#{id}\r\n" <> encoded_kv
  end
end
