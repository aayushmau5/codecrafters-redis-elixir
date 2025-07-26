defmodule Commands.Xrange do
  @stream_table :stream_storage

  def handle_xrange([_, stream_key, _, "-", _, "+"]) do
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

  def handle_xrange([_, stream_key, _, "-", _, end_id]) do
    {end_ms, end_offset, end_offset_specified?} =
      if String.contains?(end_id, "-") do
        {ms, offset} = Utils.id_to_tuple(end_id)
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

  def handle_xrange([_, stream_key, _, start_id, _, "+"]) do
    {start_ms, start_offset} =
      if String.contains?(start_id, "-"),
        do: Utils.id_to_tuple(start_id),
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

  def handle_xrange([_, stream_key, _, start_id, _, end_id]) do
    {start_ms, start_offset} =
      if String.contains?(start_id, "-"),
        do: Utils.id_to_tuple(start_id),
        else: {String.to_integer(start_id), 0}

    {end_ms, end_offset, end_offset_specified?} =
      if String.contains?(end_id, "-") do
        {ms, offset} = Utils.id_to_tuple(end_id)
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
      match when match in [:"$end_of_table", []] -> Utils.return_nil()
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
end
