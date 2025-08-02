defmodule Commands.Subscribe do
  def handle_subscribe([_, channel_name]) do
    Process.put(:in_subscription, true)
    subscribed_channels = Process.get(:subscribed_channels, [])
    total_channels = [channel_name | subscribed_channels]

    if not already_subscribed?(channel_name),
      do: Process.put(:subscribed_channels, total_channels)

    "*3\r\n$9\r\nsubscribe\r\n$#{String.length(channel_name)}\r\n#{channel_name}\r\n:#{length(total_channels)}\r\n"
  end

  defp already_subscribed?(channel_name) do
    Process.get(:subscribed_channels, [])
    |> Enum.member?(channel_name)
  end
end
