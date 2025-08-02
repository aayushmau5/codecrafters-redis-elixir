defmodule Commands.Subscribe do
  def handle_subscribe([_, channel_name]) do
    Process.put(:in_subscription, true)

    if already_subscribed?(channel_name) do
      subscribed_channels = Process.get(:subscribed_channels)

      "*3\r\n$9\r\nsubscribe\r\n$#{String.length(channel_name)}\r\n#{channel_name}\r\n:#{length(subscribed_channels)}\r\n"
    else
      subscribed_channels = Process.get(:subscribed_channels, [])
      total_channels = [channel_name | subscribed_channels]
      Process.put(:subscribed_channels, total_channels)

      "*3\r\n$9\r\nsubscribe\r\n$#{String.length(channel_name)}\r\n#{channel_name}\r\n:#{length(total_channels)}\r\n"
    end
  end

  defp already_subscribed?(channel_name) do
    Process.get(:subscribed_channels, [])
    |> Enum.member?(channel_name)
  end
end
