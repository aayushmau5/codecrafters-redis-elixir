defmodule Commands do
  def execute(command, data) do
    case command do
      "xadd" -> Commands.Xadd.handle_xadd(data)
      "xrange" -> Commands.Xrange.handle_xrange(data)
      "xread" -> Commands.Xread.handle_xread(data)
      "replconf" -> Commands.Replconf.handle_repl_conf(data)
      "psync" -> Commands.Psync.handle_psync(data)
      "info" -> Commands.Info.handle_info(data)
      "wait" -> Commands.Wait.handle_wait(data)
      "config" -> Commands.Config.handle_config(data)
      "keys" -> Commands.Keys.handle_key(data)
      "type" -> Commands.Type.handle_type(data)
      "multi" -> Commands.Multi.handle_multi(data)
      "discard" -> Commands.Discard.handle_discard(data)
      "exec" -> Commands.Exec.handle_exec(data)
      "llen" -> Commands.Llen.handle_llen(data)
      "lpush" -> Commands.Push.handle_lpush(data)
      "rpush" -> Commands.Push.handle_rpush(data)
      "lrange" -> Commands.Lrange.handle_lrange(data)
      "blpop" -> Commands.Blpop.handle_blpop(data)
      "lpop" -> Commands.Lpop.handle_lpop(data)
      "set" -> Commands.Set.handle_set(data)
      "incr" -> Commands.Incr.handle_incr(data)
      "get" -> Commands.Get.handle_get(data)
      "echo" -> Commands.Echo.handle_echo(data)
      "ping" -> Commands.Ping.handle_ping(data)
      "command" -> Utils.return_ok()
    end
  end

  def get_command([_prefix, _length, command | rest]) do
    {String.downcase(command), rest}
  end

  # Command
  def reconstruct_binary_command(command_body) do
    Enum.join(command_body, "\r\n") <> "\r\n"
  end
end
