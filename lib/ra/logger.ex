defmodule Ra.Logger do

  def log(:notice, fmt, args, metadata), do: log(:info, fmt, args, metadata)
  def log(:warning, fmt, args, metadata), do: log(:warn, fmt, args, metadata)

  def log(level, fmt, args, metadata) do
    require Logger

    ra_log_level = Application.get_env(:chassis, :ra_log_level, :debug)

    if Logger.compare_levels(level, ra_log_level) != :lt do
      case level do
        :debug ->
          Logger.debug(log_func(fmt, args, metadata))

        :info ->
          Logger.info(log_func(fmt, args, metadata))

        :warn ->
          Logger.warn(log_func(fmt, args, metadata))

        :error ->
          Logger.error(log_func(fmt, args, metadata))
      end
    end
  end

  defp log_func(fmt, args, metadata) do
    fn ->
      msg =
        "[Ra] " <>
        (fmt
        |> :io_lib.format(args)
        |> :erlang.list_to_binary())

      metadata =
        metadata
        |> Map.put(:application, :ra)
        |> Enum.flat_map(fn
          {:mfa, {m, f, a}} ->
            [ra_module: m, ra_function: f, ra_args: a]

          other ->
            [other]
        end)

      {msg, metadata}
    end
  end
end
