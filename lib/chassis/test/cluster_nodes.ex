# lovingly adapted from https://github.com/phoenixframework/phoenix_pubsub/blob/master/test/support/cluster.ex

defmodule Chassis.Test.ClusterNodes do
  require Logger

  def spawn_nodes(num) do
    case Process.whereis(__MODULE__) do
      nil ->
        init()

      _ ->
        :ok
    end

    1..num
    |> Enum.map(fn _ ->
      Agent.get_and_update(__MODULE__, fn i -> {i, i + 1} end)
    end)
    |> Enum.map(&Task.async(fn -> spawn_node(&1) end))
    |> Enum.map(&Task.await(&1, 30_000))
    |> Enum.map(fn {:ok, node} -> node end)
  end

  def destroy_nodes(nodes) do
    Enum.each(nodes, fn node ->
      :erl_boot_server.delete_slave(node)
    end)
  end

  defp spawn_node(port) do
    {:ok, node} = :slave.start('127.0.0.1', '#{port}', inet_loader_args())

    add_code_paths(node)
    transfer_configuration(node, port)
    ensure_applications_started(node)

    {:ok, node}
  end

  defp init do
    # port counter
    Agent.start_link(fn -> 8200 end, name: __MODULE__)
    Node.start(:"primary@127.0.0.1")
    :erl_boot_server.start([:"127.0.0.1"])
  end

  defp rpc(node, module, function, args) do
    :rpc.block_call(node, module, function, args)
  end

  defp inet_loader_args do
    '-loader inet -hosts 127.0.0.1 -setcookie #{:erlang.get_cookie()}'
  end

  defp add_code_paths(node) do
    rpc(node, :code, :add_paths, [:code.get_path()])
  end

  defp transfer_configuration(node, port) do
    Application.loaded_applications()
    |> Enum.map(fn {app_name, _, _} -> app_name end)
    |> Enum.map(fn app_name -> {app_name, Application.get_all_env(app_name)} end)
    |> Keyword.merge(additional_configs(port))
    |> Enum.each(fn {app_name, env} ->
      Enum.each(env, fn {key, val} ->
        :ok = rpc(node, :application, :set_env, [app_name, key, val, [persistent: true]])
      end)
    end)
  end

  defp ensure_applications_started(node) do
    ensure_application_started(node, :mix)
    rpc(node, Mix, :env, [Mix.env()])

    for {app_name, _, _} <- Application.started_applications() do
      ensure_application_started(node, app_name)
    end
  end

  defp ensure_application_started(node, application) do
    {:ok, _} = rpc(node, Application, :ensure_all_started, [application])
  end

  def additional_configs(_port) do
    []
  end
end
