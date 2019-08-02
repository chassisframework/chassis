defmodule Chassis.ClusterCase do
  use ExUnit.CaseTemplate

  alias Chassis.Test.ClusterNodes
  alias Chassis.Cluster.Router

  using args do
    quote do
      setup_all do
        args = unquote(args)
        num_nodes = Keyword.get(args, :num_nodes, 3)
        num_spare_nodes = Keyword.get(args, :num_spare_nodes, 0)

        {nodes, spare_nodes} = unquote(__MODULE__).spawn_nodes(num_nodes, num_spare_nodes)

        cluster_name = unquote(__MODULE__).random_cluster_name()

        on_exit(fn ->
          ClusterNodes.destroy_nodes(nodes)
        end)

        %{cluster_name: cluster_name, nodes: nodes, spare_nodes: spare_nodes}
      end

      setup_all %{cluster_name: cluster_name} = context do
        args = unquote(args)
        {:ok, v_node_app} = Keyword.fetch(args, :v_node_app)
        {:ok, state_app} = Keyword.fetch(args, :state_app)

        unquote(__MODULE__).start_cluster(v_node_app, state_app, context)

        on_exit(fn ->
          Chassis.destroy_cluster(cluster_name)
        end)
      end

      setup %{cluster_name: cluster_name} = context do
        :ok = Chassis.subscribe(cluster_name)
      end
    end
  end

  def spawn_nodes(num_nodes, num_spare_nodes) do
    (num_nodes + num_spare_nodes)
    |> ClusterNodes.spawn_nodes()
    |> Enum.split(num_nodes)
  end

  # TODO document
  @doc "TODO"
  def start_cluster(
        v_node_app,
        state_apps,
        context
      ) do
    :ok = ensure_modules_loaded(context)
    :ok = make_cluster(v_node_app, state_apps, context)
    :ok = subscribe_to_router(context)
  end

  defp subscribe_to_router(%{cluster_name: cluster_name}) do
    Router.subscribe(cluster_name)
  end

  defp make_cluster(v_node_app, state_app, %{cluster_name: cluster_name, nodes: nodes}) do
    Chassis.start_cluster(cluster_name, v_node_app, state_app, nodes)
  end

  defp ensure_modules_loaded(%{nodes: nodes, spare_nodes: spare_nodes}) do
    :code.all_loaded()
    |> Keyword.keys()
    |> Enum.each(fn module ->
      Enum.each(nodes ++ spare_nodes, fn node ->
        ensure_loaded(module, node)
      end)
    end)
  end

  defp ensure_loaded(module, node) do
    :rpc.call(node, Code, :ensure_loaded, [module])
  end

  def random_cluster_name do
    :crypto.strong_rand_bytes(20) |> Base.encode32()
  end
end
