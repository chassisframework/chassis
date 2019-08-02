defmodule Chassis.Cluster.Application.VNodeSupervisor do
  @moduledoc false

  alias Chassis.ClusterState
  alias Chassis.ClusterState.Config
  alias Chassis.Cluster.VNode
  # alias Chassis.Cluster.State.Layout
  # alias Chassis.Node

  use DynamicSupervisor

  def start_link(cluster_name) do
    {:ok, pid} = DynamicSupervisor.start_link(__MODULE__, [], name: name(cluster_name))

    # eventual mode
    # start_local_v_nodes(state)

    {:ok, pid}
  end

  def init([]) do
    DynamicSupervisor.init(strategy: :one_for_one)
  end

  def start_v_node(%ClusterState{config: %Config{cluster_name: cluster_name}} = cluster_state, partition_id, ranges) do
    cluster_name
    |> name()
    |> DynamicSupervisor.start_child({VNode, [cluster_state, partition_id, ranges]})
  end

  #
  # eventual mode
  #
  # defp start_local_v_nodes(%ClusterState{config: %Config{cluster_name: cluster_name},
  #                                 layout: layout} = state) do
  #   node = node()

  #   layout
  #   |> Layout.nodes()
  #   |> Enum.find_value(fn
  #     {%Node{name: ^node}, v_nodes} ->
  #       v_nodes

  #     _ ->
  #       nil
  #   end)
  #   |> Enum.each(fn v_node ->
  #     {:ok, _pid} =
  #       cluster_name
  #       |> name()
  #       |> DynamicSupervisor.start_child({VNode, [state, v_node]})
  #   end)
  # end

  def name(cluster_name) do
    Module.concat(__MODULE__, cluster_name)
  end
end
