#
# TODO: be explicit about functions that use cached routing state rather than fetching cluster state from membership
# maybe make it explicit to the user by requring them to manually fetch the cluster state and pass it to the
# function so they know if it's cached
#
defmodule Chassis do
  @moduledoc """
  Documentation for Chassis.
  """

  alias PartitionedClusterLayout.Strategy.SimpleStrategy
  alias Chassis.Cluster
  alias Chassis.ClusterState
  alias Chassis.Cluster.Status
  alias Chassis.Cluster.VNode
  alias Chassis.Reply
  alias Chassis.Request
  alias Chassis.Cluster.Router

  @default_num_replicas 3
  @default_num_partitions_per_node 8
  @default_num_local_nodes 5
  @default_num_spare_local_nodes 0
  @default_cluster_strategy SimpleStrategy
  @default_cluster_strategy_args [num_partitions_per_node: @default_num_partitions_per_node]

  # defguard is_cluster_name(cluster_name) when is_binary(cluster_name) or is_atom(cluster_name)
  # defguardp is_atom_or_binary(atom_or_binary) when is_atom(atom_or_binary) or is_binary(atom_or_binary)
  defguardp is_non_empty_list(non_empty_list) when is_list(non_empty_list) and non_empty_list != []
  defguardp is_application(module, args) when is_atom(module) and is_list(args)

  @type cluster_name :: String.t()
  @type cluster_state :: ClusterState.t()
  @type partition_id :: PartitionedClusterLayout.partition_id()
  @type request :: any()

  def start_cluster(
        cluster_name,
        {v_node_module, v_node_args} = v_node_app,
        # {state_module, state_args} = state_app,
        state_apps,
        node_names,
        opts \\ []
      )
      # when is_binary(cluster_name)
      # and is_application(state_module, state_args)
      when is_application(v_node_module, v_node_args) and
             is_non_empty_list(node_names) and
             is_list(opts) do
    num_replicas = Keyword.get(opts, :num_replicas, @default_num_replicas)
    strategy = Keyword.get(opts, :strategy, @default_cluster_strategy)
    strategy_args = Keyword.get(opts, :strategy_args, @default_cluster_strategy_args)

    strategy_args = Keyword.merge(strategy_args, nodes: node_names, num_replicas: num_replicas)

    Cluster.start_cluster(
      cluster_name,
      v_node_app,
      state_apps,
      strategy,
      strategy_args,
      self()
    )
  end

  def start_local_cluster(
        cluster_name,
        {v_node_module, v_node_args} = v_node_app,
        # {state_module, state_args} = state_app,
        state_apps,
        opts \\ []
      )
      # when is_binary(cluster_name)
      # and is_application(state_module, state_args)
      when is_application(v_node_module, v_node_args) and
             is_map(state_apps) and
             is_list(opts) do
    alias Chassis.ClusterCase
    num_nodes = Keyword.get(opts, :num_nodes, @default_num_local_nodes)
    num_spare_nodes = Keyword.get(opts, :num_spare_nodes, @default_num_spare_local_nodes)

    {nodes, spare_nodes} = ClusterCase.spawn_nodes(num_nodes, num_spare_nodes)

    context = %{cluster_name: cluster_name, nodes: [node() | nodes], spare_nodes: spare_nodes}

    :ok = ClusterCase.start_cluster(v_node_app, state_apps, context)

    {:ok, spare_nodes}
  end

  defdelegate destroy_cluster(cluster_name), to: Cluster

  # def stop_cluster(cluster_name) do
  # end

  defdelegate status, to: Status

  # TODO: configurable v_node selection (leader, follower, any..)
  def call(cluster_name, user_request, context \\ []) do
    cluster_state = Router.get_state(cluster_name)

    user_request
    |> Request.new(cluster_state, context)
    |> Cluster.call(cluster_state)
    |> maybe_unwrap_reply(context)
  end

  def cast(cluster_name, request, context \\ []) do
    request
    |> Request.new(context)
    |> Cluster.cast(cluster_name)
  end

  # can only be called from a vnode
  defdelegate command(name, command, opts \\ []), to: VNode, as: :state_command
  # TODO: consistency: :leader_consistent | :leader | :follower | :any
  defdelegate query(name, query, opts \\ []), to: VNode, as: :state_query

  def stage_node_join(cluster_name, node, args \\ []) do
    Cluster.stage_node_join(cluster_name, node, args)
  end

  defdelegate commit_stage(cluster_name), to: Cluster
  defdelegate subscribe(cluster_name), to: Cluster

  defdelegate get_cluster_state(cluster_name, opts \\ []), to: Cluster

  # test helpers / debug
  @doc false
  defdelegate partition_id_for_key(cluster_state, key), to: ClusterState
  @doc false
  defdelegate cluster_version(cluster_state), to: ClusterState, as: :version

  @doc false
  defdelegate partition_ids(cluster_state), to: Cluster
  # @doc false
  # defdelegate partition_state(cluster_name, partition_id), to: Cluster

  defp maybe_unwrap_reply(%Reply{reply: reply} = wrapped_reply, opts) do
    if should_unwrap_reply?(opts) do
      reply
    else
      wrapped_reply
    end
  end

  defp should_unwrap_reply?(opts) do
    Keyword.get(opts, :metadata, false) != true
  end
end
