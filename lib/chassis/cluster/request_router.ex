defmodule Chassis.Cluster.RequestRouter do
  @moduledoc false

  alias Chassis.ClusterState
  alias Chassis.Cluster.TransitionRanges
  alias Chassis.Cluster.Forward
  alias Chassis.Request

  def route(%Request{routing_strategy: strategy} = request, %ClusterState{} = cluster_state) do
    %Request{request | partition_id: strategy.route(cluster_state, request)}
  end

  def should_handle_or_forward(_cluster_state, _transition_ranges, %Request{key_provided: false}, _from_partition_id) do
    :handle
  end

  def should_handle_or_forward(%ClusterState{} = cluster_state, nil, %Request{key_provided: true, key: key}, from_partition_id) do
    digested_key = ClusterState.digest_key(cluster_state, key)

    do_should_handle_or_forward(cluster_state, digested_key, from_partition_id)
  end

  def should_handle_or_forward(%ClusterState{} = cluster_state, %TransitionRanges{} = transition_ranges, %Request{key_provided: true, key: key}, from_partition_id) do
    digested_key = ClusterState.digest_key(cluster_state, key)

    case TransitionRanges.should_handle_or_forward(transition_ranges, digested_key) do
      :handle ->
        :handle

      {:forward, to_partition_id, to_nodes} ->
        %Forward{from_partition_id: from_partition_id, to_partition_id: to_partition_id, to_nodes: to_nodes}

      {:error, :not_responsible_for_key} ->
        do_should_handle_or_forward(cluster_state, digested_key, from_partition_id)

      error ->
        error
    end
  end

  defp do_should_handle_or_forward(%ClusterState{} = cluster_state, digested_key, from_partition_id) do
    case ClusterState.partition_id_for_digested_key(cluster_state, digested_key) do
      ^from_partition_id ->
        :handle

      to_partition_id ->
        to_nodes = ClusterState.node_names(cluster_state, to_partition_id)

        %Forward{
          from_partition_id: from_partition_id,
          to_partition_id: to_partition_id,
          to_nodes: to_nodes
        }
    end
  end

  # defp wrap_routing_error(error, %State{partition_id: partition_id} = state) do
  #   {:reply, %RoutingError{from_partition_id: partition_id, error: error}, state}
  # end
end
