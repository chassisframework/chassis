defmodule Chassis.KeyRoutingStrategy do
  @moduledoc """
  Routes requests by digesting the key and looking it up in the cluster's partition map.
  """

  alias Chassis.ClusterState
  alias Chassis.Request

  @behaviour Chassis.RoutingStrategy

  def init(_cluster_state), do: :ok

  def route(cluster_state, %Request{key_provided: true, key: key}) do
    ClusterState.partition_id_for_key(cluster_state, key)
  end

  def on_reply(_cluster_state, _reply), do: :ok
end
