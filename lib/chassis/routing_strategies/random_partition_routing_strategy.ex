defmodule Chassis.RandomPartitionRoutingStrategy do
  @moduledoc """
  Routes requests to a random partition
  """

  alias Chassis.ClusterState

  @behaviour Chassis.RoutingStrategy

  def init(_cluster_state), do: :ok

  def route(cluster_state, _request) do
    cluster_state
    |> ClusterState.partition_ids()
    |> Enum.random()
  end

  # def on_reply(_cluster_state, _reply), do: :ok
end
