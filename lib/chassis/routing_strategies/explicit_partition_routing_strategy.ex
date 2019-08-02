defmodule Chassis.ExplicitPartitionRoutingStrategy do
  @moduledoc """
  Routes requests to an explicitly provided partition
  """

  alias Chassis.Request

  @behaviour Chassis.RoutingStrategy

  def init(_cluster_state), do: :ok

  # TODO: better error
  def route(_cluster_state, %Request{partition_id: nil}) do
    raise "asked to explicitly route to `nil` partition id"
  end

  def route(_cluster_state, %Request{partition_id: partition_id}) do
    partition_id
  end

  # def on_reply(_cluster_state, _reply), do: :ok
end
