defmodule Chassis.Cluster.Forward do
  @doc false

  defstruct [:from_partition_id, :to_partition_id, :to_nodes]
end
