defmodule Chassis.ClusterState.Config do
  defstruct [
    :v_node_app,
    :state_apps,
    :cluster_name,
    :strategy
  ]
end
