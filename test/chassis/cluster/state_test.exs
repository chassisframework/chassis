defmodule Chassis.Cluster.StateTest do
  use ExUnit.Case

  alias Chassis.Node
  alias Chassis.Cluster.State
  alias Chassis.Cluster.State.Config
  alias Chassis.Cluster.State.Layout
  alias Chassis.Cluster.PartitioningStrategy.SplitLargestStrategy

  # test "add_nodes/3" do
  #   config = %Config{partitioning_strategy: SplitLargestStrategy, num_replicas: 3}

  #   nodes = [
  #     Node.new(:a),
  #     Node.new(:b),
  #     Node.new(:c)
  #   ]

  #   layout = Layout.new(nodes, config)

  #   state = %State{config: config, layout: layout}

  #   new_nodes_and_args = [
  #     {Node.new(:x), []},
  #     {Node.new(:y), []},
  #     {Node.new(:z), []}
  #   ]

  #   State.add_nodes(state, new_nodes_and_args)
  # end
end
