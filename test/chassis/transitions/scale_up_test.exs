defmodule Chassis.Transitions.ScaleUpTest do
  # alias PartitionedClusterLayout.Replica
  alias PartitionedClusterLayout.Diff.NewPartition
  alias PartitionedClusterLayout.Node
  alias PartitionedClusterLayout.Transition
  alias Chassis.Cluster.Membership
  alias Chassis.Cluster.Membership.Changes.JoinChange
  alias Chassis.Cluster.RangeAccept
  alias Chassis.Cluster.RangeSteal
  alias Chassis.ClusterState
  alias Chassis.Range
  alias Chassis.TestState
  alias Chassis.TestVNode

  use Chassis.ClusterCase,
    v_node_app: {TestVNode, []},
    state_app: {TestState, []},
    num_spare_nodes: 1

  # doctest Chassis

  # test "partitions hold correct ranges after scaling up", %{cluster_name: cluster_name, spare_nodes: new_nodes} do
  #   Chassis.partition_ids(cluster_name)
  #   |> IO.inspect

  #   Enum.each(new_nodes, fn new_node ->
  #     {:ok, _staged_changes} = Chassis.stage_node_join(cluster_name, new_node)
  #   end)

  #   :ok = Chassis.commit_stage(cluster_name)

  #   assert_receive {:chassis_event, ^cluster_name, :membership, :transition_finished}, 15_000

  #   Chassis.partition_ids(cluster_name)
  #   |> IO.inspect
  # end

  test "Stage a node join and commit the stage", %{cluster_name: cluster_name, spare_nodes: new_nodes} do
    Enum.each(new_nodes, fn new_node ->
      {:ok, staged_changes} = Chassis.stage_node_join(cluster_name, new_node)

      assert Enum.member?(staged_changes, %JoinChange{args: [], node: new_node})

      assert_receive {:chassis_event, ^cluster_name, :membership, {:staged, %JoinChange{node: ^new_node}}}
    end)

    {:ok, %ClusterState{staged_changes: staged_changes}} = Membership.get_state(cluster_name)

    :ok = Chassis.commit_stage(cluster_name)

    old_cluster_state = Chassis.get_cluster_state(cluster_name)

    assert_receive {:chassis_event, ^cluster_name, :membership, {:stage_commit, ^staged_changes}}
    assert_receive {:chassis_event, ^cluster_name, :membership, {:beginning_transition, plan}}, 1_000

    plan
    |> Transition.to_list()
    |> Enum.each(fn {action, conf_msg} = step ->
      assert_receive {:chassis_event, ^cluster_name, :membership, {:executing_transition_step, ^step}}, 1_000

      case action do
        {:add_node, %Node{name: new_node}} ->
          assert_receive {:chassis_event, ^cluster_name, :membership, {:node_ready, ^new_node}}, 1_000

        {:add_partition,
         %NewPartition{
           id: partition_id,
           ranges: ranges,
           replicas: replicas
         }} ->
          assert_receive {:chassis_event, ^cluster_name, :membership, {:creating_partition, ^partition_id, ^replicas, ^ranges, ^conf_msg}}

          Enum.each(ranges, fn {from_partition_id, range} ->
            range = Range.new(range)
            assert_receive {:chassis_event, ^cluster_name, {:partition, ^partition_id}, {:stealing_range, %RangeSteal{from_partition_id: ^from_partition_id, range: ^range}}}, 1_000

            data = "data for range #{inspect(range)}"
            assert_receive {:chassis_event, ^cluster_name, {:partition, ^from_partition_id}, {:sending_range, %RangeAccept{from_partition_id: ^from_partition_id, range: ^range, data: ^data}}}, 1_000

            assert_receive {:chassis_event, ^cluster_name, {:partition, ^partition_id}, {:accepted_range, %RangeAccept{from_partition_id: ^from_partition_id, range: ^range, data: ^data}}}, 1_000

            old_cluster_state
            |> ClusterState.node_names(from_partition_id)
            |> Enum.each(fn node ->
              assert_receive {:chassis_event, ^cluster_name, {:v_node, ^from_partition_id, ^node}, {:releasing_range, ^range}}, 1_000
            end)
          end)

          assert_receive {:chassis_event, ^cluster_name, {:partition, ^partition_id}, :finished_stealing}, 1_000
      end
    end)

    assert_receive {:chassis_event, ^cluster_name, :membership, :finalizing_transition}, 1_000

    cluster_name
    |> Chassis.get_cluster_state()
    |> Chassis.partition_ids()
    |> Enum.map(fn partition_id ->
      assert_receive {:chassis_event, ^cluster_name, :membership, {:partition_finalized, ^partition_id}}, 1_000
      assert_receive {:chassis_event, ^cluster_name, {:partition, partition_id}, :finished_transition}, 1_000
    end)

    assert_receive {:chassis_event, ^cluster_name, :membership, :transition_finished}, 1_000
  end

  def flush do
    flush_one()
    flush()
  end

  def flush_one do
    receive do
      msg ->
        IO.inspect(msg)
    end
  end
end
