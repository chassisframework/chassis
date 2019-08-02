defmodule UniqueIdGeneratorTest do
  use Chassis.ClusterCase,
    v_node_app: {UniqueIdGenerator.VNode, []},
    state_app: {UniqueIdGenerator.State, []},
    num_spare_nodes: 1

  alias UniqueIdGenerator.SparseIDRanges

  # describe "get/1" do
  #   test "returns an integer under normal circumstances", %{cluster_name: cluster_name} do
  #     ids =
  #       Enum.map(0..1_000, fn _ ->
  #         assert {:ok, id} = UniqueIdGenerator.get(cluster_name)

  #         assert is_integer(id)

  #         id
  #       end)

  #     assert Enum.uniq(ids)
  #   end

  #   #
  #   # given that we're using the default partitioning strategy, the total id space is 2^32,
  #   # which would be incredibly hard to test for uniqueness as a black box
  #   #
  #   # in order to test this properly, we should use a custom partitioning strategy that shrinks the
  #   # total space to something small, then we can prove the system both returns all completely unique
  #   # ids and an :error correctly when the total id space is exhausted
  #   #
  #   test "returns the total id space, none more than once (all unique)"
  #   test "returns :error when the total space is exhausted"
  # end

  describe "scaling up" do
    test "entire keyspace should be represented", %{cluster_name: cluster_name, spare_nodes: [new_node]} do
      sparse_id_ranges_before = partition_states(cluster_name)
      assert SparseIDRanges.contiguous?(sparse_id_ranges_before)
      range_before = SparseIDRanges.range(sparse_id_ranges_before)

      {:ok, _staged_changes} = Chassis.stage_node_join(cluster_name, new_node)
      :ok = Chassis.commit_stage(cluster_name)
      assert_receive {:chassis_event, ^cluster_name, :membership, :transition_finished}, 15_000

      sparse_id_ranges_after = partition_states(cluster_name)
      assert SparseIDRanges.contiguous?(sparse_id_ranges_after)
      assert range_before == SparseIDRanges.range(sparse_id_ranges_after)
    end
  end

  defp partition_states(cluster_name) do
    cluster_name
    |> Chassis.get_cluster_state()
    |> Chassis.partition_ids()
    |> Enum.map(fn id -> UniqueIdGenerator.partition_state(cluster_name, id) end)
  end
end
