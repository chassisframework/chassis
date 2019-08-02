defmodule KVTest do
  use Chassis.ClusterCase,
    v_node_app: {KV.VNode, []},
    state_app: {KV.State, []},
    num_spare_nodes: 1

  alias Chassis.Reply

  test "put/3 + get/2", %{cluster_name: cluster_name} do
    :ok = KV.put(cluster_name, "a key", "a value")

    assert {:ok, "a value"} == KV.get(cluster_name, "a key")

    KV.get(cluster_name, "a key", metadata: true)
  end

  test "delete/2", %{cluster_name: cluster_name} do
    :ok = KV.put(cluster_name, "delete me", "a value")

    assert {:ok, "a value"} == KV.get(cluster_name, "delete me")

    :ok = KV.delete(cluster_name, "delete me")

    assert {:error, :not_found} == KV.get(cluster_name, "delete me")
  end

  describe "scaling up" do
    test "keys should be re-distributed", %{cluster_name: cluster_name, spare_nodes: [new_node]} do
      keys_and_values = Enum.into(0..1_000, %{}, fn i -> {i, "#{i} value"} end)

      cluster_state = Chassis.get_cluster_state(cluster_name)
      cluster_version = Chassis.cluster_version(cluster_state)

      Enum.each(keys_and_values, fn {key, value} ->
        :ok = KV.put(cluster_name, key, value)

        expected_partition_id = Chassis.partition_id_for_key(cluster_state, key)

        assert %Reply{
          reply: {:ok, ^value},
          from_partition_id: ^expected_partition_id
        } = KV.get(cluster_name, key, metadata: true)
      end)

      # transition the cluster by adding a new node

      {:ok, _staged_changes} = Chassis.stage_node_join(cluster_name, new_node)
      :ok = Chassis.commit_stage(cluster_name)
      assert_receive {:chassis_event, ^cluster_name, :membership, :transition_finished}, 15_000

      # ensure the cluster version has been bumped

      new_cluster_state = Chassis.get_cluster_state(cluster_name)
      new_cluster_version = Chassis.cluster_version(new_cluster_state)
      assert new_cluster_version == cluster_version + 1

      # check that at least one key is expected to be found on a different partition

      num_keys_moved =
        keys_and_values
        |> Map.keys()
        |> Enum.count(fn key ->
          old_partition_id = Chassis.partition_id_for_key(cluster_state, key)
          new_partition_id = Chassis.partition_id_for_key(new_cluster_state, key)

          old_partition_id != new_partition_id
        end)

      assert num_keys_moved > 0

      # ensure all keys are found in their new locations and have the correct value

      Enum.each(keys_and_values, fn {key, value} ->
        expected_partition_id = Chassis.partition_id_for_key(new_cluster_state, key)

        assert %Reply{
          reply: {:ok, ^value},
          from_partition_id: ^expected_partition_id
        } = KV.get(cluster_name, key, metadata: true)
      end)
    end
  end
end
