defmodule ChassisTest do
  alias Chassis.TestVNode
  alias Chassis.TestState
  alias Chassis.Cluster.Router

  use Chassis.ClusterCase,
    v_node_app: {TestVNode, []},
    state_app: {TestState, []}

  # doctest Chassis

  test "call/3 + command/1 requests are routed correctly and function", %{cluster_name: cluster_name} do
    key = "abc"

    correct_partition = Router.partition_for_key(cluster_name, key)

    assert %{
             key: key,
             v_node: correct_partition,
             state: correct_partition
           } == Chassis.call(cluster_name, key, :who)
  end

  test "cast/3 + query/1 requests are routed correctly and function", %{cluster_name: cluster_name} do
    key = "abc"
    ref = :erlang.make_ref()
    from = {self(), ref}

    Chassis.cast(cluster_name, key, {:who, from})

    correct_partition = Router.partition_for_key(cluster_name, key)

    expected = %{
      key: key,
      v_node: correct_partition,
      state: correct_partition
    }

    receive do
      {^ref, reply} ->
        assert expected == reply
    end
  end
end
