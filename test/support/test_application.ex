defmodule Chassis.TestVNode do
  use Chassis.VNode

  alias Chassis.Cluster.VNode
  alias Chassis.Cluster.VNode.Dictionary

  def init([]) do
    {:ok, nil}
  end

  def handle_call(:who, key, _from, state) do
    %Dictionary{partition_id: partition_id} = VNode.get_dictionary()

    reply = %{
      key: key,
      v_node: partition_id,
      state: Chassis.command(:who, key: key)
    }

    {:reply, reply, state}
  end

  def handle_cast({:who, {test_pid, ref}}, key, state) do
    %Dictionary{partition_id: partition_id} = VNode.get_dictionary()

    reply = %{
      key: key,
      v_node: partition_id,
      state: Chassis.query(:who, key: key)
    }

    send(test_pid, {ref, reply})

    {:noreply, state}
  end

  def release_range(_range, _cluster_state, state) do
    {:ok, state}
  end

  def accept_range(_range, _cluster_state, state) do
    {:ok, state}
  end
end

defmodule Chassis.TestState do
  use Chassis.State, mode: :consistent

  def init([], %{partition_id: partition_id}) do
    {:ok, partition_id}
  end

  def handle_command({:who, _key}, partition_id) do
    {:reply, partition_id, partition_id}
  end

  def handle_query({:who, _key}, partition_id) do
    {:reply, partition_id}
  end

  def release_range(range, _cluster_state, state) do
    {:ok, "data for range #{inspect(range)}", state}
  end

  def accept_range(_range, _data, _cluster_state, state) do
    {:ok, state}
  end
end
