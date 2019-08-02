defmodule LoadTester.VNode do
  use Chassis.VNode

  def init([]) do
    {:ok, nil}
  end

  def handle_call(:ping, _context, state) do
    {:reply, :pong, state}
  end

  # def handle_call({:ping_state, key}, _from, state) do
  #   %Dictionary{partition_id: partition_id} = VNode.get_dictionary()

  #   reply = %{
  #     key: key,
  #     v_node: partition_id,
  #     state: Chassis.command(:who)
  #   }

  #   {:reply, reply, state}
  # end
end
