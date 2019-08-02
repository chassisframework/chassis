defmodule UniqueIdGenerator.VNode do
  use Chassis.VNode

  require Logger

  # use Chassis, :v_node

  def init([], _context) do
    {:ok, nil}
  end

  def handle_call(:get, context, state) do
    {:reply, Chassis.command(:get, context), state}
  end

  def handle_call(:state, context, state) do
    {:reply, Chassis.query(:state, context), state}
  end

  def release_range(_range, _cluster_state, state) do
    {:ok, state}
  end

  def accept_range(_range, _cluster_state, state) do
    {:ok, state}
  end
end
