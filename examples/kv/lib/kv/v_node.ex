defmodule KV.VNode do
  # use Chassis.VNode

  require Logger

  # use Chassis, :v_node

  def init([]) do
    {:ok, nil}
  end

  def handle_call({:put, v}, context, state) do
    {:reply, Chassis.command(:keys, {:put, v}, context), state}
  end

  def handle_call(:delete, context, state) do
    {:reply, Chassis.command(:keys, :delete, context), state}
  end

  def handle_call(:get, context, state) do
    {:reply, Chassis.query(:keys, :get, context), state}
  end

  def release_range(_range, _cluster_state, state) do
    {:ok, state}
  end
end
