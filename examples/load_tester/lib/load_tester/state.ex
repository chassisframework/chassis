defmodule LoadTester.State do
  use Chassis.State, mode: :consistent

  def init([]) do
    {:ok, nil}
  end

  def handle_command(:ping, state) do
    {:reply, :pong, state}
  end
end
