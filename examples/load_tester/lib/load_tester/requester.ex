defmodule LoadTester.Requester do
  use GenServer

  def start_link([]) do
    GenServer.start_link(__MODULE__, [])
  end

  def start(pid, cluster_name, num_requests) do
    GenServer.cast(pid, {:start, self(), cluster_name, num_requests})
  end

  def init([]) do
    {:ok, nil}
  end

  def handle_cast({:start, collector, cluster_name, num_requests},  state) do
    Enum.each(0..num_requests, fn _i ->
      :pong = Chassis.call(cluster_name, :ping, key: :rand.uniform())
    end)

    send(collector, :done)

    {:noreply, state}
  end
end
