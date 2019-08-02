defmodule LoadTester do
  @num_requesters System.schedulers()
  @num_requests_per_requester 20000

  alias Chassis.Test.ClusterNodes

  alias LoadTester.VNode
  alias LoadTester.State
  alias LoadTester.Requester
  alias LoadTester.RequesterSupervisor

  def run do
    nodes = ClusterNodes.spawn_nodes(3)
    cluster_name = :crypto.strong_rand_bytes(20) |> Base.encode32

    IO.puts "starting cluster with #{length(nodes)} nodes..."
    :ok = Chassis.start_cluster(cluster_name, {VNode, []}, {State, []}, nodes)

    IO.puts "waiting for routing table..."
    :ok = Chassis.subscribe(cluster_name)

    IO.puts "cluster has #{num_partitions(cluster_name, nodes)} partitions"

    IO.puts "starting #{@num_requesters} requesters..."
    Enum.each(0..@num_requesters, fn _i ->
      {:ok, _pid} = RequesterSupervisor.start_requester()
    end)

    IO.puts "load testing..."
    time fn ->
      RequesterSupervisor.requesters()
      |> Enum.each(fn pid ->
        Requester.start(pid, cluster_name, @num_requests_per_requester)
      end)

      await()
    end
  end

  def time(fun) do
    {microsecs, :ok} = :timer.tc(fun)

    secs = microsecs/:math.pow(10, 6)
    total_requests = @num_requests_per_requester * @num_requesters
    IO.puts("processed #{total_requests} requests in #{secs}s -> approx #{total_requests/secs |> trunc} per sec")
  end

  def num_partitions(cluster_name, [node | _rest]) do
    :rpc.call(node, Chassis, :status, [])
    |> get_in([cluster_name, :partitions])
    |> map_size()
  end

  def await(num_left \\ @num_requesters)
  def await(0), do: :ok
  def await(num_left) do
    receive do
      :done ->
        await(num_left - 1)
    end
  end
end
