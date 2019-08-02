defmodule LoadTester.RequesterSupervisor do
  use DynamicSupervisor

  alias LoadTester.Requester

  def start_link([]) do
    DynamicSupervisor.start_link(__MODULE__, [], name: __MODULE__)
  end

  def init([]) do
    DynamicSupervisor.init(strategy: :one_for_one)
  end

  def start_requester do
    DynamicSupervisor.start_child(__MODULE__, {Requester, []})
  end

  def requesters do
    __MODULE__
    |> DynamicSupervisor.which_children()
    |> Enum.map(fn
      {:undefined, pid, :worker, [Requester]} ->
        pid
    end)
  end
end
