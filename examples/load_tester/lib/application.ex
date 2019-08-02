defmodule LoadTester.Application do
  @moduledoc false

  use Application

  alias LoadTester.RequesterSupervisor

  def start(_type, _args) do
    children = [
      {RequesterSupervisor, []}
    ]

    opts = [strategy: :one_for_one, name: LoadTester.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
