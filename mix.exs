defmodule Chassis.MixProject do
  use Mix.Project

  def project do
    [
      app: :chassis,
      version: "0.1.0",
      elixir: "~> 1.8",
      start_permanent: Mix.env() == :prod,
      elixirc_paths: elixirc_paths(Mix.env()),
      deps: deps()
    ]
  end

  def application do
    [
      extra_applications: [:logger],
      mod: {Chassis.Application, []}
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:ra, github: "rabbitmq/ra"},
      {:ra_transaction_machine, path: "../ra_transaction_machine"},
      # {:ra, "~> 1.0.0"},
      {:delta_crdt, "~> 0.5.0", optional: true},
      {:partitioned_cluster_layout, path: "../partitioned_cluster_layout", override: true},
      {:interval_map, path: "../interval_map", override: true}
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]
end
