use Mix.Config

config :chassis,
  ra_log_level: :warn

config :ra,
  data_dir: 'tmp/prod'

config :logger, :console,
  level: :warn,
  metadata: [:cluster_name, :partition_id]
