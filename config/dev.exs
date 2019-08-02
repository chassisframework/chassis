import Config

config :ra_util,
  ra_log_level: :warn

config :ra,
  data_dir: 'tmp/dev'

config :logger, :console,
  level: :info,
  metadata: [:cluster_name, :partition_id]
