use Mix.Config

config :chassis,
  ra_log_level: :warn

config :logger, :console,
  level: :warn

config :ra, data_dir: 'tmp/test'
