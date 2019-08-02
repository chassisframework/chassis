use Mix.Config

config :ra_util,
  ra_log_level: :warn

config :logger, :console, level: :warn

config :ra, data_dir: 'tmp/prod'
