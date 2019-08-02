use Mix.Config

config :ra_util,
  ra_log_level: :warn

config :logger, :console, level: :info

config :ra, data_dir: 'tmp/test'
