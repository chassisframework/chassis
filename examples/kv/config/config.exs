import Config

config :ra,
  logger_module: Ra.Logger

import_config "#{config_env()}.exs"
