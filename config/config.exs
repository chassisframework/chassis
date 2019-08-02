import Config

config :ra,
  logger_module: RaUtil.Logger

import_config "#{config_env()}.exs"
