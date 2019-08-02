use Mix.Config

config :ra,
  logger_module: Ra.Logger

import_config "#{Mix.env()}.exs"
