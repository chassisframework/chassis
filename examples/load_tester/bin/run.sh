#!/bin/sh

cd `dirname $0`/..
rm -rf tmp

export MIX_ENV="prod"

mix compile.protocols

elixir -pa _build/$MIX_ENV/consolidated  --erl "+P 134217727 +C multi_time_warp" -S mix run bin/run.exs
