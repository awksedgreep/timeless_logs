# Stale test/tmp state from an interrupted run must not poison this one.
File.rm_rf!(Path.join(__DIR__, "tmp"))

ExUnit.start()
:inets.start()
Code.require_file("support/http_client.exs", __DIR__)
