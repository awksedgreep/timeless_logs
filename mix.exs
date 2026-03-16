defmodule TimelessLogs.MixProject do
  use Mix.Project

  def project do
    [
      app: :timeless_logs,
      version: "1.3.2",
      elixir: "~> 1.18",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      description: "Embedded log compression and indexing for Elixir applications.",
      source_url: "https://github.com/awksedgreep/timeless_logs",
      homepage_url: "https://github.com/awksedgreep/timeless_logs",
      package: package(),
      docs: docs()
    ]
  end

  def application do
    [
      extra_applications: [:logger, :inets, :ssl, :public_key, :crypto],
      mod: {TimelessLogs.Application, []}
    ]
  end

  defp deps do
    [
      {:ezstd, "~> 1.2"},
      {:ex_openzl, "~> 0.4.0"},
      {:rocket, github: "awksedgreep/rocket"},
      {:telemetry, "~> 1.0"},
      {:ex_doc, "~> 0.34", only: :dev, runtime: false}
    ]
  end

  defp package do
    [
      maintainers: ["Mark Cotner"],
      licenses: ["MIT"],
      links: %{"GitHub" => "https://github.com/awksedgreep/timeless_logs"},
      files: ~w(lib .formatter.exs mix.exs README.md LICENSE)
    ]
  end

  defp docs do
    [
      main: "readme",
      extras:
        ["README.md", "LICENSE"] ++
          Path.wildcard("docs/*.md")
    ]
  end
end
