defmodule DBConnection.Mixfile do
  use Mix.Project

  @pools [:connection, :poolboy, :sojourn]
  @version "0.1.7"

  def project do
    [app: :db_connection,
     version: @version,
     elixir: "~> 1.0",
     deps: deps,
     docs: docs,
     description: description,
     package: package,
     build_per_environment: false,
     test_paths: test_paths(Mix.env),
     aliases: ["test.all": ["test", "test.pools"],
               "test.pools": &test_pools/1],
     preferred_cli_env: ["test.all": :test]]
  end

  def application do
    [applications: [:logger, :connection, :backoff],
     mod: {DBConnection.App, []}]
  end

  defp deps do
    [{:connection, "~> 1.0.2"},
     {:backoff, "~> 1.0"},
     {:poolboy, "~> 1.5", [optional: true]},
     {:sbroker, "~> 0.7", [optional: true]},
     {:earmark, "~> 0.1", only: :dev},
     {:ex_doc, "~> 0.11.1", only: :dev}]
  end

  defp docs do
    [source_url: "https://github.com/fishcakez/db_connection",
     source_ref: "v#{@version}",
     main: DBConnection]
  end

  defp description do
    """
    Database connection behaviour for database transactions and connection pooling
    """
  end

  defp package do
    %{licenses: ["Apache 2.0"],
      maintainers: ["James Fish"],
      links: %{"Github" => "https://github.com/fishcakez/db_connection"}}
  end

  defp test_paths(pool) when pool in @pools, do: ["integration_test/#{pool}"]
  defp test_paths(_), do: ["test"]

  defp test_pools(args) do
    for env <- @pools, do: env_run(env, args)
  end

  defp env_run(env, args) do
    args = if IO.ANSI.enabled?, do: ["--color"|args], else: ["--no-color"|args]

    IO.puts "==> Running tests for MIX_ENV=#{env} mix test"
    {_, res} = System.cmd "mix", ["test"|args],
                          into: IO.binstream(:stdio, :line),
                          env: [{"MIX_ENV", to_string(env)}]

    if res > 0 do
      System.at_exit(fn _ -> exit({:shutdown, 1}) end)
    end
  end
end
