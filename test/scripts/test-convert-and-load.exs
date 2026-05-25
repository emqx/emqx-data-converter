#!/usr/bin/env elixir

# test helpers
defmodule TH do
  require Logger

  @container_name "emqx-data-converter"
  # exposed by docker
  @api_port 48083

  def converter_path() do
    Path.absname("./emqx_data_converter")
  end

  def list_test_configs() do
    Path.wildcard("test/data/*.json")
  end

  def wrap_if(_x, false), do: []
  def wrap_if(x, true), do: List.wrap(x)

  def convert!(input_filepath, opts \\ []) do
    Logger.info(%{input: input_filepath, msg: "converting_input", opts: opts})

    data_files_dir = Keyword.get(opts, :data_files_dir)

    cmd =
      List.flatten([
        "-o",
        "/tmp",
        wrap_if(["--data-files-dir", data_files_dir], !!data_files_dir),
        input_filepath
      ])

    Logger.debug(%{msg: "convertion_cmd", cmd: cmd})

    {:ok, output} = run(converter_path(), cmd)

    IO.puts(output)

    res = Regex.run(~r/backup file: (?<filepath>.+)/, output, capture: ["filepath"])

    case res do
      [filepath] ->
        {:ok, filepath}

      _ ->
        {:error, {:could_not_extract_filepath, output}}
    end
  end

  def import!(backup_filepath) do
    basename = Path.basename(backup_filepath)
    container_filepath = "/tmp/#{basename}"
    {:ok, _} = docker_cp(backup_filepath, container_filepath)
    Logger.info(%{msg: "starting_emqx"})
    :ok = start_emqx()

    {:ok, output} =
      run_in_container("docker-entrypoint.sh emqx ctl data import #{container_filepath}")

    IO.puts(output)

    if output =~ ~r"data has been imported successfully"i do
      :ok
    else
      {:error, output}
    end
  end

  def start_emqx(opts \\ []) do
    wait_s = Keyword.get(opts, :wait_s, 15)

    cmd = [
      "export EMQX_WAIT_FOR_START=#{wait_s}",
      # "export DEBUG=2",
      "docker-entrypoint.sh emqx foreground"
    ]

    Logger.debug(%{msg: "starting_emqx", cmd: cmd})

    # stupid and ugly hack because, for unknown reasons, `emqx start` hangs for ~ 62
    # seconds when running before starting locally, but runs fine in CI...  🫠
    spawn_link(fn ->
      case run_in_container(cmd, stderr_to_stdout: true) do
        {:ok, _} ->
          :ok

        {:error, exit_code, output} ->
          IO.puts(output)
          exit({:failed_to_start_emqx, exit_code, output})
      end
    end)

    Enum.reduce_while(1..wait_s, :error, fn _, acc ->
      Process.sleep(1_000)

      case run_in_container("emqx ctl status") do
        {:ok, output} ->
          IO.puts(output)

          if output =~ "is started" do
            {:halt, :ok}
          else
            {:cont, acc}
          end

        _ ->
          {:cont, acc}
      end
    end)
  end

  def import_table(table, outdir) do
    Logger.info(%{msg: "importing_mnesia_table", table: table})

    {:ok, output} =
      run_in_container(~s|emqx eval 'mnesia:restore("#{outdir}/mnesia/#{table}", [])'|)

    {:ok, val} = parse_erl(output)

    if match?({:atomic, [_]}, val) do
      Logger.info(%{msg: "mnesia_import_successful", result: val})
      :ok
    else
      {:error, {:bad_result, table, val}}
    end
  end

  def parse_erl(str) do
    str = String.trim(str)

    str =
      if String.ends_with?(str, ".") do
        str
      else
        str <> "."
      end

    with {:ok, tokens, _} <- str |> to_charlist() |> :erl_scan.string(),
         {:ok, ast} <- :erl_parse.parse_exprs(tokens),
         {:value, val, _env} <- :erl_eval.exprs(ast, _env = []) do
      {:ok, val}
    else
      err ->
        {:error, {:bad_erl_str, str, err}}
    end
  end

  def start_container(image) do
    Logger.info(%{msg: "starting_container", image: image})

    run(
      "docker",
      [
        "run",
        "--rm",
        "-d",
        "-v",
        "#{File.cwd!()}:/opt/data",
        # in case we want to execute RPC via gen_rpc
        # "-p", "5370:5369",
        # dashboard
        "-p",
        "48083:18083",
        "--name",
        @container_name,
        image,
        "bash",
        "-c",
        "tail -f /dev/null"
      ]
    )
  end

  def stop_container() do
    Logger.info(%{msg: "stopping_container"})
    {:ok, _} = run("docker", ["rm", "-f", @container_name])
    Logger.info(%{msg: "container_stopped"})
  end

  def run_in_container(cmd, opts \\ []) do
    cmd =
      cmd
      |> List.wrap()
      |> Enum.join(" ; ")

    run("docker", ["exec", @container_name, "bash", "-c", cmd], opts)
  end

  def docker_cp(from, to) do
    run("docker", ["cp", from, @container_name <> ":" <> to])
  end

  def run(command, args, opts \\ []) do
    {out, exit_code} = System.cmd(command, args, opts)

    if exit_code != 0 do
      {:error, exit_code, out}
    else
      {:ok, out}
    end
  end

  def kw_update_some(kw, key, fun) do
    if Keyword.has_key?(kw, key) do
      Keyword.update!(kw, key, fun)
    else
      kw
    end
  end

  def parse_exunit_opts(argv) do
    {opts, _pos_args} = OptionParser.parse!(argv, strict: [only: :keep])

    opts
    |> parse_only()
  end

  defp parse_only(opts) do
    if filters = parse_filters(opts, :only) do
      opts
      |> Keyword.update(:include, filters, &(filters ++ &1))
      |> Keyword.update(:exclude, [:test], &[:test | &1])
    else
      opts
    end
  end

  defp parse_filters(opts, key) do
    if Keyword.has_key?(opts, key) do
      opts
      |> Keyword.get_values(key)
      |> ExUnit.Filters.parse()
    end
  end

  @doc """
  If the api user needs to be recreated, one may execute:

  ```erlang
  emqx_mgmt_auth:add_app(
    <<"app_id">>,
    <<"app_name">>,
    <<"4mVZvVT9CnC6Z3AYdk9C07Ecz9AuBCLblb43kk69BcxbBhP">>,
    <<"some description">>,
    _Status = true,
    _Expired = undefined).
  ```
  """
  def api_req!(method, path, body \\ "", _opts \\ []) do
    api_user = "app_id"
    api_pass = "4mVZvVT9CnC6Z3AYdk9C07Ecz9AuBCLblb43kk69BcxbBhP"
    authn64 = Base.encode64("#{api_user}:#{api_pass}")

    HTTPoison.request!(
      method,
      "http://localhost:#{@api_port}/api/v5/#{path}",
      body,
      [{"Authorization", "Basic #{authn64}"}]
    )
    |> Map.update!(:body, fn body ->
      case Jason.decode(body) do
        {:ok, json} ->
          json

        _ ->
          body
      end
    end)
  end
end

opts = System.argv() |> TH.parse_exunit_opts()

Mix.install([{:httpoison, "2.2.1"}, {:jason, "1.4.4"}])

ExUnit.start(opts)

defmodule Tests do
  use ExUnit.Case

  require Logger

  @moduledoc """
  Exploring locally:

      image = "emqx/emqx-enterprise:5.8.0"
      TH.start_container(image)
      outdir = "/opt/data/converted"
      path = "test/data/barebones.json"
      {:ok, converted_path} = TH.convert!(path)

  After this, `docker exec -it emqx-data-converter bash`

  Or:

      TH.import_table("emqx_app", outdir)
  """

  def delete_all_of_resouce(api_slug) do
    TH.api_req!(:get, api_slug)
    |> Map.fetch!(:body)
    |> Enum.map(fn %{"name" => name, "type" => type} ->
      TH.api_req!(:delete, api_slug <> "/#{type}:#{name}")
    end)
  end

  def delete_all_actions(), do: delete_all_of_resouce("actions")
  def delete_all_connectors(), do: delete_all_of_resouce("connectors")

  def delete_all_rules() do
    api_slug = "rules"

    TH.api_req!(:get, api_slug)
    |> Map.fetch!(:body)
    |> Map.fetch!("data")
    |> Enum.map(fn %{"id" => id} ->
      TH.api_req!(:delete, api_slug <> "/#{id}")
    end)
  end

  def cleanup_bridges_and_rules() do
    delete_all_rules()
    delete_all_actions()
    delete_all_connectors()
  end

  setup_all do
    {:ok, %{image: System.fetch_env!("EMQX_IMAGE")}}
  end

  setup %{image: image} do
    TH.stop_container()
    on_exit(&TH.stop_container/0)
    {:ok, _} = TH.start_container(image)
    :ok
  end

  test "barebones" do
    path = "test/data/barebones.json"
    {:ok, converted_path} = TH.convert!(path)
    on_exit(fn -> File.rm(converted_path) end)
    :ok = TH.import!(converted_path)
  end

  test "mnesia, redis, postgres authn/authz" do
    path = "test/data/auth-builtin-redis-postgres1.json"
    {:ok, converted_path} = TH.convert!(path)
    on_exit(fn -> File.rm(converted_path) end)
    :ok = TH.import!(converted_path)

    # import application should start working
    resp = TH.api_req!(:get, "mqtt/retainer/messages")

    assert %HTTPoison.Response{status_code: 200} = resp
  end

  # Retained messages from ram copies.  Has 1 "real" message and 3 system messages.
  test "retained messages : ram" do
    path = "test/data/auth-builtin-redis-postgres1.json"
    {:ok, converted_path} = TH.convert!(path, data_files_dir: "test/data/retainer_ram1")
    on_exit(fn -> File.rm(converted_path) end)
    :ok = TH.import!(converted_path)

    assert {:ok, _} = TH.run_in_container("docker-entrypoint.sh emqx ctl retainer reindex start")

    resp = TH.api_req!(:get, "mqtt/retainer/messages")

    assert %HTTPoison.Response{status_code: 200} = resp

    assert %{"data" => [_, _, _, _]} = resp.body
  end

  # A few rules that send data to bridges.
  #   - kafka
  #   - mqtt
  #   - postgres
  #   - http
  #   - gcp pubsub producer
  #   - redis (3 types)
  #   - republish
  #   - debug (inspect)
  @tag :bridges
  test "bridges 1" do
    path = "test/data/bridges1.json"
    {:ok, converted_path} = TH.convert!(path)
    on_exit(fn -> File.rm(converted_path) end)
    :ok = TH.import!(converted_path)

    on_exit(&cleanup_bridges_and_rules/0)

    connectors =
      TH.api_req!(:get, "connectors")
      |> Map.fetch!(:body)

    actions =
      TH.api_req!(:get, "actions")
      |> Map.fetch!(:body)

    expected_connector_types =
      MapSet.new([
        "kafka_producer",
        "mqtt",
        "pgsql",
        "http",
        "gcp_pubsub_producer",
        "mongodb",
        "cassandra",
        "pulsar",
        "clickhouse",
        "hstreamdb",
        "tdengine",
        "rabbitmq",
        "dynamo",
        "influxdb",
        "rocketmq",
        "opents",
        "redis"
      ])

    assert connectors |> Enum.map(& &1["type"]) |> Enum.into(MapSet.new()) ==
             expected_connector_types

    #   - kafka
    #   - mqtt
    #   - postgres
    #   - http
    #   - gcp pubsub producer
    #   - redis (3 types)
    #   - mongodb (3 types)
    #   - cassandra
    #   - pulsar
    #   - clickhouse
    #   - hstreamdb
    #   - tdengine
    #   - rabbitmq
    #   - dynamo
    #   - influxdb (2 types)
    #   - rocketmq (2 variants, but both share the same connector config, hence 1 connector)
    #   - opentsdb
    num_actions = 23

    #   - rocketmq (2 variants, but both share the same connector config, hence 1 connector)
    assert length(connectors) == num_actions - 1

    redis_types =
      connectors
      |> Enum.filter(&(&1["type"] == "redis"))
      |> Enum.map(&get_in(&1, ["parameters", "redis_type"]))
      |> MapSet.new()

    assert redis_types == MapSet.new(["single", "cluster", "sentinel"])

    expected_action_types =
      MapSet.new([
        "kafka_producer",
        "mqtt",
        "pgsql",
        "http",
        "gcp_pubsub_producer",
        "mongodb",
        "cassandra",
        "pulsar",
        "clickhouse",
        "hstreamdb",
        "tdengine",
        "rabbitmq",
        "dynamo",
        "influxdb",
        "rocketmq",
        "opents",
        "redis"
      ])

    assert actions |> Enum.map(& &1["type"]) |> Enum.into(MapSet.new()) == expected_action_types
    assert length(actions) == num_actions

    redis_types =
      actions
      |> Enum.filter(&(&1["type"] == "redis"))
      |> Enum.map(&get_in(&1, ["parameters", "redis_type"]))
      |> MapSet.new()

    assert redis_types == MapSet.new(["single", "cluster", "sentinel"])

    # naming convention: `c-<6 chars>` for connectors, `a-<6 chars>` for actions, `s-<6
    # chars>` for sources.
    connector_names_unconventional =
      connectors
      |> Enum.map(& &1["name"])
      |> Enum.reject(&(&1 =~ ~r/^c-[a-z0-9]{6}/))

    assert [] == connector_names_unconventional

    action_names_unconventional =
      actions
      |> Enum.map(& &1["name"])
      |> Enum.reject(&(&1 =~ ~r/^a-[a-z0-9]{6}/))

    assert [] == action_names_unconventional
  end

  # Checks that, if possible, attempts to reuse connectors with the same configuration
  # when multiple rules reference the same config/resource.
  @tag :bridges
  test "connector deduplication" do
    path = "test/data/multiple_rules_same_connector1.json"
    {:ok, converted_path} = TH.convert!(path)
    on_exit(fn -> File.rm(converted_path) end)
    :ok = TH.import!(converted_path)

    on_exit(&cleanup_bridges_and_rules/0)

    connectors =
      TH.api_req!(:get, "connectors")
      |> Map.fetch!(:body)

    actions =
      TH.api_req!(:get, "actions")
      |> Map.fetch!(:body)

    assert [_, _, _] = actions
    # only one connector, as it has the same configuration in 4.x
    assert [%{"actions" => [_, _, _]}] = connectors
  end

  # Checks that we convert mqtt sources
  @tag :bridges
  @tag :sources
  test "mqtt sources" do
    path = "test/data/mqtt_source.json"
    {:ok, converted_path} = TH.convert!(path)
    on_exit(fn -> File.rm(converted_path) end)
    :ok = TH.import!(converted_path)

    on_exit(&cleanup_bridges_and_rules/0)

    connectors =
      TH.api_req!(:get, "connectors")
      |> Map.fetch!(:body)

    sources =
      TH.api_req!(:get, "sources")
      |> Map.fetch!(:body)

    rules =
      TH.api_req!(:get, "rules")
      |> Map.fetch!(:body)
      |> Map.fetch!("data")

    # naming convention: `c-<6 chars>` for connectors, `a-<6 chars>` for actions, `s-<6
    # chars>` for sources.
    connector_names_unconventional =
      connectors
      |> Enum.map(& &1["name"])
      |> Enum.reject(&(&1 =~ ~r/^c-[a-z0-9]{6}/))

    assert [] == connector_names_unconventional

    source_names_unconventional =
      sources
      |> Enum.map(& &1["name"])
      |> Enum.reject(&(&1 =~ ~r/^s-[a-z0-9]{6}/))

    assert [] == source_names_unconventional

    # this fixture contains one mqtt source (at most one can exist in 4.4), and one mqtt
    # connector for an action
    assert [_, _] = connectors

    # we should have one source for each topic in the 4.4 subscriber
    assert [_, _] = sources
    referenced_connectors = sources |> MapSet.new(& &1["connector"])
    assert [%{}] = Enum.filter(connectors, &(&1["name"] in referenced_connectors))

    # also one rule that selects from all sources, plus an action's rule
    assert [_, _] = rules

    assert [%{"from" => [_, _], "actions" => [%{"function" => "republish"}]}] =
             rules |> Enum.filter(&(&1["actions"] |> hd() |> is_map()))
  end
end
