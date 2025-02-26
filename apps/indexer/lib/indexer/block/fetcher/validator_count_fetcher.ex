defmodule Indexer.Block.ValidatorCountFetcher do
  @moduledoc """
  Fetches and updates validator counts for blocks in batches.
  Uses Spandex for tracing and follows Block.Fetcher patterns.
  """

  use GenServer
  use Spandex.Decorators
  use Utils.CompileTimeEnvHelper, chain_type: [:explorer, :chain_type]

  require Logger

  alias Explorer.Chain
  alias Explorer.Chain.Block
  alias Explorer.Chain.Cache.BlockNumber
  alias Indexer.{Prometheus, Tracer}

  @fetch_interval :timer.minutes(1)
  @batch_size 100
  @batch_interval_ms 200
  @max_concurrent_tasks 5
  @retry_interval :timer.minutes(5)
  @max_retries 3

  def start_link(args) do
    GenServer.start_link(__MODULE__, args, name: __MODULE__)
  end

  def init(args) do
    schedule_fetch()
    {:ok, %{json_rpc_named_arguments: args, running: false, retry_counts: %{}}}
  end

  def handle_info(:fetch, %{running: true} = state) do
    schedule_fetch()
    {:noreply, state}
  end

  @decorate span(tracer: Tracer)
  def handle_info(:fetch, %{json_rpc_named_arguments: json_rpc_named_arguments} = state) do
    Task.start(fn ->
      {fetch_time, result} = :timer.tc(fn ->
        process_blocks_without_validator_count(json_rpc_named_arguments)
      end)

      # Prometheus.Instrumenter.block_validator_count_fetch(fetch_time, __MODULE__)

      case result do
        {:ok, inserted} ->
          update_block_cache(inserted[:blocks])
        _ -> :ok
      end
    end)

    schedule_fetch()
    {:noreply, %{state | running: true}}
  end

  def handle_info({:retry_block, block_number}, state) do
    %{retry_counts: retry_counts, json_rpc_named_arguments: json_rpc_named_arguments} = state

    case Map.get(retry_counts, block_number, 0) do
      count when count >= @max_retries ->
        Logger.error(fn -> ["Max retries exceeded for block ", to_string(block_number)] end)
        {:noreply, %{state | retry_counts: Map.delete(retry_counts, block_number)}}

      count ->
        Task.start(fn ->
          process_single_block(block_number, json_rpc_named_arguments)
        end)

        {:noreply, %{state |
          retry_counts: Map.put(retry_counts, block_number, count + 1)
        }}
    end
  end

  defp schedule_fetch do
    Process.send_after(self(), :fetch, @fetch_interval)
  end

  @decorate span(tracer: Tracer)
  defp process_blocks_without_validator_count(json_rpc_named_arguments) do
    try do
      blocks = Block.blocks_without_validator_count_query()
      |> Chain.list_blocks()

      results = blocks
      |> Enum.chunk_every(@batch_size)
      |> Task.async_stream(
        &process_batch(&1, json_rpc_named_arguments),
        max_concurrency: @max_concurrent_tasks,
        timeout: :timer.minutes(5)
      )
      |> Enum.reduce(
        %{blocks: []},
        fn {:ok, batch_results}, acc ->
          Map.update!(acc, :blocks, &(&1 ++ batch_results))
        end
      )

      import_validator_counts(results)
    rescue
      e ->
        Logger.error(fn -> ["Error processing blocks without validator counts: ", Exception.format(:error, e, __STACKTRACE__)] end)
        {:error, e}
    end
  end

  @decorate span(tracer: Tracer)
  defp process_batch(blocks, json_rpc_named_arguments) do
    Enum.map(blocks, fn block ->
      # Add retry delay between requests
      Process.sleep(50)  # Add small delay between RPC calls

      result = Chain.Block.fetch_confirmed_validator_count(block.number, json_rpc_named_arguments)

      case result do
        {:ok, count} when is_integer(count) and count >= 0 ->
          Logger.info("Successfully fetched validator count",
            block_number: block.number,
            count: count
          )
          %{
            hash: block.hash,
            number: block.number,
            confirmed_validator_count: count
          }

        {:ok, invalid_count} ->
          Logger.error("Invalid validator count received",
            block_number: block.number,
            response: inspect(invalid_count),
            hash: block.hash
          )
          schedule_retry(block.number)
          nil

        {:error, %{"code" => code, "message" => message}} ->
          Logger.error("RPC error while fetching validator count",
            block_number: block.number,
            error_code: code,
            error_message: message,
            hash: block.hash
          )
          schedule_retry(block.number)
          nil

        {:error, reason} ->
          Logger.error("Error fetching validator count",
            block_number: block.number,
            error: inspect(reason),
            hash: block.hash
          )
          schedule_retry(block.number)
          nil

        unexpected ->
          Logger.error("Unexpected RPC response",
            block_number: block.number,
            response: inspect(unexpected),
            hash: block.hash,
            type: inspect(unexpected)
          )
          schedule_retry(block.number)
          nil
      end
    end)
    |> Enum.reject(&is_nil/1)
  end

  @decorate span(tracer: Tracer)
  defp process_single_block(block_number, json_rpc_named_arguments) do
    with {:ok, count} <- Chain.Block.fetch_confirmed_validator_count(block_number, json_rpc_named_arguments),
         {:ok, block} <- Chain.get_block_by_number(block_number, false) do

      results = [%{
        hash: block.hash,
        number: block_number,
        confirmed_validator_count: count
      }]

      import_validator_counts(%{blocks: results})
    else
      {:error, reason} ->
        Logger.error(fn -> ["Error processing block ", to_string(block_number), ": ", inspect(reason)] end)
        schedule_retry(block_number)
    end
  end

  defp import_validator_counts(%{blocks: blocks}) do
    # Import using Chain.import like Block.Fetcher
    Chain.import(%{
      blocks: %{params: blocks},
      timeout: :infinity
    })
  end

  defp schedule_retry(block_number) do
    Process.send_after(self(), {:retry_block, block_number}, @retry_interval)
  end

  # Cache handling copied from Block.Fetcher
  defp update_block_cache([]), do: :ok

  defp update_block_cache(blocks) when is_list(blocks) do
    {min_block, max_block} = Enum.min_max_by(blocks, & &1.number)

    BlockNumber.update_all(max_block.number)
    BlockNumber.update_all(min_block.number)
  end

  defp update_block_cache(_), do: :ok
end
