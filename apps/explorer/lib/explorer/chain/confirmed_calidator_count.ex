defmodule Explorer.Chain.Block.ConfirmedValidatorCount do
  @moduledoc """
  Manages the confirmed validator count for blocks.
  """

  require Logger

  alias Explorer.Chain.Block
  alias Explorer.Repo
  import Ecto.Query

  defp log_info(message), do: IO.puts("INFO: #{message}")
  defp log_error(message), do: IO.puts(:stderr, "ERROR: #{message}")
  defp log_debug(message), do: IO.puts("DEBUG: #{message}")
  defp log_warn(message), do: IO.puts("WARNING: #{message}")


  @doc """
  Fetches validator count for a given block number using istanbul_getValidators
  """
  def fetch_confirmed_validator_count(block_number) do
    params = [
      %{
        id: 1,
        method: "istanbul_getValidators",
        params: [Integer.to_string(block_number, 16)]
      }
    ]

    case EthereumJSONRPC.json_rpc(params) do
      {:ok, %{result: validators}} when is_list(validators) ->
        count = length(validators)
        log_info("Block ##{block_number} validator count: #{count}")
        {:ok, count}

      {:error, reason} = error ->
        log_error("Failed to fetch validators for block ##{block_number}: #{inspect(reason)}")
        error

      other ->
        log_error("Unexpected response for block ##{block_number}: #{inspect(other)}")
        {:error, :invalid_response}
    end
  end

  @doc """
  Updates validator count for a single block
  """
  def update_confirmed_validator_count(%Block{} = block) do
    log_info("Updating validator count for block ##{block.number}")

    Repo.transaction(fn ->
      case fetch_confirmed_validator_count(block.number) do
        {:ok, validator_count} ->
          case block
               |> Block.confirmed_validator_count_changeset(%{confirmed_validator_count: validator_count})
               |> Repo.update() do
            {:ok, updated_block} ->
              log_info("Successfully updated validator count for block ##{block.number}: #{validator_count}")
              updated_block

            {:error, changeset} ->
              error_message = "Failed to update validator count: #{inspect(changeset.errors)}"
              log_error("#{error_message} for block ##{block.number}")
              Repo.rollback({:update_failed, error_message})
          end

        {:error, :invalid_response} ->
          error_message = "Invalid response in validator fetch"
          log_error("#{error_message} for block ##{block.number}")
          Repo.rollback({:invalid_response, error_message})

        {:error, reason} ->
          error_message = "Failed in validator fetch: #{inspect(reason)}"
          log_error("#{error_message} for block ##{block.number}")
          Repo.rollback({:fetch_failed, error_message})
      end
    end)
  end

  @doc """
  Updates validator counts for multiple blocks efficiently
  """
  def update_confirmed_validator_counts(block_numbers) when is_list(block_numbers) do
    total_blocks = length(block_numbers)
    log_info("Starting batch update of validator counts for #{total_blocks} blocks")

    block_numbers
    |> Enum.chunk_every(50)
    |> Enum.with_index(1)
    |> Enum.each(fn {chunk, batch_num} ->
      log_info("Processing batch #{batch_num} with #{length(chunk)} blocks")

      blocks = from(b in Block, where: b.number in ^chunk)
        |> Repo.all()

      results = Enum.map(blocks, fn block ->
        {block.number, update_confirmed_validator_count(block)}
      end)

      # Log batch results
      {successes, failures} = Enum.split_with(results, fn {_, result} -> match?({:ok, _}, result) end)

      log_info("Batch #{batch_num} completed - Successes: #{length(successes)}, Failures: #{length(failures)}")

      if length(failures) > 0 do
        failed_blocks = Enum.map(failures, fn {number, _} -> number end)
        log_warn("Failed blocks in batch #{batch_num}: #{inspect(failed_blocks)}")
      end
    end)

    log_info("Completed all validator count updates")
  end
end
