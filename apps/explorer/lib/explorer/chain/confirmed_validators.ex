defmodule Explorer.Chain.Block.Confirmed_Validator_Count do
  @moduledoc """
  By CROSS
  ADD Block Data - Confirmed Validators Count Per Block
  """

  require Logger

  alias Explorer.Chain.Block
  alias Explorer.Repo
  import Ecto.Query

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
      # {:ok, %{result: validators}} when is_list(validators) -> {:ok, length(validators)}
      # {:error, reason} -> {:error, reason}
      # _ -> {:error, :invalid_response}
      {:ok, %{result: validators}} when is_list(validators) ->
        count = length(validators)
        Logger.info(fn -> "Block ##{block_number} validator count: #{count}" end)
        {:ok, count}

      {:error, reason} = error ->
        Logger.error(fn -> "Failed to fetch validators for block ##{block_number}: #{inspect(reason)}" end)
        error

      other ->
        Logger.error(fn -> "Unexpected response for block ##{block_number}: #{inspect(other)}" end)
        {:error, :invalid_response}
    end
  end

  @doc """
  Updates validator count for a single block
  """
  def update_confirmed_validator_count(%Block{} = block) do
    Logger.info(fn -> "Updating validator count for block ##{block.number}" end)

    # with {:ok, confirmed_validator_count} <- fetch_confirmed_validator_count(block.number) do
    #   block
    #   |> Block.confirmed_validator_count_changeset(%{confirmed_validator_count: confirmed_validator_count})
    #   |> Repo.update()

    case fetch_confirmed_validator_count(block.number) do
      {:ok, validator_count} ->
        result = block
        |> Block.validator_count_changeset(%{confirmed_validator_count: validator_count})
        |> Repo.update()

        case result do
          {:ok, updated_block} ->
            Logger.info(fn -> "Successfully updated validator count for block ##{block.number}: #{validator_count}" end)
            {:ok, updated_block}

          {:error, changeset} = error ->
            Logger.error(fn -> "Failed to update validator count for block ##{block.number}: #{inspect(changeset.errors)}" end)
            error
        end

      {:error, _} = error ->
        Logger.error(fn -> "Failed in validator fetch for block ##{block.number}" end)
        error
    end
  end

  @doc """
  Updates validator counts for multiple blocks efficiently
  """
  def update_confirmed_validator_counts(block_numbers) when is_list(block_numbers) do
    Logger.info(fn -> "Starting batch update of validator counts for #{length(block_numbers)} blocks" end)
    # block_numbers
    # |> Enum.chunk_every(50)  # Process in batches to avoid overloading
    # |> Enum.each(fn chunk ->
    #   # Fetch blocks
    #   blocks = from(b in Block, where: b.number in ^chunk)
    #     |> Repo.all()

    #   # Update each block's validator count
    #   Enum.each(blocks, &update_confirmed_validator_count/1)
    # end)
    block_numbers
    |> Enum.chunk_every(50)
    |> Enum.with_index(1)
    |> Enum.each(fn {chunk, batch_num} ->
      Logger.info(fn -> "Processing batch #{batch_num} with #{length(chunk)} blocks" end)

      blocks = from(b in Block, where: b.number in ^chunk)
        |> Repo.all()

      Enum.each(blocks, fn block ->
        case update_confirmed_validator_count(block) do
          {:ok, _} ->
            Logger.debug(fn -> "Batch #{batch_num}: Successfully updated block ##{block.number}" end)
          {:error, reason} ->
            Logger.warn(fn -> "Batch #{batch_num}: Failed to update block ##{block.number}: #{inspect(reason)}" end)
        end
      end)

      Logger.info(fn -> "Completed batch #{batch_num}" end)
    end)

    Logger.info(fn -> "Completed all validator count updates" end)
  end
end
