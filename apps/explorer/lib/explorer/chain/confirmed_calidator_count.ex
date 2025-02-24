defmodule Explorer.Chain.Block.ConfirmedValidatorCount do
  @moduledoc """
  Manages the confirmed validator count for blocks.

  This module provides functionality to:
  - Fetch validator counts from the blockchain
  - Update validator counts for individual blocks
  - Batch update validator counts for multiple blocks
  """

  alias Explorer.Chain.Block
  alias Explorer.Repo
  alias Explorer.EthRPC
  import Ecto.Query
  require Logger

  @type block_number :: non_neg_integer()
  @type update_result :: {:ok, Block.t()} | {:error, term()}
  @type batch_result :: {non_neg_integer(), [term()]}

  @batch_size 50
  @max_concurrency 5
  @timeout :timer.minutes(5)

  @doc """
  Fetches validator count for a given block number using istanbul_getValidators RPC method.

  ## Parameters
    * `block_number` - The block number to fetch validators for

  ## Returns
    * `{:ok, count}` - The number of validators for the block
    * `{:error, reason}` - If the fetch operation fails

  ## Examples
      iex> fetch_confirmed_validator_count(12345)
      {:ok, 4}

      iex> fetch_confirmed_validator_count(99999)
      {:error, "block not found"}
  """
  @spec fetch_confirmed_validator_count(block_number()) :: {:ok, non_neg_integer()} | {:error, term()}
  def fetch_confirmed_validator_count(block_number) do
    hex_block = "0x" <> Integer.to_string(block_number, 16)

    params = [
      %{
        "id" => 1,
        "jsonrpc" => "2.0",
        "method" => "istanbul_getValidators",
        "params" => [hex_block]  # 블록 번호를 16진수로 변환
      }
    ]

    try do
      case EthRPC.responses(params) do
        [%{result: validators}] when is_list(validators) ->
          count = length(validators)
          Logger.info("Block #{block_number} validator count: #{count}")
          {:ok, count}

        [%{error: %{code: code, message: message}}] ->  # 에러 응답 구조 수정
          Logger.error("Failed to fetch validators",
            block_number: block_number,
            error_code: code,
            error_message: message
          )
          {:error, message}

        other ->
          Logger.error("Unexpected response",
            block_number: block_number,
            response: inspect(other)
          )
          {:error, :invalid_response}
      end
    rescue
      e ->
        Logger.error("Exception while fetching validators",
          block_number: block_number,
          error: inspect(e),
          stacktrace: __STACKTRACE__
        )
        {:error, e}
    end
  end

  @doc """
  Updates validator count for a single block.

  ## Parameters
    * `block` - The Block struct to update

  ## Returns
    * `{:ok, block}` - Updated block with new validator count
    * `{:error, reason}` - If the update operation fails

  ## Examples
      iex> update_confirmed_validator_count(%Block{number: 12345})
      {:ok, %Block{number: 12345, confirmed_validator_count: 4}}
  """
  @spec update_confirmed_validator_count(Block.t()) :: update_result()
  def update_confirmed_validator_count(%Block{} = block) do
    Logger.info("Updating validator count for block ##{block.number}")

    Repo.transaction(fn ->
      case fetch_confirmed_validator_count(block.number) do
        {:ok, validator_count} ->
          case block
               |> Block.confirmed_validator_count_changeset(%{confirmed_validator_count: validator_count})
               |> Repo.update() do
            {:ok, updated_block} ->
              Logger.info("Successfully updated validator count",
                block_number: block.number,
                count: validator_count
              )
              updated_block

            {:error, changeset} ->
              Logger.error("Failed to update validator count",
                block_number: block.number,
                errors: inspect(changeset.errors)
              )
              Repo.rollback({:update_failed, changeset.errors})
          end

        {:error, reason} ->
          Logger.error("Failed to fetch validator count",
            block_number: block.number,
            error: inspect(reason)
          )
          Repo.rollback({:fetch_failed, reason})
      end
    end)
  end

  @doc """
  Updates validator counts for multiple blocks efficiently in batches.

  ## Parameters
    * `block_numbers` - List of block numbers to update

  ## Returns
    * `{successful_count, errors}` - Tuple with count of successful updates and list of errors

  ## Examples
      iex> update_confirmed_validator_counts([12345, 12346, 12347])
      {3, []}

      iex> update_confirmed_validator_counts([12345, 99999])
      {1, [{99999, {:error, "block not found"}}]}
  """
  @spec update_confirmed_validator_counts([block_number()]) :: batch_result()
  def update_confirmed_validator_counts(block_numbers) when is_list(block_numbers) do
    Logger.info("Starting batch update for #{length(block_numbers)} blocks")

    Repo.transaction(fn ->
      block_numbers
      |> Enum.chunk_every(@batch_size)
      |> Task.async_stream(
        fn chunk ->
          Enum.map(chunk, fn number ->
            case fetch_confirmed_validator_count(number) do
              {:ok, count} ->
                query = from(b in Block, where: b.number == ^number)
                case Repo.update_all(query, set: [confirmed_validator_count: count]) do
                  {1, nil} -> {:ok, number}
                  _ -> {:error, "Block update failed"}
                end
              error -> {:error, error}
            end
          end)
        end,
        max_concurrency: @max_concurrency,
        timeout: @timeout
      )
      |> Enum.reduce({0, []}, fn
        {:ok, results}, {success, errors} ->
          {ok, err} = Enum.split_with(results, &match?({:ok, _}, &1))
          {success + length(ok), errors ++ err}
        {:error, reason}, {success, errors} ->
          {success, errors ++ [reason]}
      end)
    end)
    |> case do
      {:ok, result} -> result
      {:error, reason} -> {0, [reason]}
    end
  end
end
