defmodule Explorer.Chain.Block.Confirmed_Validators do
  @moduledoc """
  By CROSS
  ADD Block Data - Confirmed Validators Count Per Block
  """

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
      {:ok, %{result: validators}} when is_list(validators) -> {:ok, length(validators)}
      {:error, reason} -> {:error, reason}
      _ -> {:error, :invalid_response}
    end
  end

  @doc """
  Updates validator count for a single block
  """
  def update_confirmed_validator_count(%Block{} = block) do
    with {:ok, confirmed_validator_count} <- fetch_confirmed_validator_count(block.number) do
      block
      |> Block.confirmed_validator_count_changeset(%{confirmed_validator_count: confirmed_validator_count})
      |> Repo.update()
    end
  end

  @doc """
  Updates validator counts for multiple blocks efficiently
  """
  def update_confirmed_validator_count(block_numbers) when is_list(block_numbers) do
    block_numbers
    |> Enum.chunk_every(50)  # Process in batches to avoid overloading
    |> Enum.each(fn chunk ->
      # Fetch blocks
      blocks = from(b in Block, where: b.number in ^chunk)
        |> Repo.all()

      # Update each block's validator count
      Enum.each(blocks, &update_confirmed_validator_count/1)
    end)
  end
end
