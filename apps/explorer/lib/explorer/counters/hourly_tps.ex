defmodule Explorer.Counters.HourlyTps do
  @moduledoc """
  by CROSS
  TPS(Transactions Per Second) 측정을 위한 모듈입니다.
  """
  use GenServer
  import Ecto.Query
  alias Explorer.{Chain, Repo}
  alias Explorer.Chain.Transaction

  @hourly_tps_name "hourly_tps"

  @doc """
  Starts a process to periodically update the TPS counter.
  """
  @spec start_link(term()) :: GenServer.on_start()
  def start_link(_) do
    GenServer.start_link(__MODULE__, :ok, name: __MODULE__)
  end

  @impl true
  def init(_args) do
    {:ok, %{consolidate?: enable_consolidation?()}, {:continue, :ok}}
  end

  defp schedule_next_consolidation do
    Process.send_after(self(), :consolidate, cache_interval())
  end

  @impl true
  def handle_continue(:ok, %{consolidate?: true} = state) do
    consolidate()
    schedule_next_consolidation()
    {:noreply, state}
  end

  @impl true
  def handle_continue(:ok, state) do
    {:noreply, state}
  end

  @impl true
  def handle_info(:consolidate, state) do
    consolidate()
    schedule_next_consolidation()
    {:noreply, state}
  end

  @doc """
  Fetches the current TPS value from the `last_fetched_counters` table.
  """
  def fetch_count(options) do
    Chain.get_last_fetched_counter(@hourly_tps_name, options)
  end

  @doc """
  현재 TPS를 계산하고 저장합니다.
  1시간 평균 TPS (소수점 2자리)
  """
  def consolidate do
    one_hour_ago = DateTime.utc_now() |> DateTime.add(-3600, :second)

    query =
      from(transaction in Transaction,
        join: block in assoc(transaction, :block),
        where: transaction.block_timestamp >= ^one_hour_ago,
        select: %{
          count: count(transaction.hash)
        }
      )

    %{count: count} = Repo.one!(query, timeout: :infinity)

    # TempLog
    IO.puts("One hour ago: #{one_hour_ago}")

    # TempLog
    IO.puts("Transaction count in last hour: #{count}")

    # 1시간(3600초)동안의 총 트랜잭션을 3600으로 나누어 초당 평균을 계산
    tps = count
      |> Decimal.new()
      |> Decimal.div(Decimal.new(3600))
      |> Decimal.round(2)  # 소수점 2자리까지 반올림

    # TempLog
    IO.puts("Calculated TPS: #{tps}")

    Chain.upsert_last_fetched_counter(%{
      counter_type: @hourly_tps_name,
      value: tps
    })
  end

  @doc """
  Returns a boolean that indicates whether consolidation is enabled
  """
  def enable_consolidation?, do: Application.get_env(:explorer, __MODULE__)[:enable_consolidation]

  defp cache_interval, do: Application.get_env(:explorer, __MODULE__)[:cache_period]
end
