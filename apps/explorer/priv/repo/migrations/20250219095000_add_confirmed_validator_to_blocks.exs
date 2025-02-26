defmodule Explorer.Repo.Migrations.AddValidatorCountToBlocks do
  use Ecto.Migration

  def change do
    alter table(:blocks) do
      add :confirmed_validator_count, :integer
    end

    # Optional: Add an index if you plan to query by validator_count
    create index(:blocks, [:confirmed_validator_count])
  end
end
