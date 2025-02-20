defmodule Explorer.Repo.Migrations.AddValidatorCountToBlocks do
  use Ecto.Migration

  def change do
    alter table(:blocks) do
      add :validator_counts, :integer
    end

    # Optional: Add an index if you plan to query by validator_count
    create index(:blocks, [:validator_count])
  end
end
