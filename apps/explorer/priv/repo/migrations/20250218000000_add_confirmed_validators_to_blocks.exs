defmodule Explorer.Repo.Migrations.AddConfirmedValidatorsToBlocks do
  use Ecto.Migration

  def change do
    alter table(:blocks) do
      add(:confirmed_validator, :integer)
    end
  end
end
