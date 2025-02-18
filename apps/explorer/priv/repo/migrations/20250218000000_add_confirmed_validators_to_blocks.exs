defmodule Explorer.Repo.Migrations.AddConfirmedValidatorsToBlocks do
  use Ecto.Migration

  def change do
    alter table(:blocks) do
      add(:confirmed_validators, :integer)
    end
  end
end
