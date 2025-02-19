defmodule Explorer.Repo.Migrations.AddConfirmedValidatorToBlocks do
  use Ecto.Migration

  def change do
    alter table(:blocks) do
      add :confirmed_validator, :integer, null: true
    end
  end
end
