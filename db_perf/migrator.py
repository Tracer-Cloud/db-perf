import subprocess
from pathlib import Path


class DatabaseMigrator:
    def __init__(self, database_url, migration_folder: Path):
        self.database_url = database_url
        self.migration_folder = migration_folder

    def _check_sqlx_installed(self):
        # Check if sqlx is installed
        try:
            subprocess.run(
                ["sqlx", "--version"],
                check=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
        except subprocess.CalledProcessError:
            print("sqlx not found. Installing...")
            subprocess.run(
                [
                    "cargo",
                    "install",
                    "sqlx-cli",
                    "--no-default-features",
                    "--features",
                    "postgres",
                ],
                check=True,
            )

    def run_migrations(self):
        # Check if sqlx is installed
        self._check_sqlx_installed()

        print(f"Using database URL: {self.database_url}")
        print(f"Running migrations from {self.migration_folder}...")

        # Run migrations using sqlx
        subprocess.run(
            [
                "sqlx",
                "migrate",
                "run",
                "--database-url",
                self.database_url,
                "--source",
                self.migration_folder,
            ],
            check=True,
        )

        print("Migration completed successfully!")

    def rollback_migrations(self):
        # Check if sqlx is installed
        self._check_sqlx_installed()

        print(f"Using database URL: {self.database_url}")
        print("Recreating database...")

        # Resert db
        subprocess.run(
            [
                "sqlx",
                "database",
                "drop",
                "--database-url",
                self.database_url,
                "--force",
                "-y",
            ],
            check=True,
        )

        subprocess.run(
            [
                "sqlx",
                "database",
                "create",
                "--database-url",
                self.database_url,
            ],
            check=True,
        )
        print("Migration rollback successfully!")
