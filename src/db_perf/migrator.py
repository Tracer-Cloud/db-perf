import subprocess

class DatabaseMigrator:
    def __init__(self, database_url, migration_folder: str):
        self.database_url = database_url 
        self.migration_folder = migration_folder


    def _check_sqlx_installed(self):
        # Check if sqlx is installed
        try:
            subprocess.run(["sqlx", "--version"], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        except subprocess.CalledProcessError:
            print("sqlx not found. Installing...")
            subprocess.run(["cargo", "install", "sqlx-cli", "--no-default-features", "--features", "postgres"], check=True)

    def run_migrations(self):
        # Check if sqlx is installed
        self._check_sqlx_installed()
        
        print(f"Using database URL: {self.database_url}")
        print(f"Running migrations from {self.migration_folder}...")

        # Run migrations using sqlx
        subprocess.run(["sqlx", "migrate", "run", "--database-url", self.database_url, "--migration-dir", self.migration_folder], check=True)

        print("Migration completed successfully!")


