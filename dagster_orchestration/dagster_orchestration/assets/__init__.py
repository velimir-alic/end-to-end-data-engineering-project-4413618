import os
from pathlib import Path

from dagster import AssetExecutionContext
from dagster_airbyte import AirbyteWorkspace, DagsterAirbyteTranslator, build_airbyte_assets_definitions
from dagster_dbt import DagsterDbtTranslator, DbtCliResource, dbt_assets


# Airbyte configuration
AIRBYTE_HOST = os.getenv("AIRBYTE_HOST", "localhost")
AIRBYTE_PORT = os.getenv("AIRBYTE_PORT", "8000")
AIRBYTE_WORKSPACE_ID = os.getenv("AIRBYTE_WORKSPACE_ID")
AIRBYTE_CLIENT_ID = os.getenv("AIRBYTE_CLIENT_ID")
AIRBYTE_CLIENT_SECRET = os.getenv("AIRBYTE_CLIENT_SECRET")

# Get dbt directories from environment
DBT_PROJECT_DIR = os.getenv("DBT_PROJECT_DIR")
DBT_PROFILES_DIR = os.getenv("DBT_PROFILES_DIR")

# Path to the manifest file
DBT_MANIFEST = Path(DBT_PROJECT_DIR) / "target" / "manifest.json"


# Airbyte instance
airbyte_workspace = AirbyteWorkspace(
    rest_api_base_url=f"http://{AIRBYTE_HOST}:{AIRBYTE_PORT}/api/public/v1",
    configuration_api_base_url=f"http://{AIRBYTE_HOST}:{AIRBYTE_PORT}/api/v1",
    workspace_id=AIRBYTE_WORKSPACE_ID,
    client_id=AIRBYTE_CLIENT_ID,
    client_secret=AIRBYTE_CLIENT_SECRET,
)


# Custom translator to add "raw_data" prefix
class CustomAirbyteTranslator(DagsterAirbyteTranslator):
    def get_asset_spec(self, props):
        spec = super().get_asset_spec(props)
        # Add prefix to the asset key
        return spec._replace(
            key=spec.key.with_prefix("raw_data")
        )


# Load Airbyte assets
airbyte_assets = build_airbyte_assets_definitions(
    workspace=airbyte_workspace,
    dagster_airbyte_translator=CustomAirbyteTranslator(),
)

# DEBUG: Print Airbyte asset keys
print("\n=== AIRBYTE ASSET KEYS ===")
for asset_def in airbyte_assets:
    for key in asset_def.keys:
        print(f"  {key}")


# Custom translator to add "transformed_data" prefix to dbt assets
class CustomDbtTranslator(DagsterDbtTranslator):
    def get_asset_key(self, dbt_resource_props):
        asset_key = super().get_asset_key(dbt_resource_props)
        resource_type = dbt_resource_props.get("resource_type")
        
        print(f"Processing {resource_type}: {dbt_resource_props.get('name')} -> {asset_key}")

        if resource_type == "model":
            # Models (staging, marts) get "transformed_data" prefix
            prefixed_key = asset_key.with_prefix("transformed_data")
            print(f"  Model key: {prefixed_key}")
            return prefixed_key
        elif resource_type == "snapshot":
            # Snapshots also get the prefix
            prefixed_key = asset_key.with_prefix("transformed_data")
            print(f"  Snapshot key: {prefixed_key}")
            return prefixed_key
        else:
            print(f"  Other resource key: {asset_key}")
            return asset_key    


# Define dbt assets
@dbt_assets(manifest=DBT_MANIFEST, dagster_dbt_translator=CustomDbtTranslator())
def dbt_project_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()


resources = {
    "airbyte": airbyte_workspace,
    "dbt": DbtCliResource(
        project_dir=DBT_PROJECT_DIR,
        profiles_dir=DBT_PROFILES_DIR,
    ),
}


__all__ = ["airbyte_assets", "dbt_project_assets", "resources"]
