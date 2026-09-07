import re
from pathlib import Path

repo = Path(__file__).resolve().parents[1]
tfvars_path = repo / "terraform" / "environments" / "dev" / "terraform.tfvars"
template_path = repo / "sql" / "streaming_ingestion.sql"
output_path = repo / "sql" / "streaming_ingestion.rendered.sql"

tfvars = tfvars_path.read_text(encoding="utf-8")

def get_var(name):
    match = re.search(rf'(?m)^\s*{re.escape(name)}\s*=\s*"([^"]+)"', tfvars)
    if not match:
        raise RuntimeError(f"Missing Terraform variable: {name}")
    return match.group(1)

project_name = get_var("project_name")
environment = get_var("environment")
values = {
    "${KINESIS_STREAM_NAME}": f"{project_name}-{environment}-stream",
    "${GLUE_DATABASE_NAME}": get_var("glue_database_name"),
    "${AWS_REGION}": get_var("region"),
    "${REDSHIFT_DATABASE_NAME}": get_var("redshift_database_name"),
}

sql = template_path.read_text(encoding="utf-8")
for placeholder, value in values.items():
    sql = sql.replace(placeholder, value)

remaining = re.findall(r"\$\{[A-Z0-9_]+\}", sql)
if remaining:
    raise RuntimeError(f"Unresolved placeholders: {remaining}")

output_path.write_text(sql, encoding="utf-8")
print(f"Rendered SQL: {output_path}")
