import os
import toml

# Path to your pyproject.toml file
PYPROJECT_TOML = os.path.join("academic-observatory-workflows", "pyproject.toml")
# Path to the output requirements.txt file
REQUIREMENTS_TXT = ".liccheck_requirements.txt"

# Load the pyproject.toml file
with open(PYPROJECT_TOML, "rb") as f:
    pyproject = toml.loads(f.read().decode())

# Extract dependencies from the [project.dependencies] section
dependencies = pyproject.get("project", {}).get("dependencies", [])

# Write dependencies to requirements.txt
with open(REQUIREMENTS_TXT, "w") as f:
    f.write("\n".join(dependencies))

print(f"Dependencies have been written to {REQUIREMENTS_TXT}.")
