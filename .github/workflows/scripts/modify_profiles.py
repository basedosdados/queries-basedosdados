"""
Change credentials path to current path
"""

from pathlib import Path

import ruamel.yaml as ryaml


def load_ruamel():
    """
    Loads a YAML file.
    """
    ruamel = ryaml.YAML()
    ruamel.default_flow_style = False
    ruamel.top_level_colon_align = True
    ruamel.indent(mapping=2, sequence=4, offset=2)
    return ruamel


def load_yaml_file(filepath: str) -> dict:
    """
    Loads the file that contains path to the models' metadata.
    """
    ruamel = load_ruamel()
    return ruamel.load((Path(filepath)).open(encoding="utf-8"))


if __name__ == "__main__":
    profiles = load_yaml_file("profiles.yml")
    profiles["default"]["outputs"]["dev"]["keyfile"] = str(
        Path.absolute(Path("./dbt-sa.json"))
    )
    profiles["default"]["outputs"]["prod"]["keyfile"] = str(
        Path.absolute(Path("./dbt-sa.json"))
    )
    ruamel = load_ruamel()
    ruamel.dump(profiles, open("profiles.yml", "w"))
