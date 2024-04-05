import os
import yaml
import intake
import logging

def create_yaml_intake_catalog_from_dict(dict: dict, file_name="tmp"):
    logging.info(f"{dict}")
    path = f"{file_name}.yaml"

    with open(path, 'w') as f:
        yaml.dump(dict, f)

    return path


