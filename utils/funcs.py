import os
import yaml

def parse_defaults() -> dict:
    conf = yaml.safe_load(open(os.path.join(os.path.dirname(__file__), 'defaults.yaml'), 'r').read())
    return conf

def parse_conf(conf_file: str = None) -> dict:
    """Parse conf from a YAML custom file, otherwise gets conf values
    from a default file.

    Args:
        conf_file (str, optional): Configuration file (YAML).

    Returns:
        dict: configuration values
    """
    default_conf = parse_defaults()
    custom_conf = yaml.safe_load(open(conf_file, 'r').read())
    final_conf = {}
    for k in default_conf:
        if k in custom_conf:
            final_conf[k] = custom_conf[k]
        else:
            final_conf[k] = default_conf[k]
    return final_conf