import faulthandler
faulthandler.enable(all_threads=True)

import argparse
import sys

from charybdisk.app import start_services
from charybdisk.config_loader import ConfigError, load_config


def main() -> None:
    parser = argparse.ArgumentParser(description="CharybDisk unified producer/consumer")
    parser.add_argument('--config', '-c', required=True, help="Path to configuration YAML file")
    args = parser.parse_args()

    try:
        config = load_config(args.config)
    except (FileNotFoundError, ConfigError) as e:
        print(f"Configuration error: {e}")
        sys.exit(2)
    except Exception as e:
        print(f"Unexpected error loading config: {e}")
        sys.exit(2)

    start_services(config)


if __name__ == '__main__':
    main()
