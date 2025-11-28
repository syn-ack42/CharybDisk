import argparse
import sys
from pathlib import Path

# Ensure the src directory is on the path when running without installation.
PROJECT_ROOT = Path(__file__).resolve().parent
SRC_PATH = PROJECT_ROOT / 'src'
if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))

from charybdisk.app import start_services  # noqa: E402
from charybdisk.config_loader import ConfigError, load_config  # noqa: E402


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

