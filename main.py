import argparse
import sys
from pathlib import Path

# Ensure the src directory is on the path when running without installation.
PROJECT_ROOT = Path(__file__).resolve().parent
SRC_PATH = PROJECT_ROOT / 'src'
if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))

from charybdisk.__main__ import main  # noqa: E402

if __name__ == '__main__':
    main()
