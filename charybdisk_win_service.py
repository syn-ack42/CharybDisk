import sys
import os
import threading
import traceback
from pathlib import Path

import win32serviceutil
import win32service
import win32event
import servicemanager
import win32timezone  # Ensure PyInstaller bundles this pywin32 module.

from charybdisk.app import start_services  # noqa: E402
from charybdisk.config_loader import ConfigError, load_config  # noqa: E402

CONFIG_OPTION = "config_path"
INSTALL_UPDATE_COMMANDS = {"install", "update"}
VALUE_OPTS = {
    "-c",
    "--username",
    "--password",
    "--startup",
    "--perfmonini",
    "--perfmondll",
    "--wait",
}
FLAG_OPTS = {"--interactive"}


def _default_config_path() -> Path:
    if getattr(sys, "frozen", False):
        base_dir = Path(sys.executable).resolve().parent
    else:
        base_dir = Path(__file__).resolve().parent
    return base_dir / "charybdisk-conf.yaml"


def _handle_custom_options(opts):
    config_path = None
    for opt, val in opts:
        if opt == "-c":
            config_path = str(Path(val).resolve())
    if config_path:
        win32serviceutil.SetServiceCustomOption(CharybDiskService._svc_name_, CONFIG_OPTION, config_path)


def _normalize_install_update_argv(argv):
    if len(argv) < 2:
        return argv
    cmd_index = None
    for i, token in enumerate(argv[1:], start=1):
        if token in INSTALL_UPDATE_COMMANDS:
            cmd_index = i
            break
    if cmd_index is None:
        return argv
    before = argv[1:cmd_index]
    after = argv[cmd_index + 1 :]
    moved = []
    remaining = []
    i = 0
    while i < len(after):
        token = after[i]
        if token in VALUE_OPTS:
            if i + 1 >= len(after):
                remaining.append(token)
                i += 1
                continue
            moved.extend([token, after[i + 1]])
            i += 2
            continue
        if token in FLAG_OPTS:
            moved.append(token)
            i += 1
            continue
        remaining.append(token)
        i += 1
    return [argv[0], *before, *moved, argv[cmd_index], *remaining]


class CharybDiskService(win32serviceutil.ServiceFramework):
    _svc_name_ = "CharybDiskService"
    _svc_display_name_ = "CharybDiskService"
    _svc_description_ = "CharybDisk background service"

    def __init__(self, args):
        super().__init__(args)
        self.stop_event = threading.Event()
        self.stop_handle = win32event.CreateEvent(None, 0, 0, None)

    def SvcStop(self):
        self.ReportServiceStatus(win32service.SERVICE_STOP_PENDING)
        self.stop_event.set()
        win32event.SetEvent(self.stop_handle)

    def SvcDoRun(self):
        servicemanager.LogInfoMsg("CharybDiskService started")
        self.main()

    def main(self):
        config_path = win32serviceutil.GetServiceCustomOption(self, CONFIG_OPTION, None)
        if not config_path:
            config_path = str(_default_config_path())
        try:
            config_path = str(Path(config_path).resolve())
            os.chdir(str(Path(config_path).parent))
            servicemanager.LogInfoMsg(f"CharybDiskService using config: {config_path}")
            config = load_config(config_path)
        except (FileNotFoundError, ConfigError) as exc:
            servicemanager.LogErrorMsg(f"Configuration error: {exc}")
            raise
        except Exception:
            servicemanager.LogErrorMsg(f"Startup failure:\n{traceback.format_exc()}")
            raise
        start_services(config, stop_event=self.stop_event)

if __name__ == "__main__":
    if len(sys.argv) == 1:
        servicemanager.Initialize()
        servicemanager.PrepareToHostSingle(CharybDiskService)
        servicemanager.StartServiceCtrlDispatcher()
    else:
        win32serviceutil.HandleCommandLine(
            CharybDiskService,
            customInstallOptions="c:",
            customOptionHandler=_handle_custom_options,
            argv=_normalize_install_update_argv(sys.argv),
        )
