from cr_assis.load import *
from cr_monitor.daily.capital_monitor import CapitalMonitor

capital = CapitalMonitor(log_path = "/mnt/efs/fs1/data_ssh")
capital.run_monitor()