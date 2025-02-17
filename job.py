import os
import subprocess

packages = [
    'pandas',
    'numpy',
    'psycopg2-binary',
    'boto3',
    'apscheduler',
]
flag_file = 'packages_installed.flag'

if not os.path.exists(flag_file):
    for package in packages:
        try:
            subprocess.check_call(['pip', 'install', package])
            print(f"Package {package} has been successfully installed.")
        except subprocess.CalledProcessError as e:
            print(f"An error occurred while installing {package}: {e}")

    with open(flag_file, 'w') as f:
        f.write('Packages installed successfully')
    print("All packages have been installed.")


# from dotenv import load_dotenv
# load_dotenv()
from apscheduler.triggers.cron import CronTrigger
from common.common_logger import logger
from apscheduler.schedulers.blocking import BlockingScheduler
from datetime import datetime
from participant_phase.index import handler as participant_phase_handler


def activate_phase():
    logger.info(f"Job executed at {datetime.now()}")
    participant_phase_handler(None, None)


if __name__ == "__main__":
    logger.info('-------------job starting-------------')
    scheduler = BlockingScheduler()
    trigger = CronTrigger(minute=0)

    scheduler.add_job(activate_phase, trigger)

    scheduler.start()
    logger.info('-------------job started-------------')
