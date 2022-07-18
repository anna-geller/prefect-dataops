import prefect
from prefect import flow, task, get_run_logger
from prefect_dataops.deployments import deploy_to_kubernetes, deploy_to_s3


@task
def say_hi():
    logger = get_run_logger()
    logger.info("Hello from the Health Check Flow! 👋")


@task
def log_platform_info():
    import platform
    import sys
    from prefect.orion.api.server import ORION_API_VERSION

    logger = get_run_logger()
    logger.info("Host's network name = %s", platform.node())
    logger.info("OS/Architecture = %s/%s", sys.platform, platform.machine())
    logger.info("Platform information (instance type) = %s 💻", platform.platform())
    logger.info("Python version = %s", platform.python_version())
    logger.info("Prefect version = %s 🚀", prefect.__version__)
    logger.info("Prefect API version = %s", ORION_API_VERSION)


@flow
def healthcheck():
    hi = say_hi()
    log_platform_info(wait_for=[hi])


deploy_to_kubernetes(flow=healthcheck)
# deploy_to_s3(healthcheck)
# deploy_to_s3(healthcheck, cron_schedule="*/2 * * * *")


if __name__ == "__main__":
    healthcheck()
