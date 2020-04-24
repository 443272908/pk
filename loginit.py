# -*- coding:utf-8 -*-
import yaml
import logging.config
import os


def setup_logging(default_path="logging.yaml", default_level=logging.INFO, env_key="LOG_CFG"):
    path = default_path
    value = os.getenv(env_key, None)
    if value:
        path = value
    if os.path.exists(path):
        with open(path, "r") as f:
            data = f.read()
            config = yaml.load(data, Loader=yaml.FullLoader)
            logging.config.dictConfig(config)
    else:
        print('no config file found')
        logging.basicConfig(level=default_level)


def func():
    logging.info("start func")

    logging.info("exec func")

    logging.info("end func")


if __name__ == "__main__":
    setup_logging(default_path="../logconfig.yml")
    func()