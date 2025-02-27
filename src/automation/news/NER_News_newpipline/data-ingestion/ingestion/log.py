# import logging
# import time
# import os
# from airflow import LoggingMixin
#
# class Log(object):
#     def __init__(self, logger_name="airflow.task"):
#         """
#         让日志存储到 Airflow 默认的 logs/ 目录
#         """
#         self.name = logger_name
#         self.logger = LoggingMixin().log  # Airflow 日志
#         self.logger.setLevel(logging.DEBUG)
#
#         # **强制存储日志到 Airflow 的 logs/ 目录**
#         airflow_log_dir = os.getenv("AIRFLOW_LOGS", "./logs")  # 确保 logs/ 目录一致
#         if not os.path.exists(airflow_log_dir):
#             os.makedirs(airflow_log_dir)
#
#         self.log_path = os.path.join(airflow_log_dir, f"{self.name}_{time.strftime('%Y%m%d')}.log")
#
#         # **1️⃣ 添加文件日志**
#         file_handler = logging.FileHandler(self.log_path, "a", encoding="utf-8")
#         file_handler.setLevel(logging.INFO)
#
#         # **2️⃣ 添加控制台日志**
#         console_handler = logging.StreamHandler()
#         console_handler.setLevel(logging.INFO)
#
#         # **3️⃣ 统一日志格式**
#         formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(name)s - %(message)s")
#         file_handler.setFormatter(formatter)
#         console_handler.setFormatter(formatter)
#
#         # **4️⃣ 避免重复添加 handler**
#         if not any(isinstance(h, logging.FileHandler) for h in self.logger.handlers):
#             self.logger.addHandler(file_handler)
#         if not any(isinstance(h, logging.StreamHandler) for h in self.logger.handlers):
#             self.logger.addHandler(console_handler)
#
#     def getlog(self):
#         return self.logger
#
#     def getpath(self):
#         return self.log_path
#
#
#
#
#
#
import logging
import time
import os

class Log:
    def __init__(self, logger=None):
        # Create a logger with the specified name
        self.name = logger
        self.logger = logging.getLogger(logger)
        self.logger.setLevel(logging.DEBUG)

        # Avoid adding handlers multiple times
        if not self.logger.handlers:  # Check if handlers already exist
            # Create a handler for logging to a file
            self.log_time = time.strftime("%Y%m%d")
            file_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "../log")

            if not os.path.exists(file_dir):
                os.makedirs(file_dir)

            self.log_path = os.path.join(file_dir, f"{self.name}_{self.log_time}.log")

            file_handler = logging.FileHandler(self.log_path, "a", encoding="utf-8")
            file_handler.setLevel(logging.DEBUG)

            # Create a handler for logging to the console
            console_handler = logging.StreamHandler()
            console_handler.setLevel(logging.INFO)

            # Define a formatter with function name and line number
            formatter = logging.Formatter(
                '%(asctime)s - %(levelname)s - %(name)s - [%(funcName)s:%(lineno)d] - %(message)s'
            )
            file_handler.setFormatter(formatter)
            console_handler.setFormatter(formatter)

            # Add handlers to the logger
            self.logger.addHandler(file_handler)
            self.logger.addHandler(console_handler)

            # Disable propagation to prevent logging duplication
            self.logger.propagate = False

    def getlog(self):
        """Returns the configured logger."""
        return self.logger

    def getpath(self):
        """Returns the log file path."""
        return self.log_path
