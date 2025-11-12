import logging
import numbers
import os
import select
import socket
from errno import EINTR
from socket import AF_INET, SOCK_STREAM, SOMAXCONN

import ray

logger = logging.getLogger(__file__)


def compute_real_exit_code(exit_code):
    # SystemExit's code can be integer or string, but os._exit only accepts integers
    if isinstance(exit_code, numbers.Integral):
        return exit_code
    else:
        return 1


@ray.remote
class PyWorker:
    def __init__(self):
        logger.info("PyWorker is created")

        self.listen_sock = socket.socket(AF_INET, SOCK_STREAM)
        self.listen_sock.bind(("127.0.0.1", 0))
        self.listen_sock.listen(max(1024, SOMAXCONN))
        listen_host, self.listen_port = self.listen_sock.getsockname()

    def heartbeat(self):
        return f"{os.getpid()} is alive"

    def get_port(self) -> int:
        return self.listen_port

    def start(self):
        import time
        # Most of the code is copied from PySpark's daemon.py

        from pyspark.serializers import (
            UTF8Deserializer,
            write_int,
            write_with_length,
        )
        from pyspark.worker import main as worker_main

        logger.info(f"Starting PyWorker with pid: {os.getpid()}, listen_port={self.listen_port}")
        while True:
            try:
                logger.info("Waiting for connection")
                ready_fds = select.select([0, self.listen_sock], [], [], 1)[0]
            except select.error as ex:
                logger.error(f"select error: {ex}")
                if ex[0] == EINTR:
                    continue
                else:
                    raise

            logger.info(f"ready_fds: {ready_fds}")
            if self.listen_sock in ready_fds:
                try:
                    sock, _ = self.listen_sock.accept()
                except OSError as e:
                    logger.error(f"Failed to accept connection: {e}")
                    if e.errno == EINTR:
                        continue
                    raise

                try:
                    logger.info("Connection accepted")
                    # Acknowledge that the fork was successful
                    outfile = sock.makefile(mode="wb")
                    write_int(os.getpid(), outfile)
                    outfile.flush()
                    outfile.close()
                    while True:
                        buffer_size = int(os.environ.get("SPARK_BUFFER_SIZE", 65536))
                        infile = os.fdopen(os.dup(sock.fileno()), "rb", buffer_size)
                        outfile = os.fdopen(os.dup(sock.fileno()), "wb", buffer_size)
                        client_secret = UTF8Deserializer().loads(infile)
                        if os.environ["PYTHON_WORKER_FACTORY_SECRET"] == client_secret:
                            write_with_length("ok".encode("utf-8"), outfile)
                            outfile.flush()
                        else:
                            write_with_length("err".encode("utf-8"), outfile)
                            outfile.flush()
                            sock.close()
                            return 1

                        try:
                            code = worker_main(infile, outfile)
                            logger.info(f"normal exit code: {code}")
                        except SystemExit as exc:
                            code = compute_real_exit_code(exc.code)
                        finally:
                            try:
                                outfile.flush()
                            except Exception:
                                pass
                        # wait for closing
                        logger.info(f"exit code: {code}")
                        # logger.info("Waiting for closing")
                        # try:
                        #     while sock.recv(1024):
                        #         pass
                        # except Exception:
                        #     pass
                        logger.info("Closing. Waiting for next loop")
                        break
                except BaseException as e:
                    logger.error(f"PyWorker failed with exception: {e}")
                    return 1
                # else:
                #     return 0
            else:
                time.sleep(0.5)
