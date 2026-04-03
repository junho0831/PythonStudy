from pyftpdlib.authorizers import DummyAuthorizer
from pyftpdlib.handlers import FTPHandler
from pyftpdlib.servers import FTPServer

from ftp_batch.config.local_test_settings import (
    CLIENT_FTP_HOST,
    CLIENT_FTP_PASSWORD,
    CLIENT_FTP_PORT,
    CLIENT_FTP_USERNAME,
    LOCAL_FTP_HOME_DIR,
    LOCAL_CLIENT_SCAN_DIR,
    LOCAL_SERVER_SCAN_DIR,
)


class LocalFTPServer:
    def __init__(
        self,
        host=CLIENT_FTP_HOST,
        port=CLIENT_FTP_PORT,
        username=CLIENT_FTP_USERNAME,
        password=CLIENT_FTP_PASSWORD,
        home_dir=str(LOCAL_FTP_HOME_DIR),
        perm="elradfmw",
    ):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.home_dir = home_dir
        self.perm = perm
        self.server = None

    def start(self):
        try:
            LOCAL_FTP_HOME_DIR.mkdir(parents=True, exist_ok=True)
            LOCAL_CLIENT_SCAN_DIR.mkdir(parents=True, exist_ok=True)
            LOCAL_SERVER_SCAN_DIR.mkdir(parents=True, exist_ok=True)

            authorizer = DummyAuthorizer()
            authorizer.add_user(
                self.username,
                self.password,
                self.home_dir,
                perm=self.perm,
            )

            handler = FTPHandler
            handler.authorizer = authorizer

            self.server = FTPServer((self.host, self.port), handler)

            print(f"[START] FTP 서버: {self.host}:{self.port}")
            print(f"[USER ] {self.username} / {self.password}")
            print(f"[DIR  ] {self.home_dir}")

            self.server.serve_forever()

        except Exception as e:
            print("🔥 서버 실행 에러:", e)

    def stop(self):
        if self.server:
            self.server.close_all()
            print("[STOP ] FTP 서버 종료")


if __name__ == "__main__":
    server = LocalFTPServer()
    server.start()
