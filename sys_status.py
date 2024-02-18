from core.bean import *
from threading import Thread
from datetime import datetime
from core.util import ExitEvent, HostInfoThread, PingIt
from localconfig import Configuration
from remote import RemoteConnection
import mariadb


class SystemStatusThread(Thread):

    def __init__(self, config: Configuration):
        Thread.__init__(self, name="system-status-query-thread")
        self.polling_period_s_ok = config.get_system_status_polling_period_ok()
        self.polling_period_s_ko = config.get_system_status_polling_period_ko()
        self.db_user = config.get_db_user()
        self.db_password = config.get_db_password()
        self.db_host = config.get_db_host()
        self.db_schema = config.get_db()
        self._the_reading: SystemStatusJson = None
        self._internet_test = RemoteConnection(config.get_speedtest_host())
        self._service_tests = [
            (RemoteConnection(_endpoint + r"/hc"), _name)
            for _endpoint, _name in config.get_system_services()
        ]
        self._host_tests = [
            (RemoteConnection(_endpoint + r"/hs"), _name)
            for _endpoint, _name in config.get_remote_hosts()
        ]
        self._local_host_info = HostInfoThread(config.get_host_status_polling_period())
        self._local_host_info.start()

    def run(self) -> None:
        while not ExitEvent().is_set():
            self._the_reading = SystemStatusJson(
                database_status=self._database_status(),
                speedtest_result=self._internet_status(),
                service_statuses=self._service_statuses(),
                host_statuses=self._hosts_statuses(),
                timestamp=datetime.now()
            )
            ExitEvent().wait(
                timeout=self.polling_period_s_ok
                if self._the_reading.internet_connection_status.alive and self._the_reading.database_status.is_available
                else self.polling_period_s_ko
            )
        self._local_host_info.join()

    def reading(self) -> AbstractJsonBean:
        if self._the_reading is None:
            return NotAvailableJsonBean()

        return self._the_reading

    def _database_status(self) -> DatabaseStatusJson:
        _start_tm = datetime.now()
        _available = False
        _issue = None
        _log = ""
        _connected_to = None
        _connection = None
        _cursor = None

        try:
            _tm = datetime.now()
            _connection = mariadb.connect(
                user=self.db_user,
                password=self.db_password,
                host=self.db_host,
                autocommit=False
            )
            _log += f"Connected ({round((datetime.now()-_tm).total_seconds()*1000)}[ms]). "
            _connected_to = f"{self.db_user}@{self.db_schema}@{self.db_host}"
            _tm = datetime.now()
            _connection.ping()
            _log += f"Pinged ({round((datetime.now()-_tm).total_seconds()*1000)} [ms]). "
            _available = True
            _tm = datetime.now()
            _cursor = _connection.cursor(dictionary=False, buffered=False)
            _cursor.execute('show databases')
            _databases = _cursor.fetchall()
            if _databases is None or len(_databases) == 0:
                _available = False
                _issue = f"No databases detected"
            elif "bhs" not in [_d[0] for _d in _databases]:
                _available = False
                _issue = f"BHS database not found"
            _log += f"Verified ({round((datetime.now() - _tm).total_seconds() * 1000)} [ms]). "
        except mariadb.Error as exc:
            _available = False
            _issue = str(exc)
        finally:
            if _cursor is not None:
                try:
                    _cursor.close()
                    _log += f"Cursor closed. "
                finally:
                    pass
            if _connection is not None:
                try:
                    _connection.close()
                    _log += f"Connection closed"
                finally:
                    pass
        return DatabaseStatusJson(
            is_available=_available,
            connected_to=_connected_to,
            issue=_issue,
            log=_log,
            total_connection_test_duration_ms=round((datetime.now() - _start_tm).total_seconds() * 1000),
            timestamp=_start_tm
        )

    def _internet_status(self) -> SpeedtestReadingJson:
        _error, _response = self._internet_test.make_request()

        if _error:
            return SpeedtestReadingJson()

        return json_to_bean(_response.json())

    def _service_statuses(self) -> list:
        _statuses = list()
        _tm = datetime.now()
        for _remote_service, _name in self._service_tests:
            _error, _response = _remote_service.make_request()

            if _error:
                _statuses.append(
                    ServiceStateJson(
                        name=_name,
                        activities_state=[
                            ServiceActivityJson(
                                name=_name+'-main',
                                state=ServiceActivityState.NOT_AVAILABLE,
                                message="N/A" if type(_error) == NotAvailableJsonBean else _error.error(),
                                timestamp=_tm
                            )
                        ],
                        timestamp=_tm
                    )
                )
            else:
                _statuses.append(json_to_bean(_response.json()))

        return _statuses

    def _hosts_statuses(self) -> list:
        _statuses = list()
        _tm = datetime.now()
        for _remote_service, _name in self._host_tests:
            _error, _response = _remote_service.make_request()

            if _error:
                _statuses.append(
                    HostStateJson(
                        up=PingIt(_remote_service.host, ping_timeout_ms=1000).ping().alive(),
                        host_name=_name,
                        timestamp=_tm
                    )
                )
            else:
                _status = json_to_bean(_response.json())
                if _status.up is None:
                    _status.up = True
                _statuses.append(_status)
        _statuses.append(self._local_host_info.host_status)
        return _statuses


if __name__ == "__main__":
    t = SystemStatusThread(Configuration())
    t.start()
    while 1:
        print(t.reading().to_dict())
        t.join(5)

# EOF
