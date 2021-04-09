import datetime
import time

import requests

from sshtunnel import SSHTunnelForwarder
from sqlalchemy.orm import sessionmaker
import sqlalchemy

import xml.etree.ElementTree as ET


def send_to_yandex(array, clid, route):
    root = ET.Element('tracks', clid=clid)
    for i in array:
        track = ET.SubElement(root, 'track', uuid=str(i[3]), category='s', route=route, vehicle_type='minibus')
        ET.SubElement(track, 'point', latitude=str(i[6]), longitude=str(i[7]), avg_speed=str(i[8]),
                      direction=str(i[30]),
                      time=datetime.datetime.strptime(str(i[2]), "%Y-%m-%d %H:%M:%S").utcnow().strftime(
                          "%d%m%Y:%H%M%S"))

    data = {
        'compressed': '0',
        'data': f'{ET.tostring(root).decode()}'
    }

    headers = {'Content-Type': 'application/x-www-form-urlencoded'}

    result = requests.post(url='http://extjams.maps.yandex.net/mtr_collect/1.x/', data=data, headers=headers)
    print(result.text)


class Yandex:

    def __init__(self):
        self.T_17 = list()
        self.deviceID_17 = list()
        self.ndata_17 = list()
        with SSHTunnelForwarder(
                ('192.168.1.252', 22),  # Remote server IP and SSH port
                ssh_username="el",
                ssh_password="Gls22058",
                remote_bind_address=('localhost', 5432)) as server:
            server.start()

            print('Server connected via SSH')

            # CONNECT TO PostgreSQL

            local_port = str(server.local_bind_port)
            engine = sqlalchemy.create_engine('postgresql://tamirlan:Gls22058@127.0.0.1:' + local_port + '/vms_ws')

            session = sessionmaker(bind=engine)
            self.session = session()

            print('Database session created')

            self.load_file()
            self.get_deviceID(kwarg_17=self.T_17)  # , kwarg_18 = self.T_18)
            # self.get_data(kwarg_17=self.deviceID_17)  # , kwarg_18 = self.deviceID_18)

            i = 0
            while True:
                if i == 2:
                    break
                self.get_data(kwarg_17=self.deviceID_17)
                send_to_yandex(array=self.ndata_17, clid='nalchik', route='17')

                # self.send_to_yandex(array=self.ndata_18, clid='nalchik', route='18')

                self.ndata_17.clear()

                time.sleep(15)
                i += 1

            self.session.close()

    def get_data(self, **kwargs):

        dataParams = list()

        # self.ndata_18 = list()
        for i in kwargs['kwarg_17']:
            dataParams.append(
                self.session.execute(
                    f"SELECT * FROM nddata WHERE deviceid = '{i}' AND createddatetime > '{datetime.datetime.now() - datetime.timedelta(minutes=3)}' ORDER BY id DESC LIMIT 1")
            )

        for i in dataParams:
            for k in i:
                self.ndata_17.append(k)

        dataParams.clear()

        # print(self.ndata_17)
        # print('\n\n')
        # print(datetime.datetime.now())
        # print(datetime.datetime.now() - datetime.timedelta(minutes=3))
        # for i in kwargs['kwarg_18']: nddataParams.append(self.session.execute(f"SELECT * FROM nddata WHERE deviceid
        # = '{i}' ORDER BY id DESC LIMIT 1"))
        #
        # for i in nddataParams:
        #     for k in i:
        #         self.ndata_18.append(k)
        #
        # nddataParams.clear()

    def get_deviceID(self, **kwargs):

        deviceID = list()

        # self.deviceID_18 = list()

        for i in kwargs['kwarg_17']:
            deviceID.append((self.session.execute(f"SELECT id FROM navigationdevice where code = '{i}' ")))

        for i in deviceID:
            for k in i:
                self.deviceID_17.append(k[0])

        deviceID.clear()
        # for i in kwarg['kwarg_18']:
        #    deviceID.append((self.session.execute(f"SELECT id FROM navigationdevice where code = '{i}' ")))
        #
        #
        # for i in (deviceID):
        #     for k in i:
        #         self.deviceID_18.append(k[0])

    def load_file(self):

        TC_17 = '17.txt'
        # TC_18 = '18.txt'
        # self.T_18 = list()

        for line in open(TC_17, 'r', encoding='UTF-8'):
            if not "#" in line:
                for x in line.split():
                    if x != '':
                        self.T_17.append(x)

        # for line in open(TC_18, 'r', encoding='UTF-8'):
        #     if not "#" in line:
        #         for x in line.split():
        #             if x != '':
        #                 self.T_18.append(x)


if __name__ == "__main__":
    Yandex()
