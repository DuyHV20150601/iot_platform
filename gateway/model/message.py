class Message(object):
    def __init__(self, payload, device_id):
        self.__payload = payload
        self.__device_id = device_id

    @property
    def payload(self):
        return self.__payload

    @payload.setter
    def payload(self, value):
        self.__payload = value

    @property
    def device_id(self):
        return self.__device_id

    @device_id.setter
    def device_id(self, value):
        self.__device_id = value