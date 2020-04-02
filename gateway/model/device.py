class Device(object):
    def __init__(self, message, device_id):
        self.__device_id = device_id
        self.__message = message
        
    @property
    def device_id(self):
        return self.__device_id
    
    @device_id.setter
    def device_id(self, value):
        self.__device_id = value

    @property
    def message(self):
        return self.__message
    
    @message.setter
    def message(self, value):
        self.__message = value
