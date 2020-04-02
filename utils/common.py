import yaml


class Common:
    @staticmethod
    def load_config(config_path):
        with open(config_path, 'r') as file:
            config = yaml.load(stream=file, Loader=yaml.FullLoader)
        return config


if __name__ == '__main__':
    print(Common.load_config('../configs/mqtt_config.yaml'))
