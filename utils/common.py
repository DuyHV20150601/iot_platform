import yaml


class Common:
    @staticmethod
    def load_config(config_path):
        with open(config_path, 'r') as file:
            config = yaml.load(stream=file, Loader=yaml.FullLoader)
        return config
