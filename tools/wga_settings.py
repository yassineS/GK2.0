import os
import ConfigParser


# Import settings
root_path = os.path.dirname(os.path.realpath(__file__))
config = ConfigParser()
config.read(os.path.join(root_path, 'settings.conf'))
s = config['main']
