import configparser

def import_conofiguration(config_file):
    content = configparser.ConfigParser()
    content.read(config_file)
    config = {}
    config['DB_USER'] = content['database_info'].get('DB_USER')
    config['DB_PASSWORD'] = content['database_info'].get('DB_PASSWORD')
    config['DB_HOST'] = content['database_info'].get('DB_HOST')
    config['DB_NAME'] = content['database_info'].get('DB_NAME')
    config['CHARSET'] = content['database_info'].get('CHARSET')

    config['TARGET_WORD'] = content['twitter_info'].get('TARGET_WORD')
    config['CONSUMER_KEY'] = content['twitter_info'].get('CONSUMER_KEY')
    config['CONSUMER_SECRET'] = content['twitter_info'].get('CONSUMER_SECRET')
    config['CONSUMER_SECRET'] = content['twitter_info'].get('CONSUMER_SECRET')
    config['ACCESS_TOKEN'] = content['twitter_info'].get('ACCESS_TOKEN')
    config['ACCESS_TOKEN_SECRET'] = content['twitter_info'].get('ACCESS_TOKEN_SECRET')

    config['AWS_ACCESS_KEY_ID'] = content['aws_info'].get('AWS_ACCESS_KEY_ID')
    config['AWS_SECRET_ACCESS_KEY'] = content['aws_info'].get('AWS_SECRET_ACCESS_KEY')
    
    return config

