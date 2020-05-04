import configparser
from infrastructure.InfraBootstrapper import InfraBootstrapper
from infrastructure.InfrastructureSettings import InfrastructureSettings


def main():
    config = configparser.ConfigParser()
    config.read('config/dwh.cfg')
    infra = InfraBootstrapper(config)
    infra.drop_infrastucture()
   


if __name__ == "__main__":
    main()
    

    





