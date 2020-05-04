import configparser
import psycopg2
from sql.DDLScriptRunner import DDLScriptRunner
from infrastructure.InfraBootstrapper import InfraBootstrapper
from infrastructure.InfrastructureSettings import InfrastructureSettings
from utils.RedshiftStagingLoader import RedshiftJsonS3StagingLoader
from steps.EtlStepBase import EtlStepBase
from steps.LogDataStagingLoadEtlStep  import LogDataStagingLoadEtlStep
from steps.SongStagingLoadEtlStep  import SongStagingLoadEtlStep
from steps.CreateArtistDimensionsEtlStep import CreateArtistDimensionsEtlStep
from steps.CreateDateDimensionsEtlStep import CreateDateDimensionsEtlStep
from steps.CreateSongDimensionsEtlStep import CreateSongDimensionsEtlStep
from steps.CreateUserDimensionsEtlStep import CreateUserDimensionsEtlStep
from steps.CreateSongPlayFactsEtlStep import CreateSongPlayFactsEtlStep



def main():
   config = configparser.ConfigParser()
   config.read('config/dwh.cfg')
   infra = InfraBootstrapper(config)
   infra.init()
   step_array = []
   infra_settings = InfrastructureSettings(config)
   loader = RedshiftJsonS3StagingLoader(infra_settings)

   ddl_scripts = DDLScriptRunner()
   ddl_scripts.init(config)

   step_array.append(LogDataStagingLoadEtlStep(config, infra_settings, loader))
   step_array.append(SongStagingLoadEtlStep(config, infra_settings, loader))
   step_array.append(CreateArtistDimensionsEtlStep(config, infra_settings))
   step_array.append(CreateSongDimensionsEtlStep(config, infra_settings))
   step_array.append(CreateUserDimensionsEtlStep(config, infra_settings))
   step_array.append(CreateSongPlayFactsEtlStep(config, infra_settings))
   step_array.append(CreateDateDimensionsEtlStep(config, infra_settings))

   print('Running Steps:')

   for step in step_array:
       step.run()
    
   print("Job Completed. ")

    


if __name__ == "__main__":
    main()
    
    





