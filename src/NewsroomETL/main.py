from datetime import datetime
from includes.utils import get_env_conf
from ingestion.bronze_orchestrator import BronzeOrchestaror

config = get_env_conf()

def main() -> None:
    sources = config["data_sources"]
    bronze_orch = BronzeOrchestaror()
    for source in sources:
        bronze_orch.run(source)




if __name__ == "__main__":
    main()
