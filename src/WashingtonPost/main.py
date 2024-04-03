from ingestion.washington_post_data import WashingtonPostData
from trans.transform_data import TransformData


# import includes.configuration as configuration
def main() -> None:

    wp_data = WashingtonPostData()
    data = wp_data.extract_data(5)
    # transformed_data = TransformData(path_data) 
    # presentation =  # website


if __name__ == "__main__":
    main()
