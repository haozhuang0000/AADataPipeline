import os
from ingestion.db import batch_upsert, batch_upsert_raw

from ingestion.sources import update_latest_news_database
from ingestion.processing import (
    NER_TextProcessor,
    NerOutputProcessor,
    run_similarity_search,
)
from ingestion.config import (
    DEFAULT_RAW_DATA_COLLECTION,
    DEFAULT_TEXT_PROCESSED_COLLECTION,
    DEFAULT_SENTENCE_SPLIT_COLLECTION,
    DEFAULT_SELECTED_SENTENCE_COLLECTION,
)
from ingestion.log import Log


def manage_data_processing():
    #logger = Log(f"{os.path.basename(__file__)}").getlog()

    logger = Log("data_processing").getlog()
    logger.info("Data processing task started......")
    try:
        upsert = {}

        # Storing NEWS api data
        news_data = update_latest_news_database()
        # upsert[DEFAULT_RAW_DATA_COLLECTION] = [
        #     article for language in news_data.values() for article in language
        # ]
        # upsert[DEFAULT_RAW_DATA_COLLECTION] = news_data['en']
        news_data_en = news_data.get("en", None)
        news_data_en = [data for data in news_data_en if data.Content != '' and data.Content is not None]
        upsert[DEFAULT_TEXT_PROCESSED_COLLECTION] = NER_TextProcessor().run(
            news_data_en
        )

        logger.info(
            f"Text processed {len(upsert[DEFAULT_TEXT_PROCESSED_COLLECTION])} documents"
        )

        upsert[DEFAULT_SENTENCE_SPLIT_COLLECTION] = NerOutputProcessor().run(
            upsert[DEFAULT_TEXT_PROCESSED_COLLECTION]
        )
        logger.info(
            f"Split sentence {len(upsert[DEFAULT_SENTENCE_SPLIT_COLLECTION])} documents"
        )

        upsert[DEFAULT_SELECTED_SENTENCE_COLLECTION] = run_similarity_search(
            upsert[DEFAULT_SENTENCE_SPLIT_COLLECTION]
        )

    except Exception as e:
        logger.error(f"Failed to process data with error: {e}")
        raise

    try:
        ## Inserting raw data other than english
        batch_upsert_raw(news_data)

        for collection, data in upsert.items():
            batch_upsert(collection, data)
    except Exception as e:
        logger.error(f"Failed to write: {e}")
        raise

if __name__ == "__main__":
    manage_data_processing()