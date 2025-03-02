"""
This script serves as an NER output processor,
preparing input data for similarity mapping.
"""

from joblib import Parallel, delayed
from ingestion.models import SentenceSplit, NEROut
from ingestion.db import database, create_id, split_iter, pull_mongo_data
from ingestion.config import (
    DEFAULT_TEXT_PROCESSED_COLLECTION,
    DEFAULT_SENTENCE_SPLIT_COLLECTION,
)


class NerOutputProcessor:

    def __init__(
        self,
        in_col: str = DEFAULT_TEXT_PROCESSED_COLLECTION,
        out_col: str = DEFAULT_SENTENCE_SPLIT_COLLECTION,
        inserted_threshold: int = 1000,
    ) -> None:

        self.db = database
        self.in_col = self.db[in_col]
        self.out_col = self.db[out_col]
        self.inserted_threshold = inserted_threshold

    def replace_company_name(self, record: NEROut) -> list:
        """
        Replaces company names in the provided dictionary with their
        corresponding locations.

        Parameters:
        - pair (dict): A dictionary from get_ner_data()

        Returns:
        - list: A list of updated items where company names are replaced with locations.
        """
        sentence_list, companies_list, econs_list = (
            record.Sentence_list,
            record.Companies_list,
            record.Econs_list,
        )

        tagged_text_list = []
        for num in range(len(sentence_list)):
            tagged_text = sentence_list[num]
            # company_econ_list = dict(companies_list[num], **econs_list[num])
            for i, item in enumerate(companies_list[num]):
                tagged_text = tagged_text.replace(item, "locationC" + str(i + 1), 1)
            for i, item in enumerate(econs_list[num]):
                tagged_text = tagged_text.replace(item, "locationE" + str(i + 1), 1)
            tagged_text_list.append(tagged_text)
        return tagged_text_list

    def generate_sentences_info(self, record: NEROut) -> SentenceSplit:
        """
        Generates information for sentences based on company and economic entities.
        """

        sentence_id = []
        sentence_1 = []
        sentence_2 = []
        Companies_econs = []

        tagged_text_list = self.replace_company_name(record)
        companies_list, econs_list = record.Companies_list, record.Econs_list

        for i in range(len(companies_list)):
            company_econ_list = companies_list[i] + econs_list[i]
            if len(company_econ_list) == 0:
                sentence_1.append(tagged_text_list[i])
                sentence_2.append("")
                Companies_econs.append("")
                sentence_id.append(i)
            else:
                for j in range(len(companies_list[i])):
                    sentence_id.append(i)
                    sentence_1.append(tagged_text_list[i])
                    sentence_2.append("locationC" + str(j + 1))
                    Companies_econs.append(companies_list[i][j])
                for j in range(len(econs_list[i])):
                    sentence_id.append(i)
                    sentence_1.append(tagged_text_list[i])
                    sentence_2.append("locationE" + str(j + 1))
                    Companies_econs.append(econs_list[i][j])

        # new_record_id = create_id(
        #     DEFAULT_SENTENCE_SPLIT_COLLECTION,
        #     record.Title + record.Date + str(record.News_id),
        # )

        result_dict = {
            "_id": record._id,
            # "article_id": record.article_id,
            # "News_id": record.News_id,
            "Title": record.Title,
            "Date": record.Date,
            "Companies_econs": Companies_econs,
            "Sentence_id": sentence_id,
            "Sentence_1": sentence_1,
            "Sentence_2": sentence_2,
        }


        return SentenceSplit(**result_dict)

    def batch_helper(self, batch: list[NEROut]):
        """
        Processes a batch of data, generates sentence information, and
        inserts it into a MongoDB collection.
        """
        # TODO: Do an early return/submit processed IDs to the global queue
        #       Then write to database

        processed_sentence_split: list[SentenceSplit] = []

        try:
            for pair in batch:
                record = self.generate_sentences_info(pair)
                processed_sentence_split.append(record)

            return processed_sentence_split
        except Exception as e:
            print(e)
            return []

    def run(self, message_data: list[NEROut]):
        import traceback
        """
        Main method to run the NER output processor.
        It loads data from MongoDB, processes it in batches,
        and inserts the results back into the database.
        """
        try:
            ner_out_data = (
                pull_mongo_data(self.in_col, self.out_col, "NEROut")
                if len(message_data) == 0
                else message_data
            )

            return self.batch_helper(ner_out_data) if len(ner_out_data) > 0 else []
        except Exception as e:
            print(traceback.format_exc())
            raise Exception(f"NEROutputProcessor: {e}")
