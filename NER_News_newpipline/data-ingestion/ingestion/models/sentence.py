from bson.binary import Binary
from dataclasses import dataclass


@dataclass
class SentenceSplit:
    _id: Binary
    article_id: str
    News_id: Binary
    Title: str
    Date: str
    Companies_econs: list[str]
    Sentence_id: list[str]
    Sentence_1: list[str]
    Sentence_2: list[str]

    def to_out_format(self) -> 'SentenceSplitOut':
        return SentenceSplitOut(
            _id=self._id,
            article_id=self.article_id,
            News_id=self.News_id,
            Sentence_id=self.Sentence_id,
            Output_sentence1=self.Sentence_1,
            Output_sentence2=self.Sentence_2,
            Companies_econs=self.Companies_econs,
            Date=self.Date,
            Title=self.Title,
        )

@dataclass
class SentenceSplitOut:
    _id: Binary
    article_id: str
    News_id: Binary
    Title: str
    Date: str
    Companies_econs: list[str]
    Sentence_id: list[str]
    Output_sentence1: list[str]
    Output_sentence2: list[str]

    def __post_init__(self):
        if isinstance(self.Companies_econs, dict):
            self.Companies_econs = [
                str(self.Companies_econs.get("after_delete", ""))]

@dataclass
class SelectedSentence(SentenceSplitOut):
    Entity_id: list
    Bingo_entity: list
    Similarity: list
    Similarity_1st: list
    Similarity_2nd: list
    first_cleaned_ner_entity: list
    first_matched_cleaned_comp: list
    second_ner_entity: list
    second_matched_comp: list
    NER_Mapping_ID: list
    Updated: int = 0
