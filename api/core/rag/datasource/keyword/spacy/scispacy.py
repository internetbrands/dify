from core.rag.datasource.keyword.jieba.jieba import Jieba
from extensions.ext_redis import redis_client
from core.rag.models.document import Document
from core.rag.datasource.keyword.keyword_base import BaseKeyword
from core.rag.datasource.keyword.scispacy.scispacy_keyword_table_handler import SciSpacyKeywordTableHandler

class SciSpacy(Jieba):
    def create(self, texts: list[Document], **kwargs) -> BaseKeyword:
        lock_name = "keyword_indexing_lock_{}".format(self.dataset.id)
        with redis_client.lock(lock_name, timeout=600):
            keyword_table_handler = SciSpacyKeywordTableHandler()
            keyword_table = self._get_dataset_keyword_table()
            for text in texts:
                keywords = keyword_table_handler.extract_keywords(
                    text.page_content, self._config.max_keywords_per_chunk
                )
                if text.metadata is not None:
                    self._update_segment_keywords(self.dataset.id, text.metadata["doc_id"], list(keywords))
                    keyword_table = self._add_text_to_keyword_table(
                        keyword_table or {}, text.metadata["doc_id"], list(keywords)
                    )

            self._save_dataset_keyword_table(keyword_table)

            return self

    def add_texts(self, texts: list[Document], **kwargs):
        lock_name = "keyword_indexing_lock_{}".format(self.dataset.id)
        with redis_client.lock(lock_name, timeout=600):
            keyword_table_handler = SciSpacyKeywordTableHandler()

            keyword_table = self._get_dataset_keyword_table()
            keywords_list = kwargs.get("keywords_list")
            for i in range(len(texts)):
                text = texts[i]
                if keywords_list:
                    keywords = keywords_list[i]
                    if not keywords:
                        keywords = keyword_table_handler.extract_keywords(
                            text.page_content, self._config.max_keywords_per_chunk
                        )
                else:
                    keywords = keyword_table_handler.extract_keywords(
                        text.page_content, self._config.max_keywords_per_chunk
                    )
                if text.metadata is not None:
                    self._update_segment_keywords(self.dataset.id, text.metadata["doc_id"], list(keywords))
                    keyword_table = self._add_text_to_keyword_table(
                        keyword_table or {}, text.metadata["doc_id"], list(keywords)
                    )

            self._save_dataset_keyword_table(keyword_table)

    def multi_create_segment_keywords(self, pre_segment_data_list: list):
        keyword_table_handler = SciSpacyKeywordTableHandler()
        keyword_table = self._get_dataset_keyword_table()
        for pre_segment_data in pre_segment_data_list:
            segment = pre_segment_data["segment"]
            if pre_segment_data["keywords"]:
                segment.keywords = pre_segment_data["keywords"]
                keyword_table = self._add_text_to_keyword_table(
                    keyword_table or {}, segment.index_node_id, pre_segment_data["keywords"]
                )
            else:
                keywords = keyword_table_handler.extract_keywords(segment.content, self._config.max_keywords_per_chunk)
                segment.keywords = list(keywords)
                keyword_table = self._add_text_to_keyword_table(
                    keyword_table or {}, segment.index_node_id, list(keywords)
                )
        self._save_dataset_keyword_table(keyword_table)