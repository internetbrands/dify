import re
from typing import Optional, cast
import scispacy
from scispacy.linking import EntityLinker
import spacy

class SciSpacyKeywordTableHandler:
    def __init__(self):
        self.nlp = spacy.load("en_core_sci_sm")
        self.nlp.add_pipe("scispacy_linker", config={"resolve_abbreviations": True, "linker_name": "mesh"})

    def extract_keywords(self, text: str, max_keywords_per_chunk: Optional[int] = 10) -> set[str]:
        """Extract keywords with scispacy."""
        linked_scores = {}
        linker = self.nlp.get_pipe("scispacy_linker")
        doc = self.nlp(text)
        #Creating a set of ents so that we don't compute multiple times the same ent
        unique_entities = {}
        for ent in doc.ents:
            key = ent.text.lower().strip()
            if key not in unique_entities:
                unique_entities[key] = ent

            for ent in unique_entities.values():
                if ent._.kb_ents:
                    #Looking for Mesh Terms
                    #for candidate in ent._.kb_ents: -> loop to see all associated Mesh terms
                    #    cui, score = candidate
                    #    candidate_entity = linker.kb.cui_to_entity.get(cui)
                    #    candidate_name = candidate_entity.canonical_name if candidate_entity else "Unknown"
                    #    print(f"  CUI: {cui}, Score: {score}, Name: {candidate_name}")
                    filtered_links = [link for link in ent._.kb_ents if link[1] >= 0.95] #Filtering Exact Matches
                    if filtered_links:
                        best_link = max(filtered_links, key=lambda x: x[1])
                        linked_entity = linker.kb.cui_to_entity.get(best_link[0])
                        linked_name = linked_entity.canonical_name if linked_entity else "Unknown"
                        if len(linked_name) <= 35: #removing super long keywords
                            ent_text_lower = ent.text.lower().strip()
                            #if ent_text_lower in vocab:
                            #    ent_score = input_tfidf[vocab[ent_text_lower]]
                            #    print(f"'{ent_text_lower}' found in vocab, score: {ent_score}")
                            #else:
                            ent_score = 1.0
                            #    print(f"'{ent_text_lower}' not found in vocab")
                            linked_scores[linked_name] = linked_scores.get(linked_name, 0) + ent_score
        top_linked = sorted(linked_scores.items(), key=lambda x: x[1], reverse=True)[:10]
        keywords = [item[0] for item in top_linked]
        return set(keywords)
