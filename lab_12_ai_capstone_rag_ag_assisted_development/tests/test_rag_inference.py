from path_resolver import _init_bundle_path
_init_bundle_path()

import pytest

from src.rag.inference import build_augmented_prompt, retrieve_context


class MockVectorIndex:
    """
    Mock class simulating the Databricks Vector Search index similarity_search response.
    """

    def similarity_search(self, query_text: str, columns: list, num_results: int):
        return {
            "result": {
                "data_array": [
                    [
                        "pbi_bi_directional_0001",
                        "pbi_bi_directional_relationships.md",
                        "https://learn.microsoft.com/en-us/power-bi/guidance/relationships-bidirectional-filtering",
                        "Bi-directional relationships can negatively impact query performance and create ambiguity.",
                    ]
                ]
            }
        }


def test_retrieve_context_parsing():
    """
    Validates that the raw 2D array output from Vector Search is correctly mapped to dicts.
    """
    mock_index = MockVectorIndex()
    chunks = retrieve_context(mock_index, "dummy query", top_k=1)

    assert len(chunks) == 1
    assert chunks[0]["chunk_id"] == "pbi_bi_directional_0001"
    assert chunks[0]["doc_name"] == "pbi_bi_directional_relationships.md"
    assert "relationships-bidirectional-filtering" in chunks[0]["source_url"]
    assert "negatively impact query performance" in chunks[0]["content"]


def test_build_augmented_prompt_formatting():
    """
    Ensures that context passages, document metadata, and strict grounding instructions are present.
    """
    sample_chunks = [
        {
            "chunk_id": "chunk_42",
            "doc_name": "star_schema.md",
            "source_url": "https://learn.microsoft.com/star-schema",
            "content": "Favor star schema over snowflake schema for performance.",
        }
    ]
    query = "What schema design is recommended?"
    prompt = build_augmented_prompt(query, sample_chunks)

    assert "Favor star schema over snowflake schema" in prompt
    assert "https://learn.microsoft.com/star-schema" in prompt
    assert "Rely ONLY on the provided context" in prompt