import pytest
from pydantic import ValidationError

from genai.enrichment.schema import Enrichment, describe_error, json_schema

DESCRIPTION = (
    "A sturdy wooden bed frame that is easy to assemble and fits a standard double mattress, "
    "with a smooth finish for the bedroom."
)


def valid(**overrides: object) -> dict[str, object]:
    row: dict[str, object] = {
        "title": "Solid Wood Double Bed Frame",
        "description": DESCRIPTION,
        "tags": ["bed", "Bedroom ", "wood"],
        "language": "en",
    }
    row.update(overrides)
    return row


def test_valid_output_normalises_tags() -> None:
    out = Enrichment.model_validate(valid())
    assert out.tags == ["bed", "bedroom", "wood"]


@pytest.mark.parametrize(
    ("overrides", "rule_id"),
    [
        ({"title": "x" * 81}, "title:string_too_long"),
        ({"description": "Too short to be useful."}, "description:string_too_short"),
        ({"description": "word " * 130}, "description:string_too_long"),
        ({"tags": ["bed", "wood"]}, "tags:too_short"),
        ({"tags": [f"t{i}" for i in range(9)]}, "tags:too_long"),
        ({"language": "pt"}, "language:literal_error"),
        ({"title": "Cama de Casal em Madeira Maciça"}, "title:not_ascii"),
        (
            {
                "description": "Cama de casal em madeira para o quarto, com acabamento liso e "
                "montagem simples para colchao padrao."
            },
            "description:not_english",
        ),
        ({"tags": ["bed", "", "wood"]}, "tags.1:string_too_short"),
    ],
)
def test_invalid_output_has_rule_id(overrides: dict[str, object], rule_id: str) -> None:
    with pytest.raises(ValidationError) as err:
        Enrichment.model_validate(valid(**overrides))
    assert describe_error(err.value)[0] == rule_id


@pytest.mark.parametrize(
    "description",
    [  # terse English that Qwen wrote in the live pass (few stopwords)
        "Sturdy building board ideal for DIY home improvement projects. Dimensions: 23x20x16 cm. "
        "Weight: 738g.",
        "A classic vinyl record for music lovers. Measures 16x11x2 cm and weighs 1.1 kg.",
        "This professional camera lens provides 5 high-resolution photo options.",
        "Fast processor, good memory, and modern design. Quick delivery, great value.",
    ],
)
def test_terse_english_description_is_accepted(description: str) -> None:
    Enrichment.model_validate(valid(description=description))


def test_invalid_json_rule_id() -> None:
    with pytest.raises(ValidationError) as err:
        Enrichment.model_validate_json("not json")
    assert describe_error(err.value)[0] == "json_invalid"


def test_json_schema_is_strict_object() -> None:
    schema = json_schema()
    assert schema["additionalProperties"] is False
    assert set(schema["required"]) == {"title", "description", "tags", "language"}
