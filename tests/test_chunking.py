from src.bee_ingestion.chunking import _normalize_line, build_chunks, build_extraction_metrics, normalize_text, parse_text, sanitize_text
from src.bee_ingestion.validation import validate_chunk


def test_parse_and_chunk_preserves_adjacency() -> None:
    text = (
        "INTRODUCTION\n\n"
        "Honey bees produce honey and wax for the colony.\n\n"
        "VARROA\n\n"
        "Varroa mites affect colonies and require careful monitoring.\n\n"
        "Treatment options depend on season and colony condition."
    )
    blocks = parse_text("doc1", text)
    chunks = build_chunks("doc1", "tenant1", blocks, target_chars=120, min_chars=40)

    assert len(chunks) >= 2
    assert chunks[0].next_chunk_id == chunks[1].chunk_id
    assert chunks[1].prev_chunk_id == chunks[0].chunk_id


def test_validation_flags_short_chunk() -> None:
    blocks = parse_text("doc1", "Tiny text.")
    chunks = build_chunks("doc1", "tenant1", blocks, target_chars=120, min_chars=20)
    validation = validate_chunk(chunks[0])

    assert validation.status in {"review", "rejected"}
    assert "too_short" in validation.reasons


def test_early_front_matter_chunk_is_not_accepted() -> None:
    text = (
        "FIRST LESSONS IN BEEKEEPING\n\n"
        "Cornell University Library. There are no known copyright restrictions.\n\n"
        "\f"
        "Introduction By Dr. C. C. Miller. Honey bees produce honey and wax."
    )
    blocks = parse_text("doc1", text)
    chunks = build_chunks("doc1", "tenant1", blocks, target_chars=300, min_chars=20, document_class="book")

    first_validation = validate_chunk(chunks[0])
    assert chunks[0].metadata["chunk_role"] == "front_matter"
    assert first_validation.status == "review"


def test_leading_section_marker_is_preserved() -> None:
    blocks = parse_text("doc1", "Introduction By Dr. C. C. Miller. Honey bees produce honey.")

    assert blocks[0].section_path == ["Introduction"]


def test_back_matter_catalogue_is_filtered() -> None:
    blocks = parse_text("doc1", "Great Reductions in this Catalogue")
    chunks = build_chunks("doc1", "tenant1", blocks, target_chars=120, min_chars=20, document_class="book")
    validation = validate_chunk(chunks[0])

    assert chunks[0].metadata["chunk_role"] == "back_matter"
    assert validation.status == "review"


def test_normalize_line_repairs_basic_spacing() -> None:
    normalized = _normalize_line("Preface.Thisshorttreatise JohnDoe 12Bees")

    assert normalized == "Preface. Thisshorttreatise John Doe 12 Bees"


def test_single_letter_line_is_not_heading() -> None:
    blocks = parse_text("doc1", "J\n\nS. Harbison, Capt.")

    assert blocks[0].block_type == "paragraph"


def test_random_fragment_is_rejected() -> None:
    blocks = parse_text("doc1", "Jxqv.")
    chunks = build_chunks("doc1", "tenant1", blocks, target_chars=120, min_chars=20)
    validation = validate_chunk(chunks[0])

    assert validation.status == "rejected"


def test_page_marker_fragment_is_rejected() -> None:
    blocks = parse_text("doc1", "[[Page 14]]\n\n2")
    chunks = build_chunks("doc1", "tenant1", blocks, target_chars=120, min_chars=20, document_class="book")
    validation = validate_chunk(chunks[0])

    assert validation.status == "rejected"
    assert "too_few_words" in validation.reasons


def test_parse_text_uses_actual_page_numbers_from_markers() -> None:
    text = "[[Page 17]]\n\nINTRODUCTION\n\nHoney bees organize brood rearing.\f[[Page 18]]\n\nQueens mate once."
    blocks = parse_text("doc1", text, document_class="book")

    assert [block.page for block in blocks] == [17, 17, 18]
    assert blocks[0].block_id.startswith("doc1:p0017:")


def test_normalize_text_and_metrics_capture_improvement() -> None:
    raw = "Preface.Thisshorttreatise\n\nIntroduction.ByDr.C.C.Miller"
    normalized = normalize_text(raw)
    metrics = build_extraction_metrics(raw, normalized)

    assert "Preface. Thisshorttreatise" in normalized
    assert metrics["raw_chars"] == len(raw)
    assert metrics["normalized_chars"] == len(normalized)
    assert metrics["raw_pages"] == 1


def test_short_body_chunk_is_not_accepted() -> None:
    blocks = parse_text("doc1", "Honey bees swarm often.")
    chunks = build_chunks("doc1", "tenant1", blocks, target_chars=120, min_chars=20, document_class="book")
    validation = validate_chunk(chunks[0])

    assert validation.status in {"review", "rejected"}
    assert "too_few_words" in validation.reasons


def test_short_coherent_body_with_section_is_accepted() -> None:
    text = "SPRING MANAGEMENT\n\nInspect brood before adding supers."
    blocks = parse_text("doc1", text)
    chunks = build_chunks("doc1", "tenant1", blocks, target_chars=120, min_chars=20, document_class="book")
    validation = validate_chunk(chunks[0])

    assert chunks[0].metadata["chunk_role"] == "body"
    assert validation.status == "accepted"
    assert "too_few_words" in validation.reasons


def test_short_coherent_body_without_section_is_accepted() -> None:
    blocks = parse_text("doc1", "Inspect brood before adding supers.")
    chunks = build_chunks("doc1", "tenant1", blocks, target_chars=120, min_chars=20, document_class="book")
    validation = validate_chunk(chunks[0])

    assert "missing_section" in validation.reasons
    assert "too_few_words" in validation.reasons
    assert validation.status == "accepted"


def test_short_fragment_without_sentence_punctuation_stays_review() -> None:
    text = "SPRING MANAGEMENT\n\nInspect brood before adding supers"
    blocks = parse_text("doc1", text)
    chunks = build_chunks("doc1", "tenant1", blocks, target_chars=120, min_chars=20, document_class="book")
    validation = validate_chunk(chunks[0])

    assert validation.status == "review"
    assert "too_few_words" in validation.reasons


def test_body_sentence_with_word_contents_is_not_mislabeled() -> None:
    text = (
        "Natural History Of The Honey Bee\n\n"
        "To dissect out the contents of the abdomen requires a microscope and steady hands."
    )
    blocks = parse_text("doc1", text, document_class="note")
    chunks = build_chunks("doc1", "tenant1", blocks, target_chars=300, min_chars=40, document_class="note")

    assert chunks[0].metadata["chunk_role"] == "body"


def test_contents_like_page_is_classified_as_contents() -> None:
    text = (
        "Contents\n\n"
        "Natural History Of The Honey Bee\n\n"
        "Hives\n\n"
        "An Apiary\n\n"
        "Artificial Swarming\n\n"
        "Supers And Their Management\n\n"
        "Diseases And Enemies Of Bees"
    )
    blocks = parse_text("doc1", text, document_class="note")
    chunks = build_chunks("doc1", "tenant1", blocks, target_chars=260, min_chars=20, document_class="note")

    assert chunks[0].metadata["chunk_role"] == "contents"


def test_dense_body_page_is_not_classified_as_contents() -> None:
    text = (
        "Natural History Of The Honey Bee\n\n"
        "To dissect out the spermatheca of a Queen is an easy task to anyone who has a moderately delicate sense of touch and a microscope. "
        "It is only necessary to separate the last segments carefully, and the contents can then be observed without treating the whole passage as a table of contents.\n\n"
        "Mr. Pettigrew also states that in a village in Lanarkshire the profits of bee-keeping averaged over many successive years, and that several hives showed remarkable performance when managed carefully.\n\n"
        "In country districts these hives were well and economically cut out of the boll of a felled tree, but the main passage here is still ordinary body prose rather than navigational structure."
    )
    blocks = parse_text("doc1", text, document_class="book")
    chunks = build_chunks("doc1", "tenant1", blocks, target_chars=600, min_chars=40, document_class="book")

    assert chunks[0].metadata["chunk_role"] == "body"


def test_parse_text_preserves_hierarchy_path() -> None:
    text = "CHAPTER I\n\nSpring Management\n\nColonies build rapidly when nectar is available."
    blocks = parse_text("doc1", text)

    assert blocks[-1].section_path == ["Chapter I", "Spring Management"]


def test_heading_is_attached_to_following_atomic_chunk() -> None:
    text = "INTRODUCTION\n\nHoney bees produce honey and wax for the colony while workers regulate heat and feed brood."
    blocks = parse_text("doc1", text)
    chunks = build_chunks("doc1", "tenant1", blocks, target_chars=220, min_chars=20, document_class="book")

    assert len(chunks) == 1
    assert chunks[0].metadata["section_title"] == "Introduction"
    assert chunks[0].metadata["hierarchy_path"] == ["Introduction"]
    assert "INTRODUCTION" not in chunks[0].text
    assert chunks[0].text.startswith("Introduction")


def test_missing_section_does_not_force_review_for_coherent_body_chunk() -> None:
    text = (
        "Among those who were instrumental in introducing advanced methods in bee-culture among the beekeepers of Europe in the last century, "
        "this paragraph is coherent body text, includes punctuation, and should not be held in review solely because it lacks a detected heading."
    )
    blocks = parse_text("doc1", text, document_class="book")
    chunks = build_chunks("doc1", "tenant1", blocks, target_chars=400, min_chars=40, document_class="book")
    validation = validate_chunk(chunks[0])

    assert "missing_section" in validation.reasons
    assert validation.status == "accepted"


def test_sanitize_text_removes_nul_characters() -> None:
    assert sanitize_text("abc\x00def") == "abcdef"


def test_normalize_text_removes_nul_characters() -> None:
    normalized = normalize_text("Preface.\x00Thisshorttreatise")

    assert "\x00" not in normalized


def test_contents_hint_does_not_override_dense_prose_chunk() -> None:
    text = (
        "THE QUEEN\n\n"
        "§ 5. If for some reason the queen is unable to mate within the first three weeks of her life, she loses the desire to mate.\n\n"
        "These produce only drones. In about two days after mating, she commences to lay, and she is capable, if prolific, of laying three thousand or more eggs per day.\n\n"
        "When a queen lays eggs in the super or honey receptacle, it is a sign that the hive is full."
    )
    blocks = parse_text("doc1", text, document_class="book")
    chunks = build_chunks("doc1", "tenant1", blocks, target_chars=800, min_chars=40, document_class="book")
    validation = validate_chunk(chunks[0])

    assert chunks[0].metadata["chunk_role"] == "body"
    assert validation.status == "accepted"
