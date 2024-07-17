import random
from time import time
from typing import List, Tuple

from src.data_extractors.wd_tagger import V3_MODELS
from src.db import (
    get_database_connection,
    get_folders_from_database,
    get_most_common_tags,
    search_files,
)


def select_random_tags(
    tags_list: List[Tuple[str, int]], min_tags: int = 1, max_tags: int = 3
) -> List[str]:
    # Calculate exponential weights for the number of tags
    weights_num_tags = [
        2 ** -(i - min_tags) for i in range(min_tags, max_tags + 1)
    ]

    # Normalize the weights to sum to 1
    total_weight = sum(weights_num_tags)
    weights_num_tags = [w / total_weight for w in weights_num_tags]

    num_tags = random.choices(
        range(min_tags, max_tags + 1), weights=weights_num_tags, k=1
    )[0]

    tags, counts = zip(*tags_list)

    return random.choices(tags, weights=counts, k=num_tags)


def filter_tags(
    tags: List[Tuple[str, int]], tags_to_remove: List[str]
) -> List[Tuple[str, int]]:
    return [(name, count) for name, count in tags if name not in tags_to_remove]


def perform_search(
    conn,
    tags: List[Tuple[str, int]],
    page: int = 1,
    page_size: int = 10,
    folders: List[str] = [],
    use_total_count=True,
) -> Tuple[int, int]:
    positive_tags = select_random_tags(tags, min_tags=0, max_tags=3)
    tags = filter_tags(tags, positive_tags)
    tags_match_any = select_random_tags(tags, min_tags=0, max_tags=5)
    tags = filter_tags(tags, tags_match_any)
    negative_tags_match_all = select_random_tags(tags, min_tags=0, max_tags=5)
    tags = filter_tags(tags, negative_tags_match_all)
    negative_tags = select_random_tags(tags, min_tags=0, max_tags=3)

    print(f"Positive tags: {positive_tags}")
    print(f"Negative tags: {negative_tags}")
    print(f"Negative tags match all: {negative_tags_match_all}")
    print(f"Tags match any: {tags_match_any}")
    res = list(
        search_files(
            conn,
            tags=positive_tags,
            negative_tags=negative_tags,
            negative_tags_match_all=negative_tags_match_all,
            tags_match_any=tags_match_any,
            tag_namespace=random.choice(["danbooru:", None, "dan"]),
            min_confidence=random.choice([None, 0.1, 0.2, 0.3]),
            setters=random.choices(
                V3_MODELS, k=random.randint(0, len(V3_MODELS))
            ),
            all_setters_required=random.choice([True, False, None]),
            item_type=random.choice(["image", "image/", "im", None]),
            include_path_prefix=random.choice([None, *folders]),
            order_by=random.choice(["path", "last_modified", None]),
            order=random.choice(["asc", "desc", None]),
            page=page,
            page_size=page_size,
            check_path_exists=False,
            return_total_count=use_total_count,
        )
    )
    if not res:
        return 0, 0
    rows, total = zip(*res)
    return len(rows), total[0] or len(rows)


if __name__ == "__main__":
    conn = get_database_connection()
    folders = get_folders_from_database(conn)

    tags = [
        (name, count)
        for namespace, name, count in get_most_common_tags(conn, limit=100)
    ]
    print(len(tags))
    durations = []
    durations_no_results = []
    durations_with_results = []
    duration_last_page_with_results = []
    duration_last_page_no_results = []
    n_results = []
    page_size = 100
    use_total_count = True
    for i in range(1000):
        random.seed(i)
        start_time = time()
        returned, total = perform_search(
            conn,
            tags,
            page=1,
            page_size=page_size,
            folders=folders,
            use_total_count=use_total_count,
        )
        print(f"Found {total} files in {time() - start_time:.2f} seconds")
        n_results.append(total)
        durations.append(time() - start_time)
        if total == 0:
            durations_no_results.append(time() - start_time)
        else:
            durations_with_results.append(time() - start_time)

        if total > page_size:
            # Get last page
            start_time = time()
            random.seed(i)
            total_last_page, _ = perform_search(
                conn,
                tags,
                page=total // page_size,
                page_size=page_size,
                folders=folders,
                use_total_count=use_total_count,
            )
            print(
                f"Returned {total_last_page} files in {time() - start_time:.2f} seconds (page: {total // page_size})"
            )
            if total_last_page == 0:
                duration_last_page_no_results.append(time() - start_time)
            else:
                duration_last_page_with_results.append(time() - start_time)

    print(f"Total searches: {len(durations)}")
    print(f"Page size: {page_size}")
    print(
        f"Average results: {sum(n_results) / len(n_results):.2f} for total {len(n_results)} searches"
    )
    print(
        f"Average duration: {sum(durations) / len(durations):.2f} seconds for total {len(durations)} searches"
    )
    print(
        f"Average duration (no results): {sum(durations_no_results) / len(durations_no_results):.2f} seconds for total {len(durations_no_results)} searches"
    )
    print(
        f"Average duration (with results): {sum(durations_with_results) / len(durations_with_results):.2f} seconds for total {len(durations_with_results)} searches"
    )
    if len(duration_last_page_no_results) > 0:
        print(
            f"Average duration last page (no results): {sum(duration_last_page_no_results) / len(duration_last_page_no_results):.2f} seconds for total {len(duration_last_page_no_results)} searches"
        )
