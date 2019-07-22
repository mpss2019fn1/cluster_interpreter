import argparse
from pathlib import Path

from wiki_index import WikidataPageProps

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--mapping-file",
        type=Path,
        help=f"Path to the mapping file",
        required=True
    )

    args = parser.parse_args()

    mappings = WikidataPageProps.initialize_instance_from_csv(args.mapping_file)

    with open("output.csv", "w+") as output_stream:
        print("embedding_label,knlowedgebase_id", file=output_stream)
        for mapping in mappings._mapping:
            print(f"{mapping},{mappings.wikidata_id(mapping)}", file=output_stream)
