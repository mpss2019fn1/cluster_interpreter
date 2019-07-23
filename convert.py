import argparse

from util.filesystem_validators import AccessibleTextFile
from wiki_index import WikidataPageProps


def main():
    parser = _initialize_parser()
    args = parser.parse_args()

    page_props = WikidataPageProps.initialize_instance_from_csv(args.input)

    with open(args.output, "w+") as file:
        print("embedding_label,knowledgebase_id", file=file)

        for page_id in page_props:
            print(f"{page_id},{page_props.wikidata_id(page_id)}", file=file)


def _initialize_parser():
    general_parser = argparse.ArgumentParser(
        description='Enrich clusters with further information by utilizing external sources')
    general_parser.add_argument("--input", help='File containing clustered entities', action=AccessibleTextFile,
                                required=True)
    general_parser.add_argument("--output", help='Desired location for enriched clusters', required=True)
    return general_parser


if __name__ == "__main__":
    main()
