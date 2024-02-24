import argparse
import src.bulk.bulk_requestor as bulk_requestor

def get_args():
    '''
    Get program arguments specified by the user
    '''
    parser = argparse.ArgumentParser()
    requiredNamedArgs = parser.add_argument_group('required named arguments')
    requiredNamedArgs.add_argument('-o', '--option', type=str, choices=['request_bulk'],
                                   required=True, help='Option to execute')

    args = parser.parse_args()

    return args


def main():
    '''
    PDF Articles Crawler Entry Point
    '''
    args = get_args()

    option = args.option

    if (option == 'request_bulk'):
        bulk_requestor.request_data()

if __name__ == "__main__":
    main()