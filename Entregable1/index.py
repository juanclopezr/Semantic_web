import argparse
import src.bulk.bulk_requestor as bulk_requestor
import src.bulk.connections_requestor as connections_requestor
from src.utils.download_pdf import download_pdf
from time import sleep

def get_args():
    '''
    Get program arguments specified by the user
    '''
    opt_choices = ['request_bulk', 'request_connections','download_pdf']
    
    parser = argparse.ArgumentParser()
    requiredNamedArgs = parser.add_argument_group('required named arguments')
    requiredNamedArgs.add_argument('-o', '--option', type=str, choices=opt_choices,
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
        while True:
            bulk_requestor.request_data()
            print("Sleeping for 10 minutes")
            sleep(600)
            
    if (option == 'request_connections'):
        print("Creating connections catalogue")
        connections_requestor.create_connections_catalogue()
        while True:
            connections_requestor.request_data(chunk_size=6)
            print("Sleeping for 10 minutes")
            sleep(600)
            
    if (option == 'download_pdf'):
        download_pdf()


if __name__ == "__main__":
    main()